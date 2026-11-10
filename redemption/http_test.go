package redemption

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

func winningTicket(t *testing.T, id int64) (*pm.SignedTicket, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	random := big.NewInt(id)
	ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: crypto.PubkeyToAddress(key.PublicKey), Recipient: common.HexToAddress("0x5678"), FaceValue: big.NewInt(1e12), WinProb: new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)), TicketNonce: uint32(id), RecipientRandHash: crypto.Keccak256Hash(common.LeftPadBytes(random.Bytes(), 32)), CreationRound: 1, CreationRoundBlockHash: common.HexToHash("0x1"), ParamsExpirationBlock: big.NewInt(10)}, RecipientRand: random}
	signTicket(t, ticket, key)
	return ticket, key
}

func signTicket(t *testing.T, ticket *pm.SignedTicket, key *ecdsa.PrivateKey) {
	t.Helper()
	sig, err := crypto.Sign(accounts.TextHash(ticket.Hash().Bytes()), key)
	require.NoError(t, err)
	sig[64] += 27
	ticket.Sig = sig
}

func boundStore(t *testing.T, role, target string, limits Limits) (*RedeemerDB, Binding) {
	t.Helper()
	s, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "tickets.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	b := Binding{ChainID: "1", Controller: common.HexToAddress("0x1000"), Role: role, Destination: target}
	require.NoError(t, s.Bind(t.Context(), b, limits))
	return s, b
}

func TestHTTPAdmissionIdentityCapacityAndScope(t *testing.T) {
	s, b := boundStore(t, "redeemer", "chain", Limits{Pending: 2, PerRecipient: 1})
	handler := Handler(s)
	submit := func(req TicketRequest) *httptest.ResponseRecorder {
		raw, err := json.Marshal(req)
		require.NoError(t, err)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("POST", "/tickets", bytes.NewReader(raw)))
		return w
	}
	ticket, key := winningTicket(t, 1)
	req := TicketRequest{b.ChainID, b.Controller, ticketJSON(ticket)}
	response := submit(req)
	require.Equal(t, 202, response.Code)
	require.NotContains(t, response.Body.String(), "sig")
	require.Equal(t, 202, submit(req).Code, "duplicates succeed at recipient capacity")
	req.Ticket.Expiration = "11"
	require.Equal(t, 409, submit(req).Code)
	req.Ticket.Expiration = "10"
	req.ChainID = "2"
	require.Equal(t, 400, submit(req).Code)
	req.ChainID = "1"
	req.Controller = common.HexToAddress("0xabc")
	require.Equal(t, 400, submit(req).Code)
	req.Controller = b.Controller
	ticket.WinProb = big.NewInt(1)
	signTicket(t, ticket, key)
	req.Ticket = ticketJSON(ticket)
	require.Equal(t, 400, submit(req).Code, "losing tickets are never queued")
	second, _ := winningTicket(t, 2)
	require.ErrorIs(t, s.Admit(t.Context(), second), ErrCapacity)
	second.Recipient = common.HexToAddress("0x777")
	secondKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	second.PayerAddress = crypto.PubkeyToAddress(secondKey.PublicKey)
	signTicket(t, second, secondKey)
	require.NoError(t, s.Admit(t.Context(), second))
	third, _ := winningTicket(t, 3)
	require.ErrorIs(t, s.Admit(t.Context(), third), ErrCapacity)
	bad := httptest.NewRecorder()
	handler.ServeHTTP(bad, httptest.NewRequest("POST", "/tickets", bytes.NewReader(make([]byte, MaxTicketBody+1))))
	require.Equal(t, 400, bad.Code)
	changed := b
	changed.Controller = common.HexToAddress("0x999")
	require.ErrorContains(t, s.Bind(t.Context(), changed, DefaultLimits), "mismatch")
	changed = b
	changed.Destination = "http://elsewhere.example"
	require.ErrorContains(t, s.Bind(t.Context(), changed, DefaultLimits), "drain")
}

func TestHashBackfillPreservesAlternativeSignature(t *testing.T) {
	s, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "legacy.sqlite"))
	require.NoError(t, err)
	defer s.Close()
	ticket, key := winningTicket(t, 1)
	require.NoError(t, s.StoreWinningTicket(ticket))
	original := bytes.Clone(ticket.Sig)
	r, ss, err := ecdsa.Sign(rand.Reader, key, accounts.TextHash(ticket.Hash().Bytes()))
	require.NoError(t, err)
	half := new(big.Int).Rsh(new(big.Int).Set(crypto.S256().Params().N), 1)
	if ss.Cmp(half) > 0 {
		ss.Sub(crypto.S256().Params().N, ss)
	}
	sig := make([]byte, 65)
	r.FillBytes(sig[:32])
	ss.FillBytes(sig[32:64])
	for _, v := range []byte{27, 28} {
		sig[64] = v
		if (pm.DefaultSigVerifier{}).Verify(ticket.PayerAddress, ticket.Hash().Bytes(), sig) {
			break
		}
	}
	ticket.Sig = sig
	require.NoError(t, ValidateTicket(ticket))
	require.NotEqual(t, original, sig)
	require.NoError(t, s.StoreWinningTicket(ticket))
	_, err = s.db.Exec("UPDATE winning_tickets SET ticket_hash=NULL")
	require.NoError(t, err)
	_, err = s.db.Exec("DELETE FROM ticket_state")
	require.NoError(t, err)
	require.NoError(t, s.backfill(t.Context()))
	require.NoError(t, s.backfill(t.Context()))
	var rows, identities int
	require.NoError(t, s.db.QueryRow("SELECT count(*) FROM winning_tickets").Scan(&rows))
	require.NoError(t, s.db.QueryRow("SELECT count(*) FROM ticket_state").Scan(&identities))
	require.Equal(t, 2, rows)
	require.Equal(t, 1, identities)
	require.NoError(t, s.Bind(t.Context(), Binding{"1", common.HexToAddress("0x1000"), "orchestrator", "chain"}, DefaultLimits))
	require.NoError(t, s.Admit(t.Context(), ticket))
}

func TestRemoteDeliveryLostAcknowledgmentAndKeylessSettlement(t *testing.T) {
	f := newRecoveryRPC(t)
	chain := f.Chain(t)
	serverStore, _ := boundStore(t, "redeemer", "chain", DefaultLimits)
	first := true
	handler := Handler(serverStore)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first {
			first = false
			record := httptest.NewRecorder()
			handler.ServeHTTP(record, r)
			require.Equal(t, 202, record.Code)
			http.Error(w, "lost acknowledgment", 503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	s, b := boundStore(t, "orchestrator", server.URL, DefaultLimits)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	policy, err := destination.New("redeemer", []string{"127.0.0.1", "::1"})
	require.NoError(t, err)
	remote, err := NewHTTPDestination(u, policy, b)
	require.NoError(t, err)
	defer remote.Close()
	ticket, _ := winningTicket(t, 1)
	require.NoError(t, s.Admit(t.Context(), ticket))
	worker, err := NewWorker(s, chain, remote)
	require.NoError(t, err)
	require.NotEmpty(t, worker.Process(t.Context()))
	var exposed int
	var delivery string
	require.NoError(t, s.db.QueryRow("SELECT exposed,delivery FROM ticket_state").Scan(&exposed, &delivery))
	require.Equal(t, 1, exposed)
	require.Equal(t, "retryable", delivery)
	var seq int
	var name, path string
	require.NoError(t, s.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path))
	require.NoError(t, s.Close())
	s, err = OpenRedeemerDB(path)
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Bind(t.Context(), b, DefaultLimits))
	_, err = s.db.Exec("UPDATE ticket_state SET next_attempt=0")
	require.NoError(t, err)
	worker, err = NewWorker(s, chain, remote)
	require.NoError(t, err)
	require.Empty(t, worker.Process(t.Context()))
	require.Nil(t, s.recovery, "remote mode does not create a wallet recovery manager")
	var count int
	require.NoError(t, serverStore.db.QueryRow("SELECT count(*) FROM winning_tickets").Scan(&count))
	require.Equal(t, 1, count)
	key := recoveryKey(t)
	f.Mu.Lock()
	f.Wallet = key.Address()
	f.Tickets = append(f.Tickets, ticket)
	f.Finalized = false
	f.Mu.Unlock()
	serverChain := f.Chain(t)
	serverChain.Contracts.CheckRedemptionCost = true
	serverChain.Contracts.BeforeBroadcast = StandalonePolicy(serverChain, big.NewInt(1))
	chainDest, err := NewChainDestination(t.Context(), serverStore, serverChain, key, big.NewInt(1), RedemptionOptions{Timeout: time.Hour})
	require.NoError(t, err)
	serverWorker, err := NewWorker(serverStore, serverChain, chainDest)
	require.NoError(t, err)
	require.Empty(t, serverWorker.Process(t.Context()))
	require.Len(t, f.Transactions(), 1)
	f.Mine(f.Transactions()[0], true)
	_, err = s.db.Exec("UPDATE ticket_state SET next_attempt=0")
	require.NoError(t, err)
	require.Empty(t, worker.Process(t.Context()))
	var settled int
	require.NoError(t, s.db.QueryRow("SELECT settled FROM ticket_state").Scan(&settled))
	require.Zero(t, settled, "unfinalized use retains liability")
	f.Mu.Lock()
	f.Finalized = true
	f.Mu.Unlock()
	_, err = s.db.Exec("UPDATE ticket_state SET next_attempt=0")
	require.NoError(t, err)
	require.Empty(t, worker.Process(t.Context()))
	require.NoError(t, s.db.QueryRow("SELECT settled FROM ticket_state").Scan(&settled))
	require.Equal(t, 1, settled)
	var floor int64
	require.NoError(t, s.db.QueryRow("SELECT number FROM collateral_watermark").Scan(&floor))
	require.EqualValues(t, 100, floor)
}

func TestHTTPDestinationRejectsRedirectsAndBadAcknowledgments(t *testing.T) {
	ticket, _ := winningTicket(t, 1)
	for _, status := range []int{202, 302, 400, 409, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "http://127.0.0.1:1/secret")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"ticket_hash":"0x`+string(bytes.Repeat([]byte("0"), 64))+`"}`)
			}))
			defer server.Close()
			u, err := url.Parse(server.URL)
			require.NoError(t, err)
			policy, err := destination.New("redeemer", []string{"127.0.0.1", "::1"})
			require.NoError(t, err)
			d, err := NewHTTPDestination(u, policy, Binding{})
			require.NoError(t, err)
			defer d.Close()
			err = d.Submit(t.Context(), ticket)
			require.Error(t, err)
			if status == 400 || status == 409 {
				var rejection *Rejection
				require.ErrorAs(t, err, &rejection)
			}
		})
	}
}
