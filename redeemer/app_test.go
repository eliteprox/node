package redeemer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/nodeconfig"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/redemption"
	"github.com/stretchr/testify/require"
)

func bindAddress(t *testing.T) boa.HTTPBind {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	var bind boa.HTTPBind
	require.NoError(t, bind.UnmarshalText([]byte(address)))
	return bind
}

func serviceParams(t *testing.T, f *test.RedemptionRPC) Params {
	t.Helper()
	file, password := test.WriteFixedKeystore(t)
	key, err := eth.OpenKeystoreFile(file, password)
	require.NoError(t, err)
	f.Mu.Lock()
	f.Wallet = key.Address()
	f.Mu.Unlock()
	rpc, err := url.Parse(f.Server.URL)
	require.NoError(t, err)
	id := uint64(1)
	controller := common.HexToAddress("0x1000")
	return Params{Settings: nodeconfig.Settings{Network: "test", DataDir: t.TempDir()}, Listen: bindAddress(t), MetricsListen: bindAddress(t), RedeemerDB: filepath.Join(t.TempDir(), "tickets.sqlite"), KeystoreFile: file, KeystorePasswordFile: password, RPCURL: rpc, ChainID: &id, Controller: &controller, RedeemerTxTimeout: time.Second, RedeemerMaxTxReplacements: 1, MaxPendingTickets: 32, MaxTicketsPerRecipient: 8}
}

func TestDedicatedWalletStartupPolicy(t *testing.T) {
	for _, kind := range []string{"registered", "active", "unavailable", "wrong-chain"} {
		t.Run(kind, func(t *testing.T) {
			f := test.NewRedemptionRPC(t)
			p := serviceParams(t, f)
			f.Mu.Lock()
			switch kind {
			case "registered":
				f.Registered = true
			case "active":
				f.WalletActive = true
			case "unavailable":
				f.SnapshotError = true
			case "wrong-chain":
				*p.ChainID = 2
			}
			f.Mu.Unlock()
			require.Error(t, Serve(t.Context(), p, io.Discard))
			require.NoFileExists(t, p.RedeemerDB, "wallet and chain checks precede database creation")
		})
	}
}

func TestServeProbesAndShutdown(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(fmt.Sprintf("busy=%t", busy), func(t *testing.T) {
			f := test.NewRedemptionRPC(t)
			p := serviceParams(t, f)
			if busy {
				seedBusyQueue(t, p)
				handler := f.Server.Config.Handler
				selector := fmt.Sprintf("0x%x", crypto.Keccak256([]byte("usedTickets(bytes32)"))[:4])
				f.Server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					var request struct {
						Method string
						Params []json.RawMessage
					}
					require.NoError(t, json.Unmarshal(body, &request))
					if request.Method == "eth_call" {
						var call struct{ Input string }
						require.NoError(t, json.Unmarshal(request.Params[0], &call))
						if strings.HasPrefix(call.Input, selector) {
							<-r.Context().Done()
							return
						}
					}
					r.Body = io.NopCloser(bytes.NewReader(body))
					handler.ServeHTTP(w, r)
				})
			}
			wait := 5 * time.Second
			if busy {
				wait = 40 * time.Second
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, p, io.Discard) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Error("redeemer did not stop")
				}
			})
			client := &http.Client{Timeout: time.Second}
			defer client.CloseIdleConnections()
			require.Eventually(t, func() bool {
				response, err := client.Get("http://" + p.MetricsListen.String() + "/readyz")
				if err != nil {
					return false
				}
				defer response.Body.Close()
				return response.StatusCode == 200
			}, wait, 10*time.Millisecond)
			response, err := client.Get("http://" + p.MetricsListen.String() + "/metrics")
			require.NoError(t, err)
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Contains(t, string(data), `component="redeemer"`)
			response, err = client.Get("http://" + p.Listen.String() + "/tickets")
			require.NoError(t, err)
			response.Body.Close()
			require.Equal(t, 405, response.StatusCode)
			info, err := os.Stat(p.RedeemerDB)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0600), info.Mode().Perm())
		})
	}
}

func seedBusyQueue(t *testing.T, p Params) {
	t.Helper()
	store, err := redemption.OpenRedeemerDB(p.RedeemerDB)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.Bind(t.Context(), redemption.Binding{ChainID: "1", Controller: *p.Controller, Role: "redeemer", Destination: "chain"}, redemption.DefaultLimits, nil))
	payer, err := eth.OpenKeystoreFile(test.WriteFixedKeystore(t))
	require.NoError(t, err)
	for nonce := uint32(1); nonce <= 3; nonce++ {
		random := new(big.Int).SetUint64(uint64(nonce))
		ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: payer.Address(), Recipient: common.HexToAddress("0x5678"), FaceValue: big.NewInt(1e12), WinProb: new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)), TicketNonce: nonce, RecipientRandHash: crypto.Keccak256Hash(common.LeftPadBytes(random.Bytes(), 32)), CreationRound: 1, CreationRoundBlockHash: common.HexToHash("0x1"), ParamsExpirationBlock: big.NewInt(10)}, RecipientRand: random}
		ticket.Sig, err = payer.SignMessage(ticket.Hash().Bytes())
		require.NoError(t, err)
		require.NoError(t, store.Admit(t.Context(), ticket))
	}
}

func TestPrintConfigRedactsCredentials(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("LIVEPEER_REDEEMER_RPC_URL", "http://private.example/secret-token")
	t.Setenv("LIVEPEER_REDEEMER_KEYSTORE_PASSWORD", "wallet-secret")
	var out, errOut bytes.Buffer
	cmd := Root(&out, &errOut)
	cmd.SetArgs([]string{"--config=", "--print-config"})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "8941")
	require.NotContains(t, out.String(), "secret-token")
	require.NotContains(t, out.String(), "wallet-secret")
}
