package signer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/orchestrator"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

func TestPaymentEncoding(t *testing.T) {
	data, err := os.ReadFile("testdata/payment-before-authorization.json")
	require.NoError(t, err)
	var fixtures map[string]struct{ Payment, SegCreds string }
	require.NoError(t, json.Unmarshal(data, &fixtures))
	keyFile, passwordFile := test.WriteFixedKeystore(t)
	key, err := eth.OpenKeystoreFile(keyFile, passwordFile)
	require.NoError(t, err)
	s, info := testService(t)
	s.key = key
	info.Auth.Expiration = 1900000000
	for _, kind := range []string{"fixed", "live"} {
		t.Run(kind, func(t *testing.T) {
			draft, err := s.preparePayment(t.Context(), kind, info.Auth.SessionID, info, paymentState{}, -1, time.Now())
			require.NoError(t, err)
			signed, err := draft.Sign(t.Context(), key, nil)
			require.NoError(t, err)
			require.Equal(t, fixtures[kind].Payment, signed.Payment)
			require.Equal(t, fixtures[kind].SegCreds, signed.SegCreds)
		})
	}
}

func TestDraftPaymentPreservesValues(t *testing.T) {
	s, original := testService(t)
	for _, kind := range []string{"fixed", "live"} {
		t.Run(kind, func(t *testing.T) {
			// Decode a separate envelope so mutation cannot affect another case.
			info, err := wire.DecodeOrchestratorInfo(wire.EncodeOrchestratorInfo(original))
			require.NoError(t, err)
			// Retain noncanonical integer bytes rather than re-encoding big.Ints.
			info.TicketParams.FaceValue = append([]byte{0}, info.TicketParams.FaceValue...)
			now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
			state := paymentState{PMSessionID: ethcommon.BytesToHash(info.TicketParams.RecipientRandHash).Hex(), TicketNonce: 41, Balance: "1/3", LastUpdate: now.Add(-1500 * time.Millisecond), SequenceNumber: 7}
			draft, err := s.preparePayment(t.Context(), kind, info.Auth.SessionID, info, state, 7, now)
			require.NoError(t, err)
			require.Equal(t, now, draft.State.LastUpdate)
			require.Equal(t, uint64(8), draft.State.SequenceNumber)
			fee := "10"
			count := 1
			if kind == "live" {
				fee = "20"
				count = 2
			}
			require.Equal(t, fee, draft.Usage.Fee.RatString())
			require.Equal(t, count, draft.Usage.NumTickets)
			require.Equal(t, uint32(41+count), draft.State.TicketNonce)
			paymentBefore := wire.EncodePayment(draft.payment)
			segmentBefore := wire.EncodeSegData(draft.segment)
			signatureMessage := append([]byte(info.Auth.SessionID), make([]byte, 32)...)
			signatureMessage = append(signatureMessage, crypto.Keccak256(nil)...)
			for _, value := range [][]byte{info.TicketParams.Recipient, info.TicketParams.FaceValue, info.TicketParams.WinProb, info.TicketParams.RecipientRandHash, info.TicketParams.Seed, info.TicketParams.ExpirationBlock, info.TicketParams.Expiration.CreationRoundBlockHash, info.Auth.Token} {
				value[0] ^= 0xff
			}
			info.Price.PricePerUnit = 999
			info.Auth.Expiration = 0
			signed, err := draft.Sign(t.Context(), s.key, nil)
			require.NoError(t, err)
			paymentBytes, err := base64.StdEncoding.DecodeString(signed.Payment)
			require.NoError(t, err)
			payment, err := wire.DecodePayment(paymentBytes)
			require.NoError(t, err)
			require.Equal(t, draft.Usage.NumTickets, len(payment.PayerParams))
			require.Equal(t, draft.State.TicketNonce, payment.PayerParams[len(payment.PayerParams)-1].TicketNonce)
			payment.PayerParams = nil
			require.Equal(t, paymentBefore, wire.EncodePayment(payment))
			segmentBytes, err := base64.StdEncoding.DecodeString(signed.SegCreds)
			require.NoError(t, err)
			segment, err := wire.DecodeSegData(segmentBytes)
			require.NoError(t, err)
			require.True(t, (pm.DefaultSigVerifier{}).Verify(s.key.Address(), crypto.Keccak256(signatureMessage), segment.Signature))
			segment.Signature = nil
			require.Equal(t, segmentBefore, wire.EncodeSegData(segment))
		})
	}
}

type collateralChain struct{ withdraw *big.Int }

func (c collateralChain) Snapshot(context.Context) (pm.ChainSnapshot, error) {
	return pm.ChainSnapshot{Block: big.NewInt(50), Round: big.NewInt(5), RoundHash: ethcommon.HexToHash("0x1234")}, nil
}
func (c collateralChain) IsActiveAt(context.Context, ethcommon.Address, pm.ChainSnapshot) (bool, error) {
	return true, nil
}
func (c collateralChain) PayerFunds(ctx context.Context, _, _ ethcommon.Address) (pm.PayerFunds, error) {
	snapshot, err := c.Snapshot(ctx)
	return pm.PayerFunds{Snapshot: snapshot, Deposit: big.NewInt(10), Reserve: big.NewInt(10), WithdrawRound: c.withdraw}, err
}

func TestRecipientReservesWinningLiabilityAcrossConcurrentSessions(t *testing.T) {
	key, _, _ := testSignerKey(t)
	store, err := orchestrator.OpenRedeemerDB(filepath.Join(t.TempDir(), "recipient.sqlite"))
	require.NoError(t, err)
	defer store.Close()
	chain := collateralChain{new(big.Int)}
	engine, err := orchestrator.NewPaymentEngine(store, chain, ethcommon.HexToAddress("0x1234"), big.NewInt(10), new(big.Int).Sub(new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)), big.NewInt(1)))
	require.NoError(t, err)
	s := newService(key)
	s.SetPaymentChain(chain)
	payments := map[string]signedPayment{}
	for _, manifest := range []string{"one", "two", "three"} {
		_, err := engine.MakeChallenge(t.Context(), "runner", manifest, key.Address(), 9, "fixed", "https://orch.example")
		require.NoError(t, err)
		info, err := engine.ChallengeInfo(manifest)
		require.NoError(t, err)
		draft, err := s.preparePayment(t.Context(), "fixed", manifest, info, paymentState{}, -1, time.Now())
		require.NoError(t, err)
		payment, err := draft.Sign(t.Context(), key, nil)
		require.NoError(t, err)
		payments[manifest] = payment
	}
	var workers sync.WaitGroup
	errors := make(chan error, 3)
	for manifest, payment := range payments {
		workers.Go(func() {
			_, _, err := engine.Receive(t.Context(), "runner", manifest, payment.Payment, payment.SegCreds)
			errors <- err
		})
	}
	workers.Wait()
	close(errors)
	accepted, rejected := 0, 0
	for err := range errors {
		if err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, pm.ErrPayerUnavailable)
			rejected++
		}
	}
	require.Equal(t, 2, accepted)
	require.Equal(t, 1, rejected)
	count, err := store.WinningTicketCount(key.Address(), 0)
	require.NoError(t, err)
	require.Equal(t, 2, count, "rejected batches must roll back tickets and credit")
}

// Adapted from BlueClawNetwork/livepeer-node signer/payment_test.go,
// 285eda984d7cce4e3030ef0510d63ae142534dd7, by Mike Zupper (MIT; see LICENSE).
type paidPath struct {
	service   *Service
	engine    *orchestrator.PaymentEngine
	rpc       *test.ChainRPC
	recipient ethcommon.Address
	metrics   *observability.Metrics
	stop      func()
}

func newPaidPath(t *testing.T, observed bool) *paidPath {
	t.Helper()
	key, _, _ := testSignerKey(t)
	p := &paidPath{service: newService(key), rpc: test.NewChainRPC(t), recipient: ethcommon.HexToAddress("0x1234"), metrics: observability.New("orchestrator")}
	t.Cleanup(func() { _ = p.service.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	var observers sync.WaitGroup
	p.stop = func() { cancel(); observers.Wait() }
	t.Cleanup(p.stop)
	newChain := func(metrics *observability.Metrics, recipient ethcommon.Address) eth.PaymentChain {
		client, err := eth.OpenRPC(p.rpc.URL)
		require.NoError(t, err)
		t.Cleanup(func() { p.stop(); client.Close() })
		client.SetObserver(metrics.RPC)
		contracts, err := eth.NewContracts(client, p.rpc.Controller)
		require.NoError(t, err)
		chain := eth.PaymentChain{Contracts: contracts}
		if observed {
			o := eth.NewPaymentObserver(chain, true, recipient)
			chain.Observer = o
			metrics.PaymentObserver(o, nil)
			observers.Go(func() { o.Run(ctx) })
			require.Eventually(t, func() bool { return o.Status().Healthy }, 5*time.Second, time.Millisecond)
		}
		return chain
	}
	direct := newChain(p.metrics, p.recipient)
	p.service.SetPaymentChain(newChain(p.service.metrics, ethcommon.Address{}))
	store, err := orchestrator.OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	p.engine, err = orchestrator.NewPaymentEngine(store, direct, p.recipient, big.NewInt(1e12), new(big.Int).Quo(maximum, big.NewInt(1e5)))
	require.NoError(t, err)
	return p
}

func (p *paidPath) challenge(t *testing.T, ctx context.Context, manifest string) wire.OrchestratorInfo {
	t.Helper()
	_, err := p.engine.MakeChallenge(ctx, "runner", manifest, p.service.key.Address(), 9, "fixed", "https://orch.example")
	require.NoError(t, err)
	info, err := p.engine.ChallengeInfo(manifest)
	require.NoError(t, err)
	return info
}

func (p *paidPath) sign(t *testing.T, info wire.OrchestratorInfo) paymentResponse {
	t.Helper()
	response := postPayment(t, p.service, map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
	require.Equal(t, 200, response.Code, response.Body.String())
	var signed paymentResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &signed))
	return signed
}

func TestPaidRequestDirectRPCBudget(t *testing.T) {
	p := newPaidPath(t, false)
	s, engine, rpc, recipient := p.service, p.engine, p.rpc, p.recipient
	info := p.challenge(t, t.Context(), "manifest")
	require.Equal(t, 6, rpc.Stats().RoundTrips, "challenge: snapshot (4) + activity (2)")

	signed := p.sign(t, info)
	require.Equal(t, 13, rpc.Stats().RoundTrips, "signer adds one funds observation (7)")
	_, _, err := engine.Receive(t.Context(), "runner", "manifest", signed.Payment, signed.SegCreds)
	require.NoError(t, err)
	stats := rpc.Stats()
	require.Equal(t, 22, stats.RoundTrips, "receipt adds funds (7) + activity (2); baseline was 29")
	require.Equal(t, map[string]int{
		"eth_getBlockByNumber": 3, "Controller.getContract": 7,
		"RoundsManager.lastInitializedRound": 3, "RoundsManager.blockHashForRound": 3,
		"BondingManager.isActiveTranscoder": 2, "TicketBroker.getSenderInfo": 2, "TicketBroker.claimableReserve": 2,
	}, stats.Calls)

	// A signer's retained observation cannot prove that the recipient is behind
	// expiry. The recipient must still enforce its own clock at the boundary.
	info = p.challenge(t, t.Context(), "divergent")
	older, err := s.paymentChain.PayerFunds(t.Context(), s.key.Address(), recipient)
	require.NoError(t, err)
	s.SetPaymentChain(payerChainFunc(func(context.Context, ethcommon.Address, ethcommon.Address) (pm.PayerFunds, error) { return older, nil }))
	rpc.Update(func(state *test.ChainState) {
		state.L1Block = new(big.Int).SetBytes(info.TicketParams.ExpirationBlock).Uint64()
		state.Number++
		state.Hash = ethcommon.HexToHash("0xeeee")
	})
	signed = p.sign(t, info)
	_, _, err = engine.Receive(t.Context(), "runner", "divergent", signed.Payment, signed.SegCreds)
	require.ErrorIs(t, err, orchestrator.ErrInvalidPayment)
}

func TestPaidRequestCachedRPCBudget(t *testing.T) {
	p := newPaidPath(t, true)
	before := p.rpc.Stats().RoundTrips
	p.request(t, t.Context(), "cold")
	require.Equal(t, 4, p.rpc.Stats().RoundTrips-before, "one cold pair read in each process")
	require.EqualValues(t, 4, test.MetricSum(t, p.metrics.Registry, "rpc_work_total", map[string]string{"work": "foreground", "kind": "transport"})+test.MetricSum(t, p.service.metrics.Registry, "rpc_work_total", map[string]string{"work": "foreground", "kind": "transport"}))
	before = p.rpc.Stats().RoundTrips
	p.request(t, t.Context(), "warm")
	require.Equal(t, before, p.rpc.Stats().RoundTrips, "new manifests for a known payer read memory")
}

// Adapted from Mike Zupper's BlueClawNetwork/livepeer-node signer/payment_test.go,
// a4fe74293a04937c5c5514b1a3706718a502eba0 (MIT; see LICENSE).
func (p *paidPath) request(t *testing.T, ctx context.Context, manifest string) {
	t.Helper()
	ctx = eth.WithRPCWork(ctx, "foreground")
	info := p.challenge(t, ctx, manifest)
	signed := p.sign(t, info)
	_, _, err := p.engine.Receive(ctx, "runner", manifest, signed.Payment, signed.SegCreds)
	require.NoError(t, err)
}
