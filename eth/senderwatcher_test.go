package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

// Cases adapt Nico Vergauwen's go-livepeer eth/watchers/senderwatcher_test.go
// (341b39a0c), and Mike Zupper's PR #1 eth/chainview_test.go (bbe126e).
// Copyright (c) 2017 Livepeer; MIT notice in LICENSE.
func warmPayer(t *testing.T, o *PaymentObserver, payer, recipient common.Address) SenderInfo {
	t.Helper()
	require.NoError(t, o.refresh(t.Context()))
	funds, err := o.senderInfo(t.Context(), payer, recipient)
	require.NoError(t, err)
	return funds
}

func payerHistoryBloom(s *test.ChainState, mode string) {
	s.LogsBloom = nil
	switch mode {
	case "empty":
		s.LogsBloom = types.Bloom{}
	case "null":
		s.LogsBloom = json.RawMessage("null")
	case "malformed":
		s.LogsBloom = "0x01"
	case "positive", "unrelated":
		address := s.Contracts["TicketBroker"]
		if mode == "unrelated" {
			address = common.HexToAddress("0x9999")
		}
		s.Logs = []types.Log{{Address: address, BlockHash: s.Hash, BlockNumber: s.Number, Topics: []common.Hash{common.HexToHash("0x1")}}}
		s.LogsBloom = payerLogsBloom(s.Logs)
	}
}

func payerLogsBloom(logs []types.Log) types.Bloom {
	receipt := new(types.Receipt)
	for n := range logs {
		receipt.Logs = append(receipt.Logs, &logs[n])
	}
	return types.CreateBloom(receipt)
}

func TestPayerCacheWarmCopiesAndEntryFailure(t *testing.T) {
	f, o, now := newTestObserver(t)
	payer, recipient := common.HexToAddress("0x11"), common.HexToAddress("0x22")
	o.recipient = recipient
	f.Update(func(s *test.ChainState) { s.Claimable = 7; s.Reserve = 50; s.Recipient = recipient })
	info := warmPayer(t, o, payer, recipient)
	c := PaymentChain{Contracts: o.chain.Contracts, Observer: o}
	require.Equal(t, int64(7), info.Reserve.Int64())
	info.Deposit.SetInt64(0)
	info.Snapshot.Number.SetInt64(0)
	before := f.Stats()
	for range 20 {
		info, err := c.SenderInfo(t.Context(), payer, recipient)
		require.NoError(t, err)
		require.Positive(t, info.Deposit.Sign())
		require.Positive(t, info.Snapshot.Number.Sign())
		active, err := c.IsActiveAt(t.Context(), recipient, info.Snapshot)
		require.NoError(t, err)
		require.True(t, active, "payer and activity share one complete observation")
	}
	require.Equal(t, before, f.Stats())
	foreign := PaymentChain{Contracts: &Contracts{}, Observer: o}
	_, err := foreign.SenderInfo(t.Context(), payer, recipient)
	require.ErrorContains(t, err, "another contract client")
	require.Equal(t, before, f.Stats())
	total, err := c.SenderInfo(t.Context(), payer, common.Address{})
	require.NoError(t, err)
	require.Equal(t, int64(50), total.Reserve.Int64())
	bad := common.HexToAddress("0xbad")
	f.Update(func(s *test.ChainState) { s.FailPayer = bad })
	_, err = c.SenderInfo(t.Context(), bad, recipient)
	require.Error(t, err)
	before = f.Stats()
	_, err = c.SenderInfo(t.Context(), bad, recipient)
	require.Error(t, err)
	_, err = c.SenderInfo(t.Context(), payer, recipient)
	require.NoError(t, err)
	require.Equal(t, before, f.Stats(), "one failing entry cannot cause repeated or unrelated reads")
	*now = now.Add(o.maxAge)
	_, err = c.SenderInfo(t.Context(), payer, recipient)
	require.ErrorIs(t, err, ErrObservationUnavailable)
	f.Update(func(s *test.ChainState) { s.FailPayer = common.Address{} })
	advanceObserver(f, *now)
	require.NoError(t, o.refresh(t.Context()))
	_, err = c.SenderInfo(t.Context(), bad, recipient)
	require.NoError(t, err)
}

func TestPayerCacheEventInvalidation(t *testing.T) {
	for _, event := range []string{"DepositFunded", "ReserveFunded", "ReserveClaimed", "Withdrawal", "WinningTicketTransfer", "Unlock", "UnlockCancelled", "round", "activation", "registry-target", "gap", "removed", "logs-failure", "range-unchanged", "range-funded"} {
		t.Run(event, func(t *testing.T) {
			f, o, now := newTestObserver(t)
			payer, other, recipient := common.HexToAddress("0x11"), common.HexToAddress("0x12"), common.HexToAddress("0x22")
			initial := warmPayer(t, o, payer, recipient)
			_, err := o.senderInfo(t.Context(), other, recipient)
			require.NoError(t, err)
			ranged := event == "range-unchanged" || event == "range-funded"
			steps := 1
			if ranged {
				steps = 20
			}
			for range steps {
				advanceObserver(f, *now)
			}
			f.Update(func(s *test.ChainState) {
				if event != "range-unchanged" {
					s.Deposit, s.Claimable, s.WithdrawRound = 13, 17, 9
				}
				log := types.Log{Address: s.Contracts["TicketBroker"], BlockHash: s.Hash, BlockNumber: s.Number, Topics: []common.Hash{{}, common.BytesToHash(payer.Bytes())}}
				switch event {
				case "round":
					s.CurrentRound++ // Before lastInitializedRound advances.
				case "activation":
					log.Address, log.Topics[0] = s.Contracts["BondingManager"], o.chain.Contracts.abis["bondingManager"].Events["TranscoderActivated"].ID
				case "registry-target":
					log.Address, log.Topics[0] = f.Controller, o.chain.Contracts.abis["controller"].Events["SetContractInfo"].ID
				case "gap":
					s.Number += 129
				case "removed":
					log.Removed = true
				case "logs-failure":
					s.Fail = "eth_getLogs"
				case "range-unchanged", "range-funded":
					log.Topics[0] = o.chain.Contracts.abis["ticketBroker"].Events["DepositFunded"].ID
				default:
					log.Topics[0] = o.chain.Contracts.abis["ticketBroker"].Events[event].ID
				}
				if event != "range-unchanged" {
					s.Logs = []types.Log{log}
				}
				if event != "logs-failure" {
					s.LogsBloom = payerLogsBloom(s.Logs)
				}
			})
			require.NoError(t, o.refresh(t.Context()))
			if ranged {
				require.NoError(t, initial.Snapshot.Validate(), "verified continuity preserves the generation")
			}
			before := f.Stats().Calls["TicketBroker.getSenderInfo"]
			info, err := o.senderInfo(t.Context(), payer, recipient)
			require.NoError(t, err)
			if event == "range-unchanged" {
				require.Equal(t, initial.Deposit, info.Deposit)
				require.Equal(t, initial.Reserve, info.Reserve)
			} else {
				require.Equal(t, int64(13), info.Deposit.Int64())
				require.Equal(t, int64(17), info.Reserve.Int64())
				require.Equal(t, int64(9), info.WithdrawRound.Int64())
				before++
			}
			require.Equal(t, before, f.Stats().Calls["TicketBroker.getSenderInfo"])
			require.Equal(t, f.State().Number, info.Snapshot.Number.Uint64())
			_, err = o.senderInfo(t.Context(), other, recipient)
			require.NoError(t, err)
			if _, targeted := o.chain.Contracts.abis["ticketBroker"].Events[event]; targeted || ranged {
				require.Equal(t, before, f.Stats().Calls["TicketBroker.getSenderInfo"], "unaffected payer stays warm")
			}
			before = f.Stats().Calls["TicketBroker.getSenderInfo"]
			_, err = o.senderInfo(t.Context(), payer, recipient)
			require.NoError(t, err)
			require.Equal(t, before, f.Stats().Calls["TicketBroker.getSenderInfo"])
		})
	}
}

type payerGate struct {
	next             http.RoundTripper
	entered, release chan struct{}
	active           atomic.Int32
	once             bool
	used             atomic.Bool
}

func (g *payerGate) RoundTrip(r *http.Request) (*http.Response, error) {
	if g.once && g.used.Swap(true) {
		return g.next.RoundTrip(r)
	}
	response, err := g.next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	g.active.Add(1)
	defer g.active.Add(-1)
	g.entered <- struct{}{}
	<-g.release // Hold even a cancelled transport until its actual exit.
	if err := r.Context().Err(); err != nil {
		response.Body.Close()
		return nil, err
	}
	return response, nil
}

func TestPayerColdCompletionCannotRestoreInvalidatedFunds(t *testing.T) {
	for _, name := range []string{"unchanged", "funded", "registry-target"} {
		t.Run(name, func(t *testing.T) {
			changed := name != "unchanged"
			f, o, now := newTestObserver(t)
			require.NoError(t, o.refresh(t.Context()))
			g := gatePayerReads(o)
			g.once = true
			payer := common.HexToAddress("0x11")
			done := make(chan error, 1)
			go func() { _, err := o.senderInfo(t.Context(), payer, common.Address{}); done <- err }()
			<-g.entered
			advanceObserver(f, *now)
			if changed {
				f.Update(func(s *test.ChainState) {
					s.Deposit = 0
					s.Logs = []types.Log{{Address: s.Contracts["TicketBroker"], BlockHash: s.Hash, BlockNumber: s.Number, Topics: []common.Hash{o.chain.Contracts.abis["ticketBroker"].Events["DepositFunded"].ID, common.BytesToHash(payer.Bytes())}}}
					if name == "registry-target" {
						s.Logs[0].Address, s.Logs[0].Topics[0] = f.Controller, o.chain.Contracts.abis["controller"].Events["SetContractInfo"].ID
					}
				})
			}
			require.NoError(t, o.refresh(t.Context()))
			require.Contains(t, f.Stats().LogAddresses, f.Controller)
			close(g.release)
			err := <-done
			if changed {
				require.ErrorIs(t, err, ErrObservationUnavailable)
			} else {
				require.NoError(t, err)
			}
			info, err := o.senderInfo(t.Context(), payer, common.Address{})
			require.NoError(t, err)
			require.Equal(t, f.State().Number, info.Snapshot.Number.Uint64())
			if changed {
				require.Zero(t, info.Deposit.Sign())
			}
		})
	}
}

func TestPayerColdReadRecoversAtSameHead(t *testing.T) {
	for _, timing := range []string{"epoch", "rpc-before", "rpc-during", "rpc-after"} {
		t.Run(timing, func(t *testing.T) {
			f, o, _ := newTestObserver(t)
			require.NoError(t, o.refresh(t.Context()))
			head := f.State()
			if timing != "epoch" {
				f.Update(func(s *test.ChainState) { s.Fail = "TicketBroker.getSenderInfo" })
			}
			g := gatePayerReads(o)
			g.once = true
			payer := common.HexToAddress("0x11")
			done := make(chan error, 1)
			go func() { _, err := o.senderInfo(t.Context(), payer, common.Address{}); done <- err }()
			<-g.entered
			release := func() {
				close(g.release)
				err := <-done
				require.Error(t, err)
				if timing == "epoch" {
					require.ErrorIs(t, err, ErrObservationUnavailable)
				}
			}
			if timing == "rpc-before" {
				release()
				before := f.Stats()
				_, err := o.senderInfo(t.Context(), payer, common.Address{})
				require.Error(t, err)
				require.Equal(t, before, f.Stats(), "same-generation failures suppress repeated reads")
			}
			f.Update(func(s *test.ChainState) { s.Number--; s.Hash = common.HexToHash("0x10") })
			require.ErrorIs(t, o.refresh(t.Context()), ErrObservationUnavailable)
			if timing == "epoch" || timing == "rpc-during" {
				release()
			}
			f.Update(func(s *test.ChainState) { *s = head })
			require.NoError(t, o.refresh(t.Context()))
			if timing == "rpc-after" {
				release()
			}
			_, err := o.senderInfo(t.Context(), payer, common.Address{})
			require.NoError(t, err, "a failure from an old epoch must not poison a restored-head payer")
		})
	}
}

func gatePayerReads(o *PaymentObserver) *payerGate {
	client := o.chain.Contracts.RPC.http
	g := &payerGate{next: client.Transport, entered: make(chan struct{}, 16), release: make(chan struct{})}
	client.Transport = g
	return g
}

func TestPayerColdReadsBoundDistinctCancellation(t *testing.T) {
	f, o, _ := newTestObserver(t)
	warm := common.HexToAddress("0xffff")
	warmPayer(t, o, warm, common.Address{})
	g := gatePayerReads(o)
	defer func() { close(g.release); o.payerReads.Wait() }()
	for n := range payerReadLimit {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := o.senderInfo(ctx, common.BigToAddress(big.NewInt(int64(n+1))), common.Address{})
			done <- err
		}()
		select {
		case <-g.entered:
		case <-time.After(time.Second):
			t.Fatal("cold read did not start")
		}
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	}
	before := f.Stats()
	for n := payerReadLimit + 1; n <= payerEntries+payerReadLimit+1; n++ {
		_, err := o.senderInfo(t.Context(), common.BigToAddress(big.NewInt(int64(n))), common.Address{})
		require.ErrorIs(t, err, ErrObservationUnavailable)
	}
	require.Equal(t, payerReadLimit+1, len(o.payers), "rejected keys must not grow or evict the cache")
	_, err := o.senderInfo(t.Context(), warm, common.Address{})
	require.NoError(t, err)
	require.Equal(t, before, f.Stats(), "a warm payer remains available without RPCs at saturation")
	require.Equal(t, int32(payerReadLimit), g.active.Load())
	o.mu.RLock()
	require.Equal(t, payerReadLimit, o.activeReads, "cancelled transports still own their slots")
	o.mu.RUnlock()
}

func TestPayerColdReadSharedWaiters(t *testing.T) {
	_, o, _ := newTestObserver(t)
	require.NoError(t, o.refresh(t.Context()))
	g := gatePayerReads(o)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 2)
	call := func(ctx context.Context) {
		_, err := o.senderInfo(ctx, common.HexToAddress("0x11"), common.Address{})
		done <- err
	}
	go call(ctx)
	<-g.entered
	go call(t.Context())
	require.Eventually(t, func() bool {
		o.mu.RLock()
		defer o.mu.RUnlock()
		return o.payers[payerKey{payer: common.HexToAddress("0x11")}].job.waiters == 2
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	close(g.release)
	require.NoError(t, <-done)
	o.payerReads.Wait()
	require.Zero(t, o.activeReads)
}

func TestPayerCacheEvictionAndIdle(t *testing.T) {
	f, o, now := newTestObserver(t)
	require.NoError(t, o.refresh(t.Context()))
	for n := 1; n <= payerEntries+1; n++ {
		*now = now.Add(time.Millisecond)
		_, err := o.senderInfo(t.Context(), common.BigToAddress(big.NewInt(int64(n))), common.Address{})
		require.NoError(t, err)
	}
	require.Len(t, o.payers, payerEntries)
	require.NotContains(t, o.payers, payerKey{payer: common.BigToAddress(big.NewInt(1))})
	*now = now.Add(payerIdle)
	advanceObserver(f, *now)
	require.NoError(t, o.refresh(t.Context()))
	_, err := o.senderInfo(t.Context(), common.HexToAddress("0xffff"), common.Address{})
	require.NoError(t, err)
	require.Len(t, o.payers, 1)
}

func TestPayerShutdownJoinsColdReads(t *testing.T) {
	_, o, _ := newTestObserver(t)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() { o.Run(ctx); close(stopped) }()
	require.Eventually(t, func() bool { _, err := o.Snapshot(t.Context()); return err == nil }, time.Second, time.Millisecond)
	g := gatePayerReads(o)
	returned := make(chan error, 1)
	go func() {
		_, err := o.senderInfo(t.Context(), common.HexToAddress("0x11"), common.Address{})
		returned <- err
	}()
	<-g.entered
	cancel()
	select {
	case <-stopped:
		t.Fatal("observer stopped before its cold RPC exited")
	default:
	}
	close(g.release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("cold read outlived observer shutdown")
	}
	require.Error(t, <-returned)
	require.Zero(t, g.active.Load())
	_, err := o.senderInfo(t.Context(), common.HexToAddress("0x11"), common.Address{})
	require.ErrorIs(t, err, ErrObservationUnavailable)
}

func TestPayerReorgDuringLogsRejectsCandidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		gap  uint64
		fail string
	}{{"child", 1, ""}, {"child-log-failure", 1, "eth_getLogs"}, {"range", 20, ""}} {
		t.Run(tc.name, func(t *testing.T) {
			f, o, now := newTestObserver(t)
			o.recipient = common.HexToAddress("0x22")
			payer := common.HexToAddress("0x11")
			old := warmPayer(t, o, payer, o.recipient).Snapshot
			for range tc.gap {
				advanceObserver(f, *now)
			}
			f.Update(func(s *test.ChainState) {
				s.Hash, s.Fail = common.HexToHash("0x200"), tc.fail
			})
			client := o.chain.Contracts.RPC.http
			next := client.Transport
			client.Transport = reorgLogTransport{next, func() {
				f.Update(func(s *test.ChainState) {
					s.Hash, s.Deposit, s.Fail = common.HexToHash("0x201"), 0, ""
					s.Logs = []types.Log{{Address: s.Contracts["TicketBroker"], BlockHash: s.Hash, BlockNumber: s.Number, Topics: []common.Hash{o.chain.Contracts.abis["ticketBroker"].Events["Withdrawal"].ID, common.BytesToHash(payer.Bytes())}}}
				})
			}}
			require.ErrorIs(t, o.refresh(t.Context()), ErrObservationUnavailable)
			require.Equal(t, old.BlockHash, o.current.BlockHash, "orphaned candidates publish no state")
			require.ErrorIs(t, old.Validate(), ErrObservationUnavailable)
			_, err := o.senderInfo(t.Context(), payer, o.recipient)
			require.ErrorIs(t, err, ErrObservationUnavailable)
			client.Transport = next
			require.NoError(t, o.refresh(t.Context()))
			info, err := o.senderInfo(t.Context(), payer, o.recipient)
			require.NoError(t, err)
			require.Zero(t, info.Deposit.Sign(), "recovery reads replacement-chain funds")
			require.Equal(t, f.State().Hash, info.Snapshot.BlockHash)
			before := f.Stats()
			_, err = o.senderInfo(t.Context(), payer, o.recipient)
			require.NoError(t, err)
			require.Equal(t, before, f.Stats(), "warm reads remain local after recovery")
		})
	}
}

type reorgLogTransport struct {
	http.RoundTripper
	reorg func()
}

func (tr reorgLogTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	request, err := readPayerHistoryCall(r)
	if err != nil {
		return nil, err
	}
	response, err := tr.RoundTripper.RoundTrip(r)
	if request.Method == "eth_getLogs" {
		// Hash filters may still return retained orphan logs after a reorg.
		tr.reorg()
	}
	return response, err
}

type payerHistoryRoute func(*http.Request) (*http.Response, error)

func (fn payerHistoryRoute) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

type payerHistoryCall struct {
	ID     json.RawMessage
	Method string
	Params []json.RawMessage
}

func readPayerHistoryCall(r *http.Request) (call payerHistoryCall, err error) {
	body, err := r.GetBody()
	if err != nil {
		return call, err
	}
	defer body.Close()
	err = json.NewDecoder(body).Decode(&call)
	return call, err
}

func TestPayerForksAndResets(t *testing.T) {
	for _, tc := range []struct {
		name             string
		gap              int
		displaced, split bool
	}{
		{"child", 1, false, false},
		{"gap", 20, false, false},
		{"split-child", 1, false, true},
		{"split-gap", 20, false, true},
		{"complete-proof", 2, true, false},
		{"empty-proof", 2, true, false},
		{"round", 2, true, false},
		{"registry-event", 2, true, false},
		{"logs-error", 2, true, false},
		{"missing-parent", 2, true, false},
		{"beyond-bound", 129, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, o, now := newTestObserver(t)
			payer := common.HexToAddress("0x11")
			held := warmPayer(t, o, payer, common.Address{})
			fork := test.NewChainRPC(t)
			if tc.displaced {
				fork.Update(func(s *test.ChainState) { s.Hash, s.Deposit = common.HexToHash("0xb00"), 0 })
			}
			for n := 0; n < tc.gap; n++ {
				if !tc.displaced {
					advanceObserver(f, *now)
					f.Update(func(s *test.ChainState) {
						s.Deposit = 0
						if n == 0 {
							s.Logs = []types.Log{{Address: s.Contracts["TicketBroker"], BlockHash: s.Hash, BlockNumber: s.Number, Topics: []common.Hash{o.chain.Contracts.abis["ticketBroker"].Events["Withdrawal"].ID, common.BytesToHash(payer.Bytes())}}}
						}
					})
				}
				advanceObserver(fork, *now)
				fork.Update(func(s *test.ChainState) {
					s.Hash = common.BigToHash(new(big.Int).SetUint64(s.Number + 100000))
					if tc.name == "empty-proof" {
						s.LogsBloom = types.Bloom{}
					}
				})
			}
			fork.Update(func(s *test.ChainState) {
				switch tc.name {
				case "round":
					s.CurrentRound++
				case "registry-event":
					s.Logs = []types.Log{{Address: fork.Controller, BlockHash: s.Hash, BlockNumber: s.Number, Topics: []common.Hash{o.chain.Contracts.abis["controller"].Events["SetContractInfo"].ID}}}
				case "logs-error":
					s.Fail = "eth_getLogs"
				}
			})
			endpoint, err := url.Parse(fork.URL)
			require.NoError(t, err)
			client := o.chain.Contracts.RPC.http
			next := client.Transport
			revoked := tc.displaced || (tc.split && tc.gap > 1)
			validateHeld := func() {
				if revoked {
					require.ErrorIs(t, held.Snapshot.Validate(), ErrObservationUnavailable)
				} else {
					require.NoError(t, held.Snapshot.Validate(), "complete canonical ancestry preserves the generation")
				}
			}
			checked := false
			client.Transport = payerHistoryRoute(func(r *http.Request) (*http.Response, error) {
				request, err := readPayerHistoryCall(r)
				if err != nil {
					return nil, err
				}
				if request.Method == "eth_getBlockByNumber" {
					var ref string
					if err := json.Unmarshal(request.Params[0], &ref); err != nil {
						return nil, err
					}
					if ref == hexutil.EncodeBig(held.Snapshot.Number) || (tc.name == "missing-parent" && ref == hexutil.EncodeBig(new(big.Int).Add(held.Snapshot.Number, big.NewInt(1)))) {
						return next.RoundTrip(r)
					}
					if ref == hexutil.EncodeBig(new(big.Int).Add(held.Snapshot.Number, big.NewInt(int64(tc.gap)))) {
						validateHeld()
						require.Equal(t, !revoked, o.Status().Healthy, "revocation precedes the next RPC")
						checked = true
					}
				}
				if tc.displaced || (tc.split && request.Method == "eth_getLogs") {
					r = r.Clone(r.Context())
					r.URL, r.Host = endpoint, endpoint.Host
				}
				return next.RoundTrip(r)
			})
			require.NoError(t, o.refresh(t.Context()))
			require.True(t, checked)
			validateHeld()
			if tc.split {
				require.Positive(t, fork.Stats().Calls["eth_getLogs"])
			}
			funds, err := o.senderInfo(t.Context(), payer, common.Address{})
			require.NoError(t, err)
			require.Zero(t, funds.Deposit.Sign(), "fork-specific history cannot retain pre-withdrawal funds")
			canonical := f.State().Hash
			if tc.displaced {
				canonical = fork.State().Hash
			}
			require.Equal(t, canonical, funds.Snapshot.BlockHash)
			beforeA, beforeB := f.Stats(), fork.Stats()
			_, err = o.senderInfo(t.Context(), payer, common.Address{})
			require.NoError(t, err)
			require.Equal(t, beforeA, f.Stats(), "recovered warm funds stay local")
			require.Equal(t, beforeB, fork.Stats())
		})
	}
}

func TestPayerHistoryCostAndFreshness(t *testing.T) {
	for _, mode := range []string{"empty", "unrelated", "positive", "missing", "null"} {
		for _, gap := range []int{1, 20, 128} {
			t.Run(fmt.Sprintf("%s/%d", mode, gap), func(t *testing.T) {
				f, o, now := newTestObserver(t)
				o.recipient = common.HexToAddress("0x22")
				payer := common.HexToAddress("0x11")
				initial := warmPayer(t, o, payer, o.recipient)
				for range gap {
					advanceObserver(f, *now)
					f.Update(func(s *test.ChainState) { payerHistoryBloom(s, mode) })
				}
				f.Update(func(s *test.ChainState) {
					s.Before = func(context.Context, string) { *now = now.Add(110 * time.Millisecond) }
				})
				f.Reset()
				started := *now
				require.NoError(t, o.refresh(t.Context()))
				stats := f.Stats()
				logs := gap
				if mode == "empty" || mode == "unrelated" {
					logs = 0
				}
				require.Equal(t, logs, stats.Calls["eth_getLogs"])
				headers := gap + 1
				if gap > 1 {
					headers++
				} // Existing retained-anchor availability check.
				require.Equal(t, headers, stats.Calls["eth_getBlockByNumber"])
				require.Equal(t, headers+logs+7, stats.RoundTrips)
				require.Equal(t, time.Duration(stats.RoundTrips)*110*time.Millisecond, now.Sub(started))
				before := f.Stats()
				funds, err := o.senderInfo(t.Context(), payer, o.recipient)
				require.NoError(t, err)
				require.Equal(t, initial.Deposit, funds.Deposit)
				require.Equal(t, before, f.Stats(), "complete unchanged history retains warm funds")
				t.Logf("mode=%s gap=%d total=%d simulated110ms=%s", mode, gap, stats.RoundTrips, now.Sub(started))
			})
		}
	}
}

func TestPayerMalformedBloom(t *testing.T) {
	for _, intermediate := range []bool{false, true} {
		t.Run(fmt.Sprint(intermediate), func(t *testing.T) {
			f, o, now := newTestObserver(t)
			payer := common.HexToAddress("0x11")
			held := warmPayer(t, o, payer, common.Address{})
			advanceObserver(f, *now)
			f.Update(func(s *test.ChainState) {
				payerHistoryBloom(s, "malformed")
				s.Deposit = 0
			})
			if intermediate {
				advanceObserver(f, *now)
				f.Update(func(s *test.ChainState) { payerHistoryBloom(s, "empty") })
				require.NoError(t, o.refresh(t.Context()))
				require.ErrorIs(t, held.Snapshot.Validate(), ErrObservationUnavailable)
				funds, err := o.senderInfo(t.Context(), payer, common.Address{})
				require.NoError(t, err)
				require.Zero(t, funds.Deposit.Sign(), "malformed history forces exact funds recovery")
			} else {
				require.Error(t, o.refresh(t.Context()))
				require.Equal(t, held.Snapshot.BlockHash, o.current.BlockHash, "malformed candidate cannot publish")
			}
		})
	}
}

func TestPayerIncompleteHistoryResets(t *testing.T) {
	for _, mode := range []string{"parent-height", "parent-hash", "log-height", "log-hash"} {
		t.Run(mode, func(t *testing.T) {
			f, o, now := newTestObserver(t)
			payer := common.HexToAddress("0x11")
			held := warmPayer(t, o, payer, common.Address{})
			anchor := f.State()
			advanceObserver(f, *now)
			parent := f.State()
			advanceObserver(f, *now)
			head := f.State()
			method, ref := "eth_getLogs", ""
			var result any
			if mode == "parent-height" || mode == "parent-hash" {
				method, ref = "eth_getBlockByNumber", hexutil.EncodeUint64(parent.Number)
				height, hash := parent.Number, parent.Hash
				if mode == "parent-height" {
					height++
				} else {
					hash = anchor.Hash
				}
				result = map[string]any{"number": hexutil.EncodeUint64(height), "hash": hash, "parentHash": parent.Parent}
			} else {
				log := types.Log{Address: head.Contracts["TicketBroker"], BlockHash: head.Hash, BlockNumber: head.Number, Topics: []common.Hash{o.chain.Contracts.abis["ticketBroker"].Events["DepositFunded"].ID, common.HexToHash("0x12")}}
				if mode == "log-height" {
					log.BlockNumber--
				} else {
					log.BlockHash = parent.Hash
				}
				result = []types.Log{log}
			}
			payload, err := json.Marshal(result)
			require.NoError(t, err)
			client := o.chain.Contracts.RPC.http
			next := client.Transport
			client.Transport = payerHistoryRoute(func(r *http.Request) (*http.Response, error) {
				request, err := readPayerHistoryCall(r)
				if err != nil {
					return nil, err
				}
				if request.Method != method {
					return next.RoundTrip(r)
				}
				if ref != "" {
					var requested string
					if err := json.Unmarshal(request.Params[0], &requested); err != nil {
						return nil, err
					}
					if requested != ref {
						return next.RoundTrip(r)
					}
				}
				data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": json.RawMessage(payload)})
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
			})
			require.NoError(t, o.refresh(t.Context()))
			require.ErrorIs(t, held.Snapshot.Validate(), ErrObservationUnavailable)
			before := f.Stats().Calls["TicketBroker.getSenderInfo"]
			_, err = o.senderInfo(t.Context(), payer, common.Address{})
			require.NoError(t, err)
			require.Equal(t, before+1, f.Stats().Calls["TicketBroker.getSenderInfo"], "incomplete proof must reset even an unaffected payer")
		})
	}
}
