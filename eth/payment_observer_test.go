package eth

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

// Initialization, round rollback and cancellation cases adapt go-livepeer
// eth/watchers/timewatcher_test.go and eth/blockwatch/block_watcher_test.go at
// 597f882cb9f4d42ad8430b0f592afd5929af4a6b, by Nico Vergauwen, Yondon Fu and
// Rafał Leszko. Pinned-copy/expiry cases also adapt Mike Zupper's PR #1
// eth/chainview_test.go at bbe126e. MIT notice in LICENSE.
func newTestObserver(t *testing.T) (*test.ChainRPC, *PaymentObserver, *time.Time) {
	t.Helper()
	f := test.NewChainRPC(t)
	rpc, err := OpenRPC(f.URL)
	require.NoError(t, err)
	t.Cleanup(rpc.Close)
	c, err := NewContracts(rpc, f.Controller)
	require.NoError(t, err)
	o := NewPaymentObserver(PaymentChain{Contracts: c}, true, common.Address{})
	o.lifecycle = t.Context()
	now := time.Now().Truncate(time.Second)
	o.now = func() time.Time { return now }
	return f, o, &now
}

func advanceObserver(f *test.ChainRPC, now time.Time) {
	f.Update(func(s *test.ChainState) {
		s.Parent, s.Number, s.Timestamp = s.Hash, s.Number+1, uint64(now.Unix())
		s.Hash = common.BigToHash(new(big.Int).SetUint64(s.Number))
		s.L1Block++
		s.Logs, s.LogsBloom = nil, nil
	})
}

func TestObserverLocalImmutableSnapshot(t *testing.T) {
	f, o, _ := newTestObserver(t)
	_, err := o.Snapshot(t.Context())
	require.ErrorIs(t, err, ErrObservationUnavailable)
	f.Update(func(s *test.ChainState) { s.CurrentRound = 6 })
	require.NoError(t, o.refresh(t.Context()))
	before := f.Stats()
	for range 10 {
		s, err := o.Snapshot(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(50), s.Block.Int64(), "L2 height is not the protocol clock")
		require.Equal(t, int64(5), s.Round.Int64())
		require.Equal(t, int64(6), s.CurrentRound.Int64())
		s.Block.SetInt64(0)
		s.Round.SetInt64(0)
		s.CurrentRound.SetInt64(0)
	}
	require.Equal(t, before, f.Stats(), "warm snapshots and status must be local")
	require.True(t, o.Status().Healthy)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = o.Snapshot(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestObserverExplicitArbitrumChainIDRequiresL1(t *testing.T) {
	f, original, _ := newTestObserver(t)
	f.Update(func(s *test.ChainState) { s.ChainID, s.OmitL1 = 42161, true })
	rpc := original.chain.Contracts.RPC
	require.NoError(t, rpc.CheckChainID(t.Context(), big.NewInt(42161)))
	o := NewPaymentObserver(original.chain, false, common.Address{}) // No named network preset.
	require.ErrorIs(t, o.refresh(t.Context()), ErrObservationUnavailable)
	// A non-Arbitrum custom chain continues to use its native block clock.
	f.Update(func(s *test.ChainState) { s.ChainID = 1 })
	rpc.requireL1 = false
	require.NoError(t, rpc.CheckChainID(t.Context(), big.NewInt(1)))
	o = NewPaymentObserver(original.chain, false, common.Address{})
	require.NoError(t, o.refresh(t.Context()))
}

func TestObserverContactDoesNotRenewOldHead(t *testing.T) {
	f, o, now := newTestObserver(t)
	require.NoError(t, o.refresh(t.Context()))
	initial := o.Status()
	old, err := o.Snapshot(t.Context())
	require.NoError(t, err)
	*now = now.Add(10 * time.Second)
	require.NoError(t, o.refresh(t.Context()))
	status := o.Status()
	require.Equal(t, initial.ObservedAt, status.ObservedAt)
	require.Equal(t, initial.HeadProgressAt, status.HeadProgressAt)
	require.True(t, status.ContactAt.After(initial.ContactAt))
	original := f.State()
	f.Update(func(s *test.ChainState) { s.Number-- })
	require.Error(t, o.refresh(t.Context()))
	f.Update(func(s *test.ChainState) { *s = original })
	require.NoError(t, o.refresh(t.Context()))
	require.True(t, o.Status().Healthy)
	require.ErrorIs(t, old.Validate(), ErrObservationUnavailable)
	require.Equal(t, initial.ObservedAt, o.Status().ObservedAt)
	*now = now.Add(20 * time.Second)
	require.Error(t, o.refresh(t.Context()))
	_, err = o.Snapshot(t.Context())
	require.ErrorIs(t, err, ErrObservationUnavailable)
	advanceObserver(f, *now)
	require.NoError(t, o.refresh(t.Context()))
	require.True(t, o.Status().Healthy)
	*now = now.Add(time.Second)
	advanceObserver(f, now.Add(-time.Minute)) // A responsive, advancing lagging head.
	require.Error(t, o.refresh(t.Context()))
	require.Equal(t, status.ObservedAt.Add(30*time.Second), o.Status().ObservedAt)
}

func TestObserverInvalidHeaders(t *testing.T) {
	for _, change := range []func(*test.ChainState){
		func(s *test.ChainState) { s.OmitL1 = true },
		func(s *test.ChainState) { s.RoundHash = common.Hash{} },
		func(s *test.ChainState) { s.Round = 0 },
		func(s *test.ChainState) { s.CurrentRound = 4 },
		func(s *test.ChainState) { s.Timestamp += 60 },
	} {
		f, o, _ := newTestObserver(t)
		f.Update(change)
		require.Error(t, o.refresh(t.Context()))
		require.False(t, o.Status().Healthy)
	}
}

func TestObserverRejectedHeadersInvalidateConflictingIdentity(t *testing.T) {
	for metadata, change := range map[string]func(*test.ChainState, time.Time){
		"stale":             func(s *test.ChainState, now time.Time) { s.Timestamp = uint64(now.Add(-31 * time.Second).Unix()) },
		"missing timestamp": func(s *test.ChainState, _ time.Time) { s.OmitTimestamp = true },
		"missing L1":        func(s *test.ChainState, _ time.Time) { s.OmitL1 = true },
	} {
		for mode, distance := range map[string]int64{"replacement": 0, "rollback": -1, "parent": 1, "gap": 10, "same": 0} {
			t.Run(metadata+"/"+mode, func(t *testing.T) {
				f, o, now := newTestObserver(t)
				require.NoError(t, o.refresh(t.Context()))
				held, err := o.Snapshot(t.Context())
				require.NoError(t, err)
				f.Update(func(s *test.ChainState) {
					if mode != "same" {
						s.Hash, s.Parent = common.HexToHash("0xdead"), common.HexToHash("0xbad")
					}
					s.Number = uint64(int64(s.Number) + distance)
					change(s, *now)
				})
				f.Reset()
				require.ErrorIs(t, o.refresh(t.Context()), ErrObservationUnavailable)
				if mode == "gap" || mode == "same" {
					require.NoError(t, held.Validate(), "rejected metadata alone cannot orphan retained state")
					require.True(t, o.Status().Healthy)
				} else {
					require.ErrorIs(t, held.Validate(), ErrObservationUnavailable)
					require.False(t, o.Status().Healthy)
					_, err = o.Snapshot(t.Context())
					require.ErrorIs(t, err, ErrObservationUnavailable)
				}
				require.Equal(t, held.ObservedAt(), o.Status().ObservedAt)
				require.Equal(t, 1, f.Stats().RoundTrips, "conflicts must invalidate before any further RPC")
			})
		}
	}
}

func TestObserverL1ProgressDeadline(t *testing.T) {
	for _, clocks := range [][]uint64{{50, 50, 50}, {49, 50, 49}} {
		f, o, now := newTestObserver(t)
		require.NoError(t, o.refresh(t.Context()))
		progress, accepted := o.Status().L1ProgressAt, o.Status().ObservedAt
		for step, clock := range clocks {
			*now = now.Add(10 * time.Second)
			advanceObserver(f, *now)
			f.Update(func(s *test.ChainState) { s.L1Block = clock })
			err := o.refresh(t.Context())
			if step < 2 {
				require.NoError(t, err)
				accepted = o.Status().ObservedAt
			} else {
				require.ErrorIs(t, err, ErrObservationUnavailable)
				require.Equal(t, accepted, o.Status().ObservedAt)
			}
			require.Equal(t, progress, o.Status().L1ProgressAt)
		}
		require.False(t, o.Status().Healthy)
		advanceObserver(f, *now)
		f.Update(func(s *test.ChainState) { s.L1Block = 51 })
		require.NoError(t, o.refresh(t.Context()))
		require.True(t, o.Status().Healthy)
	}
}

func TestObserverExpiredCandidateIsNotPublished(t *testing.T) {
	for _, phase := range []string{"RoundsManager.currentRound", "eth_getLogs"} {
		t.Run(phase, func(t *testing.T) {
			f, o, now := newTestObserver(t)
			require.NoError(t, o.refresh(t.Context()))
			old := o.Status()
			advanceObserver(f, *now)
			f.Update(func(s *test.ChainState) {
				s.Before = func(_ context.Context, name string) {
					if name == phase {
						*now = now.Add(30 * time.Second)
					}
				}
			})
			require.ErrorIs(t, o.refresh(t.Context()), ErrObservationUnavailable)
			status := o.Status()
			require.Equal(t, old.ObservedAt, status.ObservedAt)
			require.Equal(t, old.HeadProgressAt, status.HeadProgressAt)
			require.Equal(t, old.L1ProgressAt, status.L1ProgressAt)
			require.Equal(t, int64(50), o.current.Block.Int64())
		})
	}
}

func TestObserverPinnedBackfillAndReorg(t *testing.T) {
	f, o, now := newTestObserver(t)
	require.NoError(t, o.refresh(t.Context()))
	old, err := o.Snapshot(t.Context())
	require.NoError(t, err)
	advanceObserver(f, *now)
	f.Update(func(s *test.ChainState) { s.Fail = "RoundsManager.blockHashForRound"; s.Round, s.CurrentRound = 6, 7 })
	require.Error(t, o.refresh(t.Context()))
	s, err := o.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, old.Round, s.Round, "failed candidates publish no partial round state")
	f.Update(func(s *test.ChainState) { s.Number += 10; s.Hash = common.HexToHash("0xbeef"); s.Fail = "" })
	f.Update(func(s *test.ChainState) {
		s.Before = func(_ context.Context, name string) {
			if name == "RoundsManager.blockHashForRound" {
				f.Update(func(s *test.ChainState) { s.Hash = common.HexToHash("0xbad") })
			}
		}
	})
	require.Error(t, o.refresh(t.Context()), "a reorg during pinned reads rejects the candidate")
	require.NoError(t, s.Validate(), "an orphaned candidate does not orphan its proven canonical ancestor")
	f.Update(func(s *test.ChainState) { s.Before = nil })
	require.NoError(t, o.refresh(t.Context()))
}

func TestObserverCancellationJoinsRPC(t *testing.T) {
	f, o, _ := newTestObserver(t)
	o.now = time.Now
	entered, exited := make(chan struct{}), make(chan struct{})
	f.Update(func(s *test.ChainState) {
		s.Before = func(ctx context.Context, _ string) {
			close(entered)
			<-ctx.Done()
			close(exited)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { o.Run(ctx); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("observer did not stop")
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("RPC outlived observer cancellation")
	}
	require.False(t, o.Status().Healthy)
	require.Equal(t, 1, f.Stats().RoundTrips)
}

func TestObserverGapKeepsCanonicalStateAvailable(t *testing.T) {
	f, o, _ := newTestObserver(t)
	require.NoError(t, o.refresh(t.Context()))
	for _, mode := range []string{"gap", "replacement", "parent"} {
		old, err := o.Snapshot(t.Context())
		require.NoError(t, err)
		checked := false
		f.Update(func(s *test.ChainState) {
			if mode == "gap" {
				s.Number += 10
				s.L1Block++
			}
			if mode == "parent" {
				s.Number++
			}
			s.Hash = common.BigToHash(big.NewInt(int64(s.Number) + int64(s.L1Block)))
			if mode == "replacement" {
				s.Hash = common.HexToHash("0xf00d")
				s.Round, s.CurrentRound = 4, 5
			}
			s.Before = func(_ context.Context, name string) {
				if name == "RoundsManager.lastInitializedRound" {
					checked = true
					if mode != "gap" {
						require.ErrorIs(t, old.Validate(), ErrObservationUnavailable)
						require.False(t, o.Status().Healthy)
						require.Equal(t, 1, f.Stats().Calls["eth_getBlockByNumber"], "proven reorgs must invalidate before any anchor probe")
					} else {
						require.NoError(t, old.Validate())
						require.True(t, o.Status().Healthy)
						_, err := o.Snapshot(t.Context())
						require.NoError(t, err)
					}
				}
			}
		})
		f.Reset()
		require.NoError(t, o.refresh(t.Context()))
		require.True(t, checked, "availability must be checked before the dependent read completes")
		f.Update(func(s *test.ChainState) { s.Before = nil })
		if mode == "replacement" {
			require.Equal(t, int64(4), o.current.Round.Int64(), "removed round state is backfilled")
		}
	}
}

// Round boundary/removal cases adapt Nico Vergauwen's go-livepeer
// eth/watchers/orchestratorwatcher_test.go at 597f882cb9f4d42ad8430b0f592afd5929af4a6b.
// Local/pinned failure isolation adapts Mike Zupper's PR #1 eth/chainview_test.go
// at bbe126e5eddbf59f9cc9f94d3c37703215c26850. MIT notice in LICENSE.
func TestObserverRecipientActivity(t *testing.T) {
	f, o, now := newTestObserver(t)
	o.recipient = common.HexToAddress("0x1234")
	c := PaymentChain{Contracts: o.chain.Contracts, Observer: o}
	f.Update(func(s *test.ChainState) { s.Recipient, s.Activation, s.Deactivation = o.recipient, 6, 8 })
	_, err := c.IsActive(t.Context(), o.recipient)
	require.ErrorIs(t, err, ErrObservationUnavailable)
	for _, step := range []struct {
		round  uint64
		active bool
	}{{5, false}, {6, true}, {7, true}, {8, false}} {
		f.Update(func(s *test.ChainState) { s.CurrentRound = step.round })
		require.NoError(t, o.refresh(t.Context()))
		snapshot, err := c.Snapshot(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 5, snapshot.Round.Int64(), "activity uses current round even before initialization")
		before := f.Stats()
		active, err := c.IsActiveAt(t.Context(), o.recipient, snapshot)
		require.NoError(t, err)
		require.Equal(t, step.active, active)
		_, err = c.IsActiveAt(t.Context(), common.HexToAddress("0x9999"), snapshot)
		require.ErrorIs(t, err, ErrObservationUnavailable)
		unowned := snapshot
		unowned.observer = nil
		_, err = c.IsActiveAt(t.Context(), o.recipient, unowned)
		require.ErrorIs(t, err, ErrObservationUnavailable)
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = c.IsActiveAt(cancelled, o.recipient, snapshot)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, before, f.Stats(), "local checks cannot start activity or lookup RPCs")
		advanceObserver(f, *now)
		f.Update(func(s *test.ChainState) { s.Deactivation = 0 })
		require.NoError(t, o.refresh(t.Context()))
		active, err = c.IsActiveAt(t.Context(), o.recipient, snapshot)
		require.NoError(t, err)
		require.Equal(t, step.active, active, "a newer activity result cannot change a held funds observation")
		advanceObserver(f, *now)
		f.Update(func(s *test.ChainState) { s.Deactivation = 8 })
	}

	f.Update(func(s *test.ChainState) { s.Fail = "BondingManager.isActiveTranscoder" })
	require.NoError(t, o.refresh(t.Context()), "a recipient failure must not reject unrelated chain state")
	require.True(t, o.Status().Healthy)
	_, err = c.PayerFunds(t.Context(), common.HexToAddress("0x2000"), o.recipient)
	require.NoError(t, err)
	_, err = c.IsActive(t.Context(), o.recipient)
	require.Error(t, err, "unknown must remain distinct from known inactive")
	advanceObserver(f, *now)
	f.Update(func(s *test.ChainState) { s.Fail, s.CurrentRound, s.Deactivation = "", 6, 8 })
	require.NoError(t, o.refresh(t.Context()))
	old, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	f.Update(func(s *test.ChainState) {
		s.Hash, s.Parent, s.Deactivation = common.HexToHash("0xdead"), common.Hash{}, 6
	})
	require.NoError(t, o.refresh(t.Context()))
	_, err = c.IsActiveAt(t.Context(), o.recipient, old)
	require.ErrorIs(t, err, ErrObservationUnavailable)
	active, err := c.IsActive(t.Context(), o.recipient)
	require.NoError(t, err)
	require.False(t, active, "removed activation state is replaced from the canonical block")
	*now = now.Add(30 * time.Second)
	_, err = c.IsActive(t.Context(), o.recipient)
	require.ErrorIs(t, err, ErrObservationUnavailable)
}
