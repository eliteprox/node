package redemption

import (
	"context"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	rpcfixture "github.com/livepeer/node/internal/redemptiontest"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

type recoveryRPC struct{ *rpcfixture.RedemptionRPC }

func newRecoveryRPC(t *testing.T) *recoveryRPC { return &recoveryRPC{rpcfixture.NewRedemptionRPC(t)} }
func recoveryKey(t *testing.T) *eth.Key        { return rpcfixture.RedemptionKey(t) }
func (f *recoveryRPC) store(t *testing.T, id byte) (*RedeemerDB, *pm.SignedTicket) {
	t.Helper()
	store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ticket := &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: common.HexToAddress("0x1234"), Recipient: common.HexToAddress("0x5678"), FaceValue: big.NewInt(1000), WinProb: big.NewInt(100), TicketNonce: 1, RecipientRandHash: crypto.Keccak256Hash([]byte{id}), CreationRound: 1, ParamsExpirationBlock: big.NewInt(10)}, Sig: []byte{id, 2, 3}, RecipientRand: big.NewInt(int64(id))}
	require.NoError(t, store.StoreWinningTicket(ticket))
	f.Mu.Lock()
	f.Tickets = append(f.Tickets, ticket)
	f.Mu.Unlock()
	return store, ticket
}
func ticketPhase(t *testing.T, s *RedeemerDB, sig []byte) string {
	t.Helper()
	var phase string
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_attempts WHERE sig=?", sig).Scan(&phase))
	return phase
}
func tickRecovery(t *testing.T, s *RedeemerDB, c eth.PaymentChain, k *eth.Key) {
	t.Helper()
	require.Empty(t, ProcessRedemptions(t.Context(), s, c, k, big.NewInt(1)))
}

// newRecoveryTest shares setup without starting recovery or changing RPC behavior.
func newRecoveryTest(t *testing.T, id byte) (*recoveryRPC, *RedeemerDB, *pm.SignedTicket, eth.PaymentChain, *eth.Key) {
	t.Helper()
	f := newRecoveryRPC(t)
	store, ticket := f.store(t, id)
	return f, store, ticket, f.Chain(t), recoveryKey(t)
}

func TestSharedWalletRedemptionRecovery(t *testing.T) {
	f := newRecoveryRPC(t)
	f.NonceBarrier = make(chan struct{})
	key := recoveryKey(t)
	a, ta := f.store(t, 1)
	b, tb := f.store(t, 2)
	ca, cb := f.Chain(t), f.Chain(t)
	results := make(chan []error, 2)
	go func() { results <- ProcessRedemptions(t.Context(), a, ca, key, big.NewInt(1)) }()
	go func() { results <- ProcessRedemptions(t.Context(), b, cb, key, big.NewInt(1)) }()
	first, second := <-results, <-results
	require.Equal(t, 1, len(first)+len(second))
	sent := f.Transactions()
	require.Len(t, sent, 2)
	require.EqualValues(t, 0, sent[0].Nonce())
	require.EqualValues(t, 0, sent[1].Nonce())
	require.NotEqual(t, sent[0].Hash(), sent[1].Hash())
	// The losing process discovers pending nonce 1 and retries its own ticket.
	loser, lc, lt := a, ca, ta
	if ticketPhase(t, b, tb.Sig) == "retryable" {
		loser, lc, lt = b, cb, tb
	}
	floor, err := loser.SavedNonceFloor(t.Context(), key.Address())
	require.NoError(t, err)
	require.Zero(t, floor)
	tickRecovery(t, loser, lc, key)
	sent = f.Transactions()
	require.Len(t, sent, 3)
	require.EqualValues(t, 1, sent[2].Nonce())
	require.NotEqual(t, "retryable", ticketPhase(t, loser, lt.Sig))
	f.Mine(sent[0], true)
	f.Mine(sent[2], true)
	tickRecovery(t, a, ca, key)
	tickRecovery(t, b, cb, key)
	require.Equal(t, "confirmed", ticketPhase(t, a, ta.Sig))
	require.Equal(t, "confirmed", ticketPhase(t, b, tb.Sig))
	history, err := loser.Redemptions()
	require.NoError(t, err)
	require.Len(t, history, 2)
}

func TestRejectedRedemptionDoesNotSkipNonce(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 3)
	f.RejectNext = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.Equal(t, "retryable", ticketPhase(t, s, ticket.Sig))
	tickRecovery(t, s, c, key)
	sent := f.Transactions()
	require.Len(t, sent, 2)
	require.EqualValues(t, 0, sent[1].Nonce())
	require.Equal(t, sent[0].Hash(), sent[1].Hash())
}

func TestRedemptionReplacementAndFinality(t *testing.T) {
	for _, winner := range []int{0, 1} {
		t.Run(fmt.Sprintf("winner=%d", winner), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 4)
			f.Finalized = false
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: 1}))
			tickRecovery(t, s, c, key)
			require.Eventually(t, func() bool { return len(f.Transactions()) == 2 }, time.Second, 5*time.Millisecond)
			sent := f.Transactions()
			require.Equal(t, sent[0].Nonce(), sent[1].Nonce())
			require.Equal(t, sent[0].Data(), sent[1].Data())
			require.Equal(t, big.NewInt(224), sent[1].GasFeeCap())
			require.Equal(t, big.NewInt(4), sent[1].GasTipCap())
			f.Mine(sent[winner], true)
			tickRecovery(t, s, c, key)
			require.NotEqual(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "mined" }, time.Second, 5*time.Millisecond)
			f.Mu.Lock()
			f.Finalized = true
			f.Mu.Unlock()
			tickRecovery(t, s, c, key)
			require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			var hash string
			require.NoError(t, s.db.QueryRow("SELECT tx_hash FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&hash))
			require.Equal(t, sent[winner].Hash().Hex(), hash)
			items, err := s.Redemptions()
			require.NoError(t, err)
			require.Len(t, items, 2)
			for i, tx := range sent {
				require.Equal(t, tx.Hash().Hex(), items[i].Hash)
				require.Error(t, RetryRedemption(t.Context(), s, c.Contracts, tx.Hash()))
			}
			require.Len(t, f.Transactions(), 2)
		})
	}
}

func TestRedemptionReplacementFailuresRemainQueued(t *testing.T) {
	for _, scenario := range []string{"limit", "ceiling", "rejection", "persistence"} {
		t.Run(scenario, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 6)
			s.metrics = observability.New("orchestrator")
			options := RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: 1}
			if scenario == "limit" {
				options.MaxReplacements = 0
			}
			if scenario == "rejection" {
				f.RejectSend = 2
				options.MaxReplacements = 3
			}
			if scenario == "ceiling" {
				c.Contracts.MaxFeePerGas = big.NewInt(210)
			}
			if scenario == "persistence" {
				_, err := s.db.Exec(`CREATE TRIGGER reject_replacement BEFORE INSERT ON redemption_transactions WHEN NEW.replaces_hash IS NOT NULL BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
				require.NoError(t, err)
			}
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, options))
			nextResult := captureRecoveryResults(t, s)
			tickRecovery(t, s, c, key)
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, 5*time.Millisecond)
			expected := 1
			if scenario == "rejection" {
				expected = 2
			}
			require.Len(t, f.Transactions(), expected)
			if scenario == "rejection" {
				var acknowledged bool
				require.NoError(t, s.db.QueryRow("SELECT acknowledged FROM redemption_transactions WHERE replaces_hash IS NOT NULL").Scan(&acknowledged))
				require.False(t, acknowledged, "the rejected replacement must stop the remaining replacement budget")
			}
			// A timeout/rejected replacement does not settle the still-live ticket.
			var redeemed *string
			require.NoError(t, s.db.QueryRow("SELECT redeemed_at FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&redeemed))
			require.Nil(t, redeemed)
			result := nextResult()
			require.Error(t, result.Error())
			// A rejected replacement commits retryable before the callback moves
			// the still-submitted predecessor into retryable as well.
			require.EqualValues(t, expected, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_errors_total", nil))
			require.NoError(t, s.recovery.recordResult(t.Context(), result))
			require.EqualValues(t, expected, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}), "replaying a completed callback must not emit another transition")
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_errors_total", nil), "replaying a completed callback must not emit another failure")
			require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
		})
	}
}

func TestLostResponseUsesFinalizedTicketGuard(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 7)
	f.LoseNextResponse = true
	f.Finalized = false
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	sent := f.Transactions()
	f.Mine(sent[0], true)
	tickRecovery(t, s, c, key)
	require.Equal(t, "awaiting_finality", ticketPhase(t, s, ticket.Sig))
	require.Len(t, f.Transactions(), 1)
	f.Mu.Lock()
	f.Finalized = true
	f.Mu.Unlock()
	tickRecovery(t, s, c, key)
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
	require.Len(t, f.Transactions(), 1)
	floor, err := s.SavedNonceFloor(t.Context(), key.Address())
	require.NoError(t, err)
	require.EqualValues(t, 1, floor, "a finalized receipt proves acceptance even when the send response was lost")
}

func TestUsedTicketGuardRecoversAfterReorg(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 8)
	s.metrics = observability.New("orchestrator")
	f.Finalized = false
	f.Used[ticket.Hash()] = true
	tickRecovery(t, s, c, key)
	require.Equal(t, "awaiting_finality", ticketPhase(t, s, ticket.Sig))
	require.Empty(t, f.Transactions())
	tickRecovery(t, s, c, key)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "awaiting_finality"}))
	f.Mu.Lock()
	delete(f.Used, ticket.Hash())
	f.Mu.Unlock()
	tickRecovery(t, s, c, key)
	require.Len(t, f.Transactions(), 1)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_errors_total", nil), "a reorg is not a service failure")
}

func reopenRecoveryStore(t *testing.T, s *RedeemerDB) *RedeemerDB {
	t.Helper()
	var seq int
	var name, path string
	require.NoError(t, s.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path))
	require.NoError(t, s.Close())
	next, err := OpenRedeemerDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, next.Close()) })
	return next
}

func TestRedemptionRestartRestoresTransactionCursor(t *testing.T) {
	for _, boundary := range []string{"prepared", "sending", "submitted", "replacement-prepared", "replacement-submitted"} {
		t.Run(boundary, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 10)
			if boundary == "prepared" {
				prepared, err := c.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
				require.NoError(t, err)
				require.NoError(t, s.RecordPrepared(t.Context(), ticket, prepared))
			} else {
				options := RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}
				if boundary == "replacement-submitted" {
					options.Timeout = 100 * time.Millisecond
				}
				require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, options))
				tickRecovery(t, s, c, key)
				if boundary == "replacement-submitted" {
					require.Eventually(t, func() bool {
						var count int
						err := s.db.QueryRow("SELECT count(*) FROM redemption_transactions WHERE acknowledged=1").Scan(&count)
						return err == nil && count == 2
					}, time.Second, time.Millisecond)
				}
			}
			// Stop the old runtime before editing a durable crash-boundary fixture.
			s = reopenRecoveryStore(t, s)
			if boundary == "sending" {
				_, err := s.db.Exec("UPDATE redemption_attempts SET phase='broadcast'")
				require.NoError(t, err)
				_, err = s.db.Exec("UPDATE redemption_transactions SET phase='sending',acknowledged=0")
				require.NoError(t, err)
			}
			if boundary == "replacement-prepared" {
				first := f.Transactions()[0]
				replacement := recoveryReplacement(t, key, first)
				identity, err := signedIdentity(replacement)
				require.NoError(t, err)
				require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, first.Hash().Hex()))
			}
			expectedSends := 1
			if boundary == "sending" || strings.HasPrefix(boundary, "replacement-") {
				expectedSends = 2
			}
			var originalDeadline string
			if boundary == "sending" {
				require.NoError(t, s.db.QueryRow("SELECT broadcast_at FROM redemption_transactions").Scan(&originalDeadline))
			}
			options := RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}
			if boundary == "replacement-submitted" {
				options.Timeout = time.Nanosecond
			}
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, options))
			tickRecovery(t, s, c, key)
			require.Len(t, f.Transactions(), expectedSends)
			if boundary == "replacement-submitted" {
				require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, time.Millisecond)
				require.Len(t, f.Transactions(), 2, "restart must retain the spent replacement budget")
			}
			if boundary == "sending" {
				var at string
				require.NoError(t, s.db.QueryRow("SELECT broadcast_at FROM redemption_transactions").Scan(&at))
				require.Equal(t, originalDeadline, at, "restarting an uncertain send must preserve its timeout")
				sent := f.Transactions()
				require.Equal(t, sent[0].Hash(), sent[1].Hash())
			}
		})
	}
}

func TestRedemptionRecoveryBeforeAndAfterBroadcast(t *testing.T) {
	for _, failure := range []string{"preparation", "prepared-crash", "uncertain-broadcast"} {
		t.Run(failure, func(t *testing.T) {
			f, store, ticket, chain, key := newRecoveryTest(t, 1)
			f.FinalizedNonce = 1
			if failure == "preparation" {
				f.PrepareError = "preparation unavailable"
			}
			f.LoseNextResponse = failure == "uncertain-broadcast"
			snapshot, err := chain.Snapshot(t.Context())
			require.NoError(t, err)
			var prepared eth.SignedTransaction
			if failure == "prepared-crash" {
				prepared, err = chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
				require.NoError(t, err)
				require.NoError(t, store.RecordPrepared(t.Context(), ticket, prepared))
			} else {
				require.NotEmpty(t, RedeemPending(t.Context(), store, chain, key, big.NewInt(1), snapshot))
			}
			store = reopenRecoveryStore(t, store)
			f.Mu.Lock()
			f.PrepareError = ""
			f.StaleNonce = true
			f.Mu.Unlock()
			restartKey := key
			if failure == "prepared-crash" {
				path, password := test.WriteKeystore(t, nil)
				restartKey, err = eth.OpenKeystoreFile(path, password)
				require.NoError(t, err)
			}
			require.Empty(t, RedeemPending(t.Context(), store, chain, restartKey, big.NewInt(1), snapshot))
			sent := f.Transactions()
			expectedSends := 1
			if failure == "uncertain-broadcast" {
				expectedSends = 2
			}
			require.Len(t, sent, expectedSends, "retryable sends recover automatically")
			if failure == "prepared-crash" {
				raw, err := sent[0].MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, prepared.Raw, raw, "rotated-key recovery must reuse the exact signed bytes")
			} else if failure == "uncertain-broadcast" {
				require.Equal(t, sent[0].Hash(), sent[1].Hash(), "stale pending reads must retain the saved transaction")
			}
			f.Mu.Lock()
			f.MineLocked(sent[len(sent)-1], true)
			f.ReceiptMissingStatus = true
			f.Mu.Unlock()
			before, err := store.Redemptions()
			require.NoError(t, err)
			require.Len(t, before, 1)
			require.NotEmpty(t, ReconcileSubmitted(t.Context(), store, chain), "missing receipt status must fail reconciliation")
			after, err := store.Redemptions()
			require.NoError(t, err)
			require.Equal(t, before, after, "malformed receipts must leave durable attempts unresolved")
			next := nextRecoveryTicket(ticket, 4)
			next.RecipientRandHash = ticket.RecipientRandHash
			next.TicketNonce = 2
			require.NoError(t, store.StoreWinningTicket(next))
			f.Mu.Lock()
			f.ReceiptMissingStatus = false
			f.SnapshotError = true
			f.Mu.Unlock()
			require.NotEmpty(t, ProcessRedemptions(t.Context(), store, chain, restartKey, big.NewInt(1)))
			require.Equal(t, "confirmed", ticketPhase(t, store, ticket.Sig), "reconcile despite an unavailable latest snapshot")
			require.Len(t, f.Transactions(), expectedSends)
			queued, err := store.PendingPayers()
			require.NoError(t, err)
			require.Equal(t, []common.Address{ticket.PayerAddress}, queued)
			require.Error(t, RetryRedemption(t.Context(), store, chain.Contracts, common.HexToHash(before[0].Hash)))
			if failure == "prepared-crash" {
				floor, err := store.SavedNonceFloor(t.Context(), key.Address())
				require.NoError(t, err)
				require.EqualValues(t, 2, floor)
				floor, err = store.SavedNonceFloor(t.Context(), restartKey.Address())
				require.NoError(t, err)
				require.Zero(t, floor)
				store = reopenRecoveryStore(t, store)
				f.Mu.Lock()
				f.SnapshotError = false
				f.Mu.Unlock()
				chain = f.Chain(t)
				require.Empty(t, RedeemPending(t.Context(), store, chain, key, big.NewInt(1), snapshot))
				sent = f.Transactions()
				require.EqualValues(t, 2, sent[len(sent)-1].Nonce(), "confirmed history must advance a fresh client's nonce floor")
			}
		})
	}
}

func TestTerminalAttemptRetainsUnresolvedSibling(t *testing.T) {
	for _, tc := range []struct {
		phase    string
		minRound int64
	}{{"reverted", 0}, {"invalid", 0}, {"expired", 0}, {"expired", 2}} {
		t.Run(fmt.Sprintf("%s/min-round=%d", tc.phase, tc.minRound), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 11)
			f.LoseNextResponse = true
			require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			tickRecovery(t, s, c, key)
			sent := f.Transactions()
			require.Len(t, sent, 2)
			f.Mine(sent[1], false)
			require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			require.Equal(t, "reverted", ticketPhase(t, s, ticket.Sig))
			if tc.phase != "reverted" {
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, tc.phase))
			}
			// The provider has not accounted for the earlier signed identity. The ticket
			// stops retrying, but cannot release its collateral based on one failed hash.
			next := nextRecoveryTicket(ticket, 12)
			next.CreationRound = max(ticket.CreationRound, tc.minRound)
			require.ErrorIs(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, tc.minRound, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), pm.ErrPayerUnavailable)
			f.Mine(sent[0], true)
			tickRecovery(t, s, c, key)
			require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			require.NoError(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, tc.minRound, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()))
		})
	}
}

func TestOrphanedInclusionRecoversSameNonce(t *testing.T) {
	for _, replacements := range []int{0, 1} {
		t.Run(fmt.Sprintf("replacements=%d", replacements), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 13)
			f.Finalized = false
			f.MineAfter = 1
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: replacements}))
			tickRecovery(t, s, c, key)
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "mined" }, time.Second, time.Millisecond)
			f.Mu.Lock()
			f.MineAfter = 0
			f.Receipts = map[common.Hash]*types.Receipt{}
			f.Used = map[common.Hash]bool{}
			f.Mu.Unlock()
			tickRecovery(t, s, c, key)
			if replacements == 0 {
				require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, time.Millisecond)
				status, err := s.Redemptions()
				require.NoError(t, err)
				require.Equal(t, "retryable", status[0].Phase)
				require.NotEmpty(t, status[0].Error)
				require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, f.Transactions()[0].Hash()))
			} else {
				require.Eventually(t, func() bool { return len(f.Transactions()) == 2 }, time.Second, time.Millisecond)
			}
			sent := f.Transactions()
			require.Len(t, sent, 2)
			require.Equal(t, sent[0].Nonce(), sent[1].Nonce())
		})
	}
}

func TestRedemptionPersistencePreventsInitialSend(t *testing.T) {
	f, s, _, c, key := newRecoveryTest(t, 14)
	_, err := s.db.Exec(`CREATE TRIGGER reject_identity BEFORE INSERT ON redemption_transactions BEGIN SELECT RAISE(ABORT,'storage unavailable'); END`)
	require.NoError(t, err)
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.Empty(t, f.Transactions())
	_, err = s.db.Exec("DROP TRIGGER reject_identity")
	require.NoError(t, err)
	tickRecovery(t, s, c, key)
	require.EqualValues(t, 0, f.Transactions()[0].Nonce())
}

func TestManualRedemptionRetryCannotDisplaceActiveIdentity(t *testing.T) {
	f, s, _, c, key := newRecoveryTest(t, 15)
	f.LoseNextResponse = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	first := f.Transactions()[0]
	require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, first.Hash()))
	require.Equal(t, first.Hash(), f.Transactions()[1].Hash())
	// Send another identity only after upstream timeout returns this ticket.
	_, err := s.db.Exec("UPDATE redemption_attempts SET phase='retryable'")
	require.NoError(t, err)
	tickRecovery(t, s, c, key)
	require.Len(t, f.Transactions(), 3)
	require.Error(t, RetryRedemption(t.Context(), s, c.Contracts, first.Hash()))
	require.Len(t, f.Transactions(), 3)
}

func TestExpiredAttemptDoesNotReopenRandomness(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 16)
	f.RejectNext = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, "expired"))
	next := nextRecoveryTicket(ticket, 17)
	next.RecipientRandHash = ticket.RecipientRandHash
	next.TicketNonce = 2
	require.ErrorIs(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, ticket.RecipientRandHash, 0, big.NewInt(10000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), ErrExposedRandomness)
}

func recoveryReplacement(t *testing.T, key *eth.Key, first *types.Transaction) *types.Transaction {
	t.Helper()
	next, err := key.SignTxForChain(types.NewTx(&types.DynamicFeeTx{ChainID: first.ChainId(), Nonce: first.Nonce(), Gas: first.Gas(), GasFeeCap: big.NewInt(224), GasTipCap: big.NewInt(4), To: first.To(), Data: first.Data(), Value: first.Value()}), first.ChainId())
	require.NoError(t, err)
	return next
}

func TestMinedFailureOutlivesInclusionDeadline(t *testing.T) {
	for _, tc := range []struct {
		spent, limit                 int
		receiptError, receiptMissing bool
	}{{0, 0, false, false}, {1, 1, false, false}, {0, 0, true, false}, {0, 0, false, true}, {1, 1, false, true}, {0, 1, false, false}, {1, 2, false, false}} {
		t.Run(fmt.Sprintf("spent=%d/limit=%d/receipt-error=%t/missing=%t", tc.spent, tc.limit, tc.receiptError, tc.receiptMissing), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 18)
			f.Finalized = false
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: tc.limit}))
			tickRecovery(t, s, c, key)
			s = reopenRecoveryStore(t, s)
			last := f.Transactions()[0]
			if tc.spent > 0 {
				replacement := recoveryReplacement(t, key, last)
				identity, err := signedIdentity(replacement)
				require.NoError(t, err)
				require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, last.Hash().Hex()))
				require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, replacement.Hash()))
				last = replacement
			}
			included := last
			if tc.receiptMissing && tc.spent > 0 {
				included = f.Transactions()[0]
			}
			f.Mine(included, false)
			_, err := s.db.Exec("UPDATE redemption_transactions SET broadcast_at=?", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano))
			require.NoError(t, err)
			_, err = s.db.Exec("UPDATE redemption_attempts SET phase='mined'")
			require.NoError(t, err)
			_, err = s.db.Exec("UPDATE redemption_transactions SET phase='mined' WHERE hash=?", included.Hash().Hex())
			require.NoError(t, err)
			f.Mu.Lock()
			f.ReceiptError = tc.receiptError
			f.ReceiptMissing = tc.receiptMissing
			f.Mu.Unlock()
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Nanosecond, MaxReplacements: tc.limit}))
			failures := ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1))
			require.Equal(t, tc.receiptError, len(failures) > 0)
			require.Eventually(t, func() bool { s.recovery.mu.Lock(); defer s.recovery.mu.Unlock(); return len(s.recovery.tracking) == 0 }, time.Second, time.Millisecond)
			require.Equal(t, "mined", ticketPhase(t, s, ticket.Sig), "canonical inclusion must survive an exhausted inclusion timeout")
			if included.Hash() != last.Hash() {
				var phase string
				require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", last.Hash().Hex()).Scan(&phase))
				require.Equal(t, "submitted", phase, "an included predecessor must not mark the pending replacement mined")
			}
			ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1))
			require.Len(t, f.Transactions(), tc.spent+1, "unfinalized failure must not start another redemption")
		})
	}
}

func TestTerminalTicketKeepsUnresolvedReceipts(t *testing.T) {
	for _, phase := range []string{"expired", "invalid"} {
		for _, included := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/attempt=%d", phase, included), func(t *testing.T) {
				f, s, ticket, c, key := newRecoveryTest(t, 19)
				f.LoseNextResponse = true
				f.Finalized = false
				require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
				tickRecovery(t, s, c, key)
				s = reopenRecoveryStore(t, s)
				f.Mine(f.Transactions()[included], true)
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, phase, f.CollateralSnapshot()))
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Equal(t, phase, ticketPhase(t, s, ticket.Sig), "unfinalized inclusion must not settle a terminal ticket")
				f.Mu.Lock()
				f.Finalized = true
				f.Mu.Unlock()
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
			})
		}
	}
}

func TestRevertedReplacementReleasesSameNonceLiability(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 20)
	tickRecovery(t, s, c, key)
	s = reopenRecoveryStore(t, s)
	first := f.Transactions()[0]
	replacement := recoveryReplacement(t, key, first)
	identity, err := signedIdentity(replacement)
	require.NoError(t, err)
	require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, first.Hash().Hex()))
	require.NoError(t, RetryRedemption(t.Context(), s, c.Contracts, replacement.Hash()))
	f.Mine(replacement, false)
	require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, c))
	require.Equal(t, "reverted", ticketPhase(t, s, ticket.Sig))
	next := nextRecoveryTicket(ticket, 21)
	require.NoError(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), "a finalized replacement consumes every sibling with the same sender and nonce")
}

func TestManualRetryFailureKeepsActiveMonitor(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 101)
	s.metrics = observability.New("orchestrator")
	require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
	tickRecovery(t, s, c, key)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "broadcast"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "submitted"}))
	first := f.Transactions()[0]
	f.Mu.Lock()
	f.RejectNext = true
	f.Mu.Unlock()
	require.Error(t, RetryRedemption(t.Context(), s, c.Contracts, first.Hash()))
	require.Equal(t, "submitted", ticketPhase(t, s, ticket.Sig))
	tickRecovery(t, s, c, key)
	sent := f.Transactions()
	s.recovery.mu.Lock()
	roots := len(s.recovery.tracking)
	s.recovery.mu.Unlock()
	require.Equal(t, 1, roots)
	require.Len(t, sent, 2, "an identical-byte manual retry must not admit a fresh nonce while the original monitor is active")
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "broadcast"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "submitted"}))
}

func TestConsumedNonceResolvesOtherTicketHistory(t *testing.T) {
	f := newRecoveryRPC(t)
	s, a := f.store(t, 103)
	s.metrics = observability.New("orchestrator")
	ca, cb, key := f.Chain(t), f.Chain(t), recoveryKey(t)
	b := nextRecoveryTicket(a, 104)
	b.PayerAddress = common.HexToAddress("0x9999")
	require.NoError(t, s.StoreWinningTicket(b))
	a0, err := ca.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(a))
	require.NoError(t, err)
	require.NoError(t, s.RecordPrepared(t.Context(), a, a0))
	f.Mu.Lock()
	f.RejectNext = true
	f.Mu.Unlock()
	require.Error(t, broadcastRedemption(t.Context(), s, ca.Contracts, a.Sig, a0))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	b0, err := cb.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(b))
	require.NoError(t, err)
	require.Equal(t, a0.Nonce, b0.Nonce)
	require.NoError(t, s.RecordPrepared(t.Context(), b, b0))
	require.NoError(t, broadcastRedemption(t.Context(), s, cb.Contracts, b.Sig, b0))
	a1, err := ca.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(a))
	require.NoError(t, err)
	require.EqualValues(t, 1, a1.Nonce)
	require.NoError(t, s.RecordPrepared(t.Context(), a, a1))
	require.NoError(t, broadcastRedemption(t.Context(), s, ca.Contracts, a.Sig, a1))
	// A durable sibling on the same sender/nonce must become retryable when B
	// finalizes, even though the sibling itself has no finalized receipt.
	sibling := nextRecoveryTicket(a, 108)
	require.NoError(t, s.StoreWinningTicket(sibling))
	require.NoError(t, s.RecordPrepared(t.Context(), sibling, eth.SignedTransaction{Hash: common.HexToHash("0xc"), Raw: []byte{1}, From: b0.From, Nonce: b0.Nonce}))
	sent := f.Transactions()
	f.Mine(sent[1], true)
	f.Mine(sent[2], false)
	require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, ca))
	require.Equal(t, "confirmed", ticketPhase(t, s, b.Sig))
	require.Equal(t, "reverted", ticketPhase(t, s, a.Sig))
	require.Equal(t, "retryable", ticketPhase(t, s, sibling.Sig))
	require.EqualValues(t, 2, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
	require.NoError(t, s.finishTicket(t.Context(), b.Sig, "confirmed", b0.Hash.Hex(), f.CollateralSnapshot()))
	require.EqualValues(t, 2, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "retryable"}))
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
	require.NoError(t, s.setTicketPhase(t.Context(), sibling.Sig, "invalid"))
	next := nextRecoveryTicket(a, 105)
	require.NoError(t, s.StoreWinningTickets(t.Context(), a.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), "B consumed nonce zero and A's nonce one failed; A has no remaining liability")
}

func TestMultipleFinalizedFailuresResolveEveryGroup(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 106)
	var originals []*types.Transaction
	for i := 0; i < 2; i++ {
		prepared, err := c.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
		require.NoError(t, err)
		require.NoError(t, s.RecordPrepared(t.Context(), ticket, prepared))
		require.NoError(t, broadcastRedemption(t.Context(), s, c.Contracts, ticket.Sig, prepared))
		first := f.Transactions()[2*i]
		originals = append(originals, first)
		replacement := recoveryReplacement(t, key, first)
		identity, err := signedIdentity(replacement)
		require.NoError(t, err)
		require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, first.Hash().Hex()))
		require.NoError(t, broadcastRedemption(t.Context(), s, c.Contracts, ticket.Sig, identity))
		if i == 0 {
			_, err = s.db.Exec("UPDATE redemption_attempts SET phase='retryable' WHERE sig=?", ticket.Sig)
			require.NoError(t, err)
		}
	}
	for _, tx := range originals {
		f.Mine(tx, false)
	}
	require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, c))
	history, err := s.Redemptions()
	require.NoError(t, err)
	require.Len(t, history, 4)
	next := nextRecoveryTicket(ticket, 107)
	require.NoError(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), "both finalized failed nonces must resolve their replacement siblings")
}

func TestLiveAcknowledgementRestoresNonce(t *testing.T) {
	for _, mode := range []string{"manual", "manual-external-handle", "finalized"} {
		t.Run(mode, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 111)
			f.LoseNextResponse = true
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
			require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			first := f.Transactions()[0]
			if mode != "finalized" {
				retryStore := s
				if mode == "manual-external-handle" {
					var seq int
					var name, path string
					require.NoError(t, s.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path))
					var err error
					retryStore, err = OpenRedeemerDB(path)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, retryStore.Close()) })
				}
				require.NoError(t, RetryRedemption(t.Context(), retryStore, c.Contracts, first.Hash()))
			} else {
				f.Mine(first, true)
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			}
			floor, err := s.SavedNonceFloor(t.Context(), key.Address())
			require.NoError(t, err)
			require.EqualValues(t, 1, floor)
			next := nextRecoveryTicket(ticket, 112)
			next.PayerAddress = common.HexToAddress("0x9999")
			require.NoError(t, s.StoreWinningTicket(next))
			f.Mu.Lock()
			f.StaleNonce = true
			f.Mu.Unlock()
			tickRecovery(t, s, c, key)
			sent := f.Transactions()
			require.EqualValues(t, 1, sent[len(sent)-1].Nonce(), "new submission must honor a durable acknowledgement received since this worker started")
		})
	}
}

func TestUsedTransitionRecoversAfterStorageFailure(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 222)
	f.LoseNextResponse = true
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	s = reopenRecoveryStore(t, s)
	f.Mu.Lock()
	f.Used[ticket.Hash()] = true
	f.Mu.Unlock()
	_, err := s.db.Exec(`CREATE TRIGGER fail_settlement BEFORE UPDATE OF redeemed_at ON winning_tickets BEGIN SELECT RAISE(ABORT,'injected settlement failure'); END`)
	require.NoError(t, err)
	require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
	require.Equal(t, "retryable", ticketPhase(t, s, ticket.Sig), "failed settlement must roll back the terminal summary")
	_, err = s.db.Exec("DROP TRIGGER fail_settlement")
	require.NoError(t, err)
	s = reopenRecoveryStore(t, s)
	tickRecovery(t, s, c, key)
	var redeemed bool
	require.NoError(t, s.db.QueryRow("SELECT redeemed_at IS NOT NULL FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&redeemed))
	if !redeemed {
		t.Errorf("summary=%s but redeemed_at remains NULL even after restart and retry", ticketPhase(t, s, ticket.Sig))
	}
	next := nextRecoveryTicket(ticket, 223)
	require.NoError(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 0, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), "a finalized-used ticket must not strand collateral")
}

func TestRejectedReplacementKeepsPredecessorReconcilable(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 225)
	require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
	tickRecovery(t, s, c, key)
	original := f.Transactions()[0]
	s = reopenRecoveryStore(t, s)
	replacement := recoveryReplacement(t, key, original)
	identity, err := signedIdentity(replacement)
	require.NoError(t, err)
	require.NoError(t, s.recordTransaction(t.Context(), ticket.Sig, identity, original.Hash().Hex()))
	f.Mu.Lock()
	f.RejectNext = true
	f.Mu.Unlock()
	require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1}))
	require.NotEmpty(t, s.recovery.resume(t.Context()))
	require.Equal(t, "retryable", ticketPhase(t, s, ticket.Sig))
	var phase string
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", original.Hash().Hex()).Scan(&phase))
	require.Equal(t, "submitted", phase)
	f.Mine(original, true)
	require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
}

func TestFinalizedUsePollingRetainsOnlyUnresolvedWork(t *testing.T) {
	for _, tc := range []struct {
		name              string
		unresolved, mined bool
	}{{"resolved revert", false, false}, {"unresolved revert", true, false}, {"mined", false, true}} {
		unresolved := tc.unresolved
		t.Run(tc.name, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 226)
			f.LoseNextResponse = unresolved
			if tc.mined {
				f.MineAfter = 1
				f.Finalized = false
			}
			if unresolved {
				require.NotEmpty(t, ProcessRedemptions(t.Context(), s, c, key, big.NewInt(1)))
			}
			tickRecovery(t, s, c, key)
			if tc.mined {
				require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "mined" }, time.Second, time.Millisecond)
			}
			s = reopenRecoveryStore(t, s)
			if tc.mined {
				f.Mu.Lock()
				f.Finalized = true
				f.ReceiptMissing = true
				f.Mu.Unlock()
			} else {
				sent := f.Transactions()
				f.Mine(sent[len(sent)-1], false)
				require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Equal(t, "reverted", ticketPhase(t, s, ticket.Sig))
			}
			r := &redemptionRecovery{store: s, chain: c}
			f.Mu.Lock()
			before := f.ContractCalls
			f.Mu.Unlock()
			require.Empty(t, r.reconcileUsed(t.Context()))
			require.Empty(t, r.reconcileUsed(t.Context()))
			f.Mu.Lock()
			calls := f.ContractCalls - before
			f.Mu.Unlock()
			require.Equal(t, unresolved || tc.mined, calls > 0, "only unresolved or mined tickets need contract-state polling")
			if unresolved || tc.mined {
				f.Mu.Lock()
				f.Used[ticket.Hash()] = true
				f.ReceiptMissing = true
				f.Mu.Unlock()
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.Empty(t, r.reconcileUsed(t.Context()))
				require.Equal(t, "used", ticketPhase(t, s, ticket.Sig))
				var redeemed bool
				require.NoError(t, s.db.QueryRow("SELECT redeemed_at IS NOT NULL FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&redeemed))
				require.True(t, redeemed)
			}
		})
	}
}

// Configure before sending work so the callback is immutable while jobs run.
func captureRecoveryResults(t *testing.T, s *RedeemerDB) func() *eth.TransactionReceipt {
	t.Helper()
	results := make(chan *eth.TransactionReceipt, 8)
	completed := s.recovery.manager.Completed
	s.recovery.manager.Completed = func(ctx context.Context, result *eth.TransactionReceipt) { completed(ctx, result); results <- result }
	return func() *eth.TransactionReceipt {
		t.Helper()
		select {
		case result := <-results:
			return result
		case <-time.After(3 * time.Second):
			t.Fatal("redemption callback did not complete")
			return nil
		}
	}
}

func TestRedemptionCallbackObservations(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit_failure=%t", failCommit), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 112)
			s.metrics = observability.New("orchestrator")
			f.Finalized = false
			f.MineAfter = 1
			if failCommit {
				_, err := s.db.Exec(`CREATE TRIGGER fail_mined BEFORE UPDATE OF phase ON redemption_attempts WHEN NEW.phase='mined' BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
				require.NoError(t, err)
			}
			require.NoError(t, StartRedemptionRecovery(t.Context(), s, c, key, RedemptionOptions{Timeout: time.Hour, MaxReplacements: 0}))
			nextResult := captureRecoveryResults(t, s)
			tickRecovery(t, s, c, key)
			result := nextResult()
			require.NoError(t, result.Error())
			if failCommit {
				require.NotEqual(t, "mined", ticketPhase(t, s, ticket.Sig))
				require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "mined"}))
				_, err := s.db.Exec(`DROP TRIGGER fail_mined`)
				require.NoError(t, err)
				require.NoError(t, s.recovery.recordResult(t.Context(), result))
			}
			require.Equal(t, "mined", ticketPhase(t, s, ticket.Sig))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "mined"}))
			require.NoError(t, s.recovery.recordResult(t.Context(), result))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_transitions_total", map[string]string{"event": "mined"}))
			require.Zero(t, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
			f.Mu.Lock()
			f.Finalized = true
			f.Mu.Unlock()
			require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "redemption_settlements_total", nil))
		})
	}
}

func TestCollateralWatermarkSurvivesSettlementAndRestart(t *testing.T) {
	f, store, ticket, _, _ := newRecoveryTest(t, 243)
	captured := f.CollateralSnapshot()
	settled := eth.ChainSnapshot{Number: new(big.Int).Add(captured.Number, big.NewInt(1)), BlockHash: common.HexToHash("0x101")}
	next := nextRecoveryTicket(ticket, 244)
	admit := func(snapshot eth.ChainSnapshot) error {
		return store.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, 0, new(big.Int).Set(ticket.FaceValue), []*pm.SignedTicket{next}, snapshot)
	}
	require.ErrorContains(t, store.finishTicket(t.Context(), ticket.Sig, "used", ""), "block evidence required")
	_, err := store.db.Exec(`CREATE TRIGGER fail_settlement BEFORE UPDATE OF redeemed_at ON winning_tickets BEGIN SELECT RAISE(ABORT,'storage failure'); END`)
	require.NoError(t, err)
	require.Error(t, store.finishTicket(t.Context(), ticket.Sig, "used", "", settled))
	var floors, redeemed int
	require.NoError(t, store.db.QueryRow(`SELECT count(*) FROM collateral_watermark`).Scan(&floors))
	require.Zero(t, floors, "failed settlement cannot advance its watermark")
	require.NoError(t, store.db.QueryRow(`SELECT count(*) FROM winning_tickets WHERE redeemed_at IS NOT NULL`).Scan(&redeemed))
	require.Zero(t, redeemed)
	require.ErrorIs(t, admit(captured), pm.ErrPayerUnavailable, "failed settlement retains liability")
	_, err = store.db.Exec(`DROP TRIGGER fail_settlement`)
	require.NoError(t, err)
	// The funds observation was captured while the old winner still reserved its
	// face value. Settlement can release it before this admission transaction starts.
	require.NoError(t, store.finishTicket(t.Context(), ticket.Sig, "used", "", settled))
	require.ErrorIs(t, admit(captured), pm.ErrPayerUnavailable)
	store = reopenRecoveryStore(t, store)
	require.ErrorIs(t, admit(captured), pm.ErrPayerUnavailable)
	wrongFork := settled
	wrongFork.BlockHash = common.HexToHash("0xbad")
	require.ErrorIs(t, admit(wrongFork), pm.ErrPayerUnavailable)
	require.ErrorContains(t, store.finishTicket(t.Context(), ticket.Sig, "used", "", wrongFork), "conflicting finalized")
	require.NoError(t, admit(settled), "post-settlement collateral can cover the next winner")
	require.Error(t, admit(settled), "durable settlement must retain signature replay protection")
}
