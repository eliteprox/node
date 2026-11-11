package redemption

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

func TestDroppedRedemptionRefillsNonceAfterRestart(t *testing.T) {
	for _, phase := range []string{"retryable", "expired", "invalid", "used"} {
		t.Run(phase, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 230)
			recoveryWorker(t, s, c, key, RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: 0})
			tickRecovery(t, s, c, key)
			original := f.Transactions()[0]
			f.Mu.Lock()
			delete(f.Pool, 0)
			f.Mu.Unlock()
			require.Eventually(t, func() bool { return ticketPhase(t, s, ticket.Sig) == "retryable" }, time.Second, time.Millisecond)
			s = reopenRecoveryStore(t, s)
			if phase != "retryable" {
				require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, phase, f.CollateralSnapshot()))
			}
			// A failed claim must prevent both the saved-byte replay and any fresh
			// higher nonce. Restart retains the original timestamp and identity.
			_, err := s.db.Exec(`CREATE TRIGGER reject_gap_send BEFORE UPDATE OF phase ON redemption_transactions BEGIN SELECT RAISE(ABORT,'injected persistence failure'); END`)
			require.NoError(t, err)
			require.NotEmpty(t, processRecovery(t, s, c, key))
			require.Len(t, f.Transactions(), 1)
			_, err = s.db.Exec("DROP TRIGGER reject_gap_send")
			require.NoError(t, err)
			tickRecovery(t, s, c, key)
			sent := f.Transactions()
			require.Len(t, sent, 2)
			require.Equal(t, original.Hash(), sent[1].Hash(), "refill the evicted slot with the exact saved bytes")
			if phase != "retryable" {
				require.Equal(t, phase, ticketPhase(t, s, ticket.Sig), "gap repair must not reopen terminal tickets")
			}
			f.Mine(sent[1], true)
			tickRecovery(t, s, c, key)
			next := nextRecoveryTicket(ticket, 231)
			require.NoError(t, s.StoreWinningTicket(next))
			f.Mu.Lock()
			f.Tickets[next.Hash()] = next.RecipientRandHash
			f.Mu.Unlock()
			tickRecovery(t, s, c, key)
			sent = f.Transactions()
			require.Len(t, sent, 3)
			require.EqualValues(t, 1, sent[2].Nonce())
			f.Mine(sent[2], true)
			tickRecovery(t, s, c, key)
			require.Equal(t, "confirmed", ticketPhase(t, s, next.Sig), "queued work must actually settle after the gap is repaired")
		})
	}
}

func TestClosedTicketRepairsSupersededHigherNonce(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 232)
	// The original send lost its response; a fresh attempt uses nonce one, as
	// required by the retry policy. Either attempt can later win.
	f.LoseNextResponse = true
	require.NotEmpty(t, processRecovery(t, s, c, key))
	tickRecovery(t, s, c, key)
	s = reopenRecoveryStore(t, s)
	sent := f.Transactions()
	require.Len(t, sent, 2)
	require.EqualValues(t, 0, sent[0].Nonce())
	require.EqualValues(t, 1, sent[1].Nonce())
	f.Mine(sent[0], true)
	require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
	f.Mu.Lock()
	delete(f.Pool, 1)
	f.FinalizedNonce = 1
	f.Mu.Unlock()
	remote := Binding{ChainID: "1", Controller: c.Contracts.Controller, Role: "orchestrator", Destination: "http://redeemer.example"}
	require.ErrorContains(t, s.Bind(t.Context(), remote, DefaultLimits, c), "drain local signed", "settlement did not consume the superseded higher nonce")
	// Crash after replay submission but before recording its acknowledgment.
	_, err := s.db.Exec(`CREATE TRIGGER reject_gap_ack BEFORE UPDATE OF acknowledged ON redemption_transactions BEGIN SELECT RAISE(ABORT,'injected acknowledgment failure'); END`)
	require.NoError(t, err)
	require.NotEmpty(t, processRecovery(t, s, c, key))
	s = reopenRecoveryStore(t, s)
	var interrupted string
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", sent[1].Hash().Hex()).Scan(&interrupted))
	require.Equal(t, "superseded", interrupted, "the recorded historical outcome must survive interrupted replay")
	_, err = s.db.Exec("DROP TRIGGER reject_gap_ack")
	require.NoError(t, err)
	f.Mu.Lock()
	delete(f.Pool, 1)
	f.Mu.Unlock()
	tickRecovery(t, s, c, key) // No eligible tickets remain to trigger allocation.
	sent = f.Transactions()
	require.Len(t, sent, 4)
	require.Equal(t, sent[1].Hash(), sent[3].Hash())
	require.Equal(t, sent[1].Hash(), sent[2].Hash())
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
	var current, phase string
	require.NoError(t, s.db.QueryRow("SELECT tx_hash FROM winning_tickets WHERE sig=?", ticket.Sig).Scan(&current))
	require.Equal(t, sent[0].Hash().Hex(), current)
	require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions WHERE hash=?", sent[1].Hash().Hex()).Scan(&phase))
	require.Equal(t, "superseded", phase)
	f.Mine(sent[2], false) // Already-used redemption still fills its nonce slot.
	f.Mu.Lock()
	f.FinalizedNonce = 2
	f.Mu.Unlock()
	next := nextRecoveryTicket(ticket, 233)
	require.NoError(t, s.StoreWinningTicket(next))
	f.Mu.Lock()
	f.Tickets[next.Hash()] = next.RecipientRandHash
	f.Mu.Unlock()
	tickRecovery(t, s, c, key)
	sent = f.Transactions()
	require.EqualValues(t, 2, sent[len(sent)-1].Nonce())
	f.Mine(sent[len(sent)-1], true)
	tickRecovery(t, s, c, key)
	require.Equal(t, "confirmed", ticketPhase(t, s, next.Sig))
	require.NoError(t, s.Bind(t.Context(), remote, DefaultLimits, c), "finalized account state proves the remaining nonce drained")
}

func TestDestinationChangeWaitsForNonceDrain(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(fmt.Sprintf("bound=%t", bound), func(t *testing.T) {
			f := newRecoveryRPC(t)
			s, ticket := f.store(t, 235)
			chain, key := f.Chain(t), recoveryKey(t)
			binding := Binding{ChainID: "1", Controller: chain.Contracts.Controller, Role: "orchestrator", Destination: "chain"}
			if bound {
				require.NoError(t, s.Bind(t.Context(), binding, DefaultLimits, chain))
			}
			prepared, err := chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
			require.NoError(t, err)
			require.NoError(t, s.RecordPrepared(t.Context(), ticket, prepared))

			f.Mu.Lock()
			f.Round = 4
			f.Mu.Unlock()
			finalized, err := chain.RedemptionSnapshot(t.Context(), "finalized")
			require.NoError(t, err)
			require.True(t, finalized.TicketExpired(ticket.CreationRound))
			require.NoError(t, s.settle(t.Context(), ticket, "expired", finalized))
			binding.Destination = "http://redeemer.example"
			require.ErrorContains(t, s.Bind(t.Context(), binding, DefaultLimits, nil), "drain local signed", "missing chain evidence cannot prove drainage")
			require.ErrorContains(t, s.Bind(t.Context(), binding, DefaultLimits, chain), "drain local signed")
			if !bound {
				bindRecoveryStore(t, s, chain)
			}
			transaction := new(types.Transaction)
			require.NoError(t, transaction.UnmarshalBinary(prepared.Raw))
			f.Mine(transaction, false)
			require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, chain), "the finalized reverted transaction consumed its nonce")
			require.NoError(t, s.Bind(t.Context(), binding, DefaultLimits, nil), "a stored finalized receipt proves drainage without another RPC")
		})
	}
}

func TestFinalizedExternalNonceRetiresOnlyMissingTransactions(t *testing.T) {
	for _, observation := range []string{"missing", "unfinalized", "receipt-error", "nonce-error", "nonce-null"} {
		t.Run(observation, func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 234)
			tickRecovery(t, s, c, key)
			s = reopenRecoveryStore(t, s)
			first := f.Transactions()[0]
			require.NoError(t, s.setTicketPhase(t.Context(), ticket.Sig, "expired"))
			// Eviction alone retains liability; a canonical finalized account
			// observation must prove another transaction consumed this nonce.
			f.Mu.Lock()
			delete(f.Pool, 0)
			f.Mu.Unlock()
			require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
			next := nextRecoveryTicket(ticket, 235)
			next.CreationRound = 5
			require.ErrorIs(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), pm.ErrPayerUnavailable)
			f.Mu.Lock()
			f.FinalizedNonce = 1
			f.FinalizedNonceReads = 0
			f.ReceiptError = observation == "receipt-error"
			f.FinalizedNonceError = observation == "nonce-error"
			f.FinalizedNonceNull = observation == "nonce-null"
			if observation == "unfinalized" {
				f.Finalized = false
				f.MineLocked(first, false)
			}
			f.Mu.Unlock()
			failures := ReconcileSubmitted(t.Context(), s, c)
			require.Equal(t, observation == "receipt-error" || observation == "nonce-error" || observation == "nonce-null", len(failures) > 0)
			f.Mu.Lock()
			reads := f.FinalizedNonceReads
			f.Mu.Unlock()
			require.LessOrEqual(t, reads, 1, "read finalized account state once per wallet")
			items, err := s.SubmittedRedemptions()
			require.NoError(t, err)
			if observation == "missing" {
				require.Empty(t, items)
				require.Equal(t, "expired", ticketPhase(t, s, ticket.Sig))
				require.ErrorIs(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), pm.ErrPayerUnavailable, "consuming a nonce does not finalize heuristic ticket expiry")
				f.Mu.Lock()
				f.Round = 5
				f.Mu.Unlock()
				tickRecovery(t, s, c, key)
				require.NoError(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()))
				oldEpoch := nextRecoveryTicket(ticket, 236)
				oldEpoch.RecipientRandHash = ticket.RecipientRandHash
				require.ErrorIs(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, ticket.RecipientRandHash, big.NewInt(10000), []*pm.SignedTicket{oldEpoch}, f.CollateralSnapshot()), ErrExposedRandomness)
			} else {
				require.Len(t, items, 1, "uncertain or unfinalized receipts retain liability")
			}

		})
	}
}

func TestFinalizedExternalNoncePreservesHigherAttempt(t *testing.T) {
	f, s, ticket, c, key := newRecoveryTest(t, 237)
	f.LoseNextResponse = true
	require.NotEmpty(t, processRecovery(t, s, c, key))
	tickRecovery(t, s, c, key)
	s = reopenRecoveryStore(t, s)
	f.Mu.Lock()
	delete(f.Pool, 0)
	f.FinalizedNonce = 1
	f.FinalizedNonceReads = 0
	f.Mu.Unlock()
	require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
	items, err := s.SubmittedRedemptions()
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, f.Transactions()[1].Hash(), items[0].Hash)
	require.Equal(t, "submitted", ticketPhase(t, s, ticket.Sig))
	f.Mu.Lock()
	reads := f.FinalizedNonceReads
	f.Mu.Unlock()
	require.Equal(t, 1, reads)
	next := nextRecoveryTicket(ticket, 238)
	next.CreationRound = 5
	require.ErrorIs(t, s.StoreWinningTickets(t.Context(), ticket.PayerAddress, next.RecipientRandHash, big.NewInt(1000), []*pm.SignedTicket{next}, f.CollateralSnapshot()), pm.ErrPayerUnavailable)
	// Finalized state advances beyond both attempts while the pending endpoint
	// stays stale. New signing must use that observed count, not the saved floor.
	f.Mu.Lock()
	delete(f.Pool, 1)
	f.FinalizedNonce = 3
	f.StaleNonce = true
	f.Mu.Unlock()
	tickRecovery(t, s, c, key)
	sent := f.Transactions()
	require.Len(t, sent, 3)
	require.EqualValues(t, 3, sent[2].Nonce())
	f.Mine(sent[2], true)
	tickRecovery(t, s, c, key)
	require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
}

func TestFinalizedNonceAdvancesPastResolvedHistory(t *testing.T) {
	for _, tc := range []struct {
		acknowledged       bool
		pending, finalized uint64
	}{{false, 0, 1}, {true, 0, 3}, {true, 1, 3}} {
		t.Run(fmt.Sprintf("acknowledged=%t/pending=%d", tc.acknowledged, tc.pending), func(t *testing.T) {
			f, s, ticket, c, key := newRecoveryTest(t, 239)
			f.LoseNextResponse = !tc.acknowledged
			require.Equal(t, !tc.acknowledged, len(processRecovery(t, s, c, key)) > 0)
			s = reopenRecoveryStore(t, s)
			if tc.acknowledged {
				f.Mine(f.Transactions()[0], true)
				require.Empty(t, ReconcileSubmitted(t.Context(), s, c))
				require.NoError(t, s.StoreWinningTicket(nextRecoveryTicket(ticket, 240)))
			}
			f.Mu.Lock()
			delete(f.Pool, 0)
			f.FinalizedNonce = tc.finalized
			f.StaleNonce = true
			f.StaleNonceValue = tc.pending
			f.Mu.Unlock()
			tickRecovery(t, s, c, key)
			sent := f.Transactions()
			require.Len(t, sent, 2)
			require.Equal(t, tc.finalized, sent[1].Nonce(), "local history must not hide newer finalized account state")
		})
	}
}

func nextRecoveryTicket(ticket *pm.SignedTicket, id byte) *pm.SignedTicket {
	next, body := *ticket, *ticket.Ticket
	next.Ticket = &body
	next.Sig = []byte{id, 2, 3}
	next.RecipientRandHash = crypto.Keccak256Hash([]byte{id})
	return &next
}
