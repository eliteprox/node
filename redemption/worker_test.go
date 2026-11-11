package redemption

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

type destinationFunc func(context.Context, *pm.SignedTicket) error

func (f destinationFunc) Submit(ctx context.Context, t *pm.SignedTicket) error { return f(ctx, t) }

func TestRedemptionRequiresBoundDeployment(t *testing.T) {
	for _, mismatch := range []string{"unbound", "chain", "controller", "destination"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newRecoveryRPC(t)
			s, ticket := f.store(t, 1)
			chain, key := f.Chain(t), recoveryKey(t)
			binding := Binding{ChainID: "1", Controller: chain.Contracts.Controller, Role: "orchestrator", Destination: "chain"}
			switch mismatch {
			case "chain":
				binding.ChainID = "2"
			case "controller":
				binding.Controller = common.HexToAddress("0x2000")
			case "destination":
				binding.Destination = "http://redeemer.example"
			}
			if mismatch != "unbound" {
				require.NoError(t, s.Bind(t.Context(), binding, DefaultLimits, chain))
			}
			_, err := NewChainDestination(t.Context(), s, chain, key, big.NewInt(1), RedemptionOptions{Timeout: time.Hour})
			require.Error(t, err)
			require.Nil(t, s.recovery, "invalid configuration must not start recovery")
			if mismatch == "unbound" {
				_, err = NewWorker(s, chain, destinationFunc(func(context.Context, *pm.SignedTicket) error { t.Fatal("unbound delivery"); return nil }))
				require.Error(t, err)
				require.NotEmpty(t, ReconcileSubmitted(t.Context(), s, chain))
				require.Error(t, RetryRedemption(t.Context(), s, chain.Contracts, ticket.Hash()))
			}
			require.Empty(t, f.Transactions())
		})
	}
}

func TestCanceledRedemptionPassRotatesUnvisitedTickets(t *testing.T) {
	f := newRecoveryRPC(t)
	s, _ := boundStore(t, "orchestrator", "http://redeemer.example", DefaultLimits)
	first, key := winningTicket(t, 1)
	second, _ := winningTicket(t, 2)
	second.PayerAddress = first.PayerAddress
	signTicket(t, second, key)
	require.NoError(t, s.Admit(t.Context(), first))
	require.NoError(t, s.Admit(t.Context(), second))
	cancellations := make(chan context.CancelFunc, 2)
	handler := f.Server.Config.Handler
	f.Server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if bytes.Contains(body, []byte(first.Hash().Hex()[2:])) {
			(<-cancellations)()
			<-r.Context().Done()
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		handler.ServeHTTP(w, r)
	})
	var seen []common.Hash
	worker, err := NewWorker(s, f.Chain(t), destinationFunc(func(_ context.Context, ticket *pm.SignedTicket) error {
		seen = append(seen, ticket.Hash())
		return nil
	}))
	require.NoError(t, err)
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		cancellations <- cancel
		require.NotEmpty(t, worker.Process(ctx))
		cancel()
	}
	require.Equal(t, []common.Hash{second.Hash()}, seen, "canceled RPC work must rotate behind the unvisited tail")
}

func TestDeliveryOutcomeSurvivesPassCancellation(t *testing.T) {
	for _, phase := range []string{"acknowledged", "rejected", "retryable"} {
		t.Run(phase, func(t *testing.T) {
			f := newRecoveryRPC(t)
			s, _ := boundStore(t, "orchestrator", "http://redeemer.example", DefaultLimits)
			ticket, _ := winningTicket(t, 1)
			require.NoError(t, s.Admit(t.Context(), ticket))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			worker, err := NewWorker(s, f.Chain(t), destinationFunc(func(context.Context, *pm.SignedTicket) error {
				cancel()
				if phase == "rejected" {
					return &Rejection{409}
				}
				if phase == "retryable" {
					return context.Canceled
				}
				return nil
			}))
			require.NoError(t, err)
			worker.Process(ctx)
			var actual string
			var failures, next int64
			require.NoError(t, s.db.QueryRow("SELECT delivery,failures,next_attempt FROM ticket_state").Scan(&actual, &failures, &next))
			require.Equal(t, phase, actual)
			if phase == "retryable" {
				require.EqualValues(t, 1, failures)
				require.Greater(t, next, time.Now().UnixMilli())
			}
		})
	}
}

func TestQueuePageRotatesAcrossRecipients(t *testing.T) {
	s, _ := boundStore(t, "redeemer", "chain", DefaultLimits)
	ticket, key := winningTicket(t, 1)
	for nonce := uint32(1); nonce <= 129; nonce++ {
		copy := *ticket
		body := *ticket.Ticket
		copy.Ticket = &body
		copy.TicketNonce = nonce
		signTicket(t, &copy, key)
		require.NoError(t, s.Admit(t.Context(), &copy))
	}
	other, _ := winningTicket(t, 130)
	other.Recipient[0] = 1
	// Preserve the payer's key while changing the recipient.
	other.PayerAddress = ticket.PayerAddress
	signTicket(t, other, key)
	require.NoError(t, s.Admit(t.Context(), other))
	page, err := s.page(t.Context(), 128)
	require.NoError(t, err)
	found := false
	for _, candidate := range page {
		found = found || candidate.ticket.Hash() == other.Hash()
	}
	require.True(t, found, "a recipient arriving behind a large backlog must appear in the first bounded page")
}

func TestBoundQueueFairnessLiabilityAndFinalizedExpiry(t *testing.T) {
	f := newRecoveryRPC(t)
	chain := f.Chain(t)
	s, _ := boundStore(t, "orchestrator", "http://redeemer.example", DefaultLimits)
	first, key := winningTicket(t, 1)
	first.ParamsExpirationBlock = big.NewInt(120)
	require.NoError(t, s.Admit(t.Context(), first))
	second, _ := winningTicket(t, 2)
	second.PayerAddress = first.PayerAddress
	signTicket(t, second, key)
	require.NoError(t, s.Admit(t.Context(), second))
	submitted := 0
	worker, err := NewWorker(s, chain, destinationFunc(func(_ context.Context, ticket *pm.SignedTicket) error {
		submitted++
		require.Equal(t, second.Hash(), ticket.Hash())
		return nil
	}))
	require.NoError(t, err)
	require.Empty(t, worker.Process(t.Context()))
	require.Equal(t, 1, submitted, "a future-expiry head cannot block an eligible ticket from the same payer/recipient")
	third, _ := winningTicket(t, 3)
	third.PayerAddress = first.PayerAddress
	signTicket(t, third, key)
	ready := func() { _, e := s.db.Exec("UPDATE ticket_state SET next_attempt=0"); require.NoError(t, e) }
	f.Mu.Lock()
	f.Round = 4
	f.Finalized = false
	f.Mu.Unlock()
	ready()
	require.Empty(t, worker.Process(t.Context()))
	var settled int
	require.NoError(t, s.db.QueryRow("SELECT sum(settled) FROM ticket_state").Scan(&settled))
	require.Zero(t, settled, "latest expiry is not finalized evidence")
	require.ErrorIs(t, s.StoreWinningTickets(t.Context(), first.PayerAddress, third.RecipientRandHash, new(big.Int).Mul(first.FaceValue, big.NewInt(2)), []*pm.SignedTicket{third}, f.CollateralSnapshot()), pm.ErrPayerUnavailable, "latest expiry must not release unsettled remote liability")
	f.Mu.Lock()
	f.Validity = 5
	f.Mu.Unlock()
	ready()
	require.Empty(t, worker.Process(t.Context()))
	require.Equal(t, 1, submitted, "an acknowledged handoff is not resent")
	f.Mu.Lock()
	f.Validity = 3
	f.Finalized = true
	f.Mu.Unlock()
	ready()
	require.Empty(t, worker.Process(t.Context()))
	require.NoError(t, s.db.QueryRow("SELECT sum(settled) FROM ticket_state").Scan(&settled))
	require.Equal(t, 2, settled, "equality with creation round plus validity is expired")
	snapshot := f.CollateralSnapshot()
	snapshot.Number.Sub(snapshot.Number, big.NewInt(1))
	require.ErrorIs(t, s.StoreWinningTickets(t.Context(), first.PayerAddress, third.RecipientRandHash, big.NewInt(1e15), []*pm.SignedTicket{third}, snapshot), pm.ErrPayerUnavailable, "settlement and its collateral floor commit together")
	var exposed int
	require.NoError(t, s.db.QueryRow("SELECT exposed FROM ticket_state WHERE hash=?", second.Hash().Bytes()).Scan(&exposed))
	require.Equal(t, 1, exposed, "finalized expiry never reopens exposed randomness")
}

func TestStandaloneCostAndWalletPoliciesCoverSavedBytes(t *testing.T) {
	f := newRecoveryRPC(t)
	chain := f.Chain(t)
	key := recoveryKey(t)
	f.Mu.Lock()
	f.Wallet = key.Address()
	f.Mu.Unlock()
	chain.Contracts.CheckRedemptionCost = true
	chain.Contracts.BeforeBroadcast = StandalonePolicy(chain, big.NewInt(1))
	ticket, payer := winningTicket(t, 1)
	// The fixture estimates 21000 gas, base fee 100 and tip 3: fee cap 203.
	ticket.FaceValue = big.NewInt(21000 * 203)
	signTicket(t, ticket, payer)
	_, err := chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
	require.ErrorIs(t, err, eth.ErrUneconomicalRedemption)
	ticket.FaceValue.Add(ticket.FaceValue, big.NewInt(1))
	signTicket(t, ticket, payer)
	prepared, err := chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
	require.NoError(t, err)
	f.Mu.Lock()
	f.Registered = true
	f.Mu.Unlock()
	require.ErrorIs(t, chain.Contracts.Broadcast(t.Context(), prepared), eth.ErrRedeemerIsOrchestrator)
	f.Mu.Lock()
	require.Equal(t, key.Address(), f.RegisteredAccount)
	f.Registered = false
	f.WalletActive = true
	f.Mu.Unlock()
	require.ErrorIs(t, chain.Contracts.Broadcast(t.Context(), prepared), eth.ErrRedeemerIsOrchestrator)
	f.Mu.Lock()
	f.WalletActive = false
	f.Mu.Unlock()
	require.NoError(t, chain.Contracts.Broadcast(t.Context(), prepared))
	require.Len(t, f.Transactions(), 1)
	s, _ := boundStore(t, "redeemer", "chain", DefaultLimits)
	require.NoError(t, s.Admit(t.Context(), ticket))
	require.NoError(t, s.RecordPrepared(t.Context(), ticket, prepared))
	f.Mu.Lock()
	f.Registered = true
	f.Mu.Unlock()
	require.ErrorIs(t, RetryRedemption(t.Context(), s, chain.Contracts, prepared.Hash), eth.ErrRedeemerIsOrchestrator)
	require.Len(t, f.Transactions(), 1, "manual rebroadcast must not bypass the wallet check")
}

func TestWithheldReplacementPreservesPendingTicket(t *testing.T) {
	for _, policy := range []string{"cost", "wallet"} {
		t.Run(policy, func(t *testing.T) {
			f := newRecoveryRPC(t)
			chain := f.Chain(t)
			chain.Contracts.CheckRedemptionCost = true
			s, binding := boundStore(t, "redeemer", "chain", DefaultLimits)
			ticket, payer := winningTicket(t, 1)
			var paused atomic.Bool
			refusal := eth.ErrUneconomicalRedemption
			if policy == "cost" {
				ticket.FaceValue = big.NewInt(21000*203 + 1)
				signTicket(t, ticket, payer)
			} else {
				refusal = errors.New("wallet policy unavailable")
				chain.Contracts.BeforeBroadcast = func(context.Context, *types.Transaction) error {
					if paused.Load() {
						return refusal
					}
					return nil
				}
				f.OnSend = func(*types.Transaction) { paused.Store(true) }
			}
			require.NoError(t, s.Admit(t.Context(), ticket))
			key := recoveryKey(t)
			dest, err := NewChainDestination(t.Context(), s, chain, key, big.NewInt(1), RedemptionOptions{Timeout: 30 * time.Millisecond, MaxReplacements: 1})
			require.NoError(t, err)
			nextResult := captureRecoveryResults(t, s)
			worker, err := NewWorker(s, chain, dest)
			require.NoError(t, err)
			require.Empty(t, worker.Process(t.Context()))
			require.ErrorIs(t, nextResult().Error(), refusal)
			for range 4 {
				require.Empty(t, worker.Process(t.Context()))
				require.Equal(t, "submitted", ticketPhase(t, s, ticket.Sig))
				var phase string
				require.NoError(t, s.db.QueryRow("SELECT phase FROM redemption_transactions").Scan(&phase))
				require.Equal(t, "submitted", phase)
			}
			require.Len(t, f.Transactions(), 1)
			var identities int
			require.NoError(t, s.db.QueryRow("SELECT count(*) FROM redemption_transactions").Scan(&identities))
			require.Equal(t, 1, identities, "a withheld replacement cannot reopen the ticket at another nonce")

			path := s.path
			require.NoError(t, s.Close())
			s, err = OpenRedeemerDB(path)
			require.NoError(t, err)
			defer s.Close()
			require.NoError(t, s.Bind(t.Context(), binding, DefaultLimits, nil))
			dest, err = NewChainDestination(t.Context(), s, chain, key, big.NewInt(1), RedemptionOptions{Timeout: time.Hour, MaxReplacements: 1})
			require.NoError(t, err)
			worker, err = NewWorker(s, chain, dest)
			require.NoError(t, err)
			require.Empty(t, worker.Process(t.Context()))
			require.Len(t, f.Transactions(), 1, "restart re-tracks the pending identity")
			f.Mine(f.Transactions()[0], true)
			require.Empty(t, worker.Process(t.Context()))
			require.Empty(t, worker.Process(t.Context()))
			require.Equal(t, "confirmed", ticketPhase(t, s, ticket.Sig))
		})
	}
}

func TestRedemptionEligibility(t *testing.T) {
	for _, tc := range []struct {
		block, round        int64
		active, lookupError bool
	}{{9, 5, true, false}, {10, 5, true, false}, {11, 5, true, false}, {10, 5, false, false}, {10, 5, true, true}, {10, 8, true, false}} {
		t.Run(fmt.Sprintf("block=%d/round=%d/active=%t/error=%t", tc.block, tc.round, tc.active, tc.lookupError), func(t *testing.T) {
			f, store, ticket, chain, key := newRecoveryTest(t, 1)
			f.Block, f.Round, f.Inactive, f.ActiveError = tc.block, tc.round, !tc.active, tc.lookupError
			f.Validity = 7
			f.OnSend = func(tx *types.Transaction) {
				items, err := store.Redemptions()
				require.NoError(t, err)
				require.Equal(t, []RedemptionStatus{{Hash: tx.Hash().Hex(), Phase: "broadcast"}}, items, "persist the signed identity before sending")
			}
			failures := processRecovery(t, store, chain, key)
			require.Equal(t, tc.lookupError, len(failures) != 0, "%v", failures)
			expired := tc.round >= 8
			redeemed := tc.block >= 10 && !expired && tc.active && !tc.lookupError
			require.Equal(t, redeemed, len(f.Transactions()) == 1)
			if tc.block >= 10 && !expired {
				f.Mu.Lock()
				recipient := f.ActiveRecipient
				f.Mu.Unlock()
				require.Equal(t, ticket.Recipient, recipient, "activity lookup must use the ticket recipient")
			}
			queued, err := store.WinningTicketCount(ticket.PayerAddress)
			require.NoError(t, err)
			require.Equal(t, !expired, queued == 1)
		})
	}
}

func TestTerminalRedemptionSubmissionError(t *testing.T) {
	f, store, ticket, chain, key := newRecoveryTest(t, 2)
	f.PrepareError = "private endpoint and calldata: " + eth.ErrTicketCreationRound.Error()
	failures := processRecovery(t, store, chain, key)
	require.Len(t, failures, 1)
	require.ErrorIs(t, failures[0], eth.ErrTicketCreationRound)
	require.NotContains(t, failures[0].Error(), "private")
	require.Equal(t, "invalid", ticketPhase(t, store, ticket.Sig))
	queued, err := store.WinningTicketCount(ticket.PayerAddress)
	require.NoError(t, err)
	require.Equal(t, 1, queued, "a preparation error is not finalized settlement")
	require.Empty(t, f.Transactions())
	store = reopenRecoveryStore(t, store)
	f.Mu.Lock()
	f.PrepareError = ""
	f.Mu.Unlock()
	tickRecovery(t, store, chain, key)
	require.Equal(t, "invalid", ticketPhase(t, store, ticket.Sig))
	require.Empty(t, f.Transactions(), "a terminal preparation failure must not be submitted after restart")
}
