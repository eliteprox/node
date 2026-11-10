package redemption

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

type destinationFunc func(context.Context, *pm.SignedTicket) error

func (f destinationFunc) Submit(ctx context.Context, t *pm.SignedTicket) error { return f(ctx, t) }

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

func TestLegacySignedWorkMustDrainBeforeRemoteAdoption(t *testing.T) {
	_, s, ticket, chain, key := newRecoveryTest(t, 1)
	signed, err := chain.PrepareRedemption(t.Context(), key, big.NewInt(1), redeemTicket(ticket))
	require.NoError(t, err)
	require.NoError(t, s.RecordPrepared(t.Context(), ticket, signed))
	b := Binding{ChainID: "1", Controller: chain.Contracts.Controller, Role: "orchestrator", Destination: "http://redeemer.example"}
	require.ErrorContains(t, s.Bind(t.Context(), b, DefaultLimits), "drain local signed")
	b.Destination = "chain"
	require.NoError(t, s.Bind(t.Context(), b, DefaultLimits))
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
	require.ErrorIs(t, s.StoreWinningTickets(t.Context(), first.PayerAddress, third.RecipientRandHash, 100, new(big.Int).Mul(first.FaceValue, big.NewInt(2)), []*pm.SignedTicket{third}, f.CollateralSnapshot()), pm.ErrPayerUnavailable, "latest round cutoffs must not release unsettled remote liability")
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
	require.ErrorIs(t, s.StoreWinningTickets(t.Context(), first.PayerAddress, third.RecipientRandHash, 0, big.NewInt(1e15), []*pm.SignedTicket{third}, snapshot), pm.ErrPayerUnavailable, "settlement and its collateral floor commit together")
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
	f.Mu.Lock()
	f.Registered = false
	f.Mu.Unlock()
	_, err = NewChainDestination(t.Context(), s, chain, key, big.NewInt(1), RedemptionOptions{Timeout: time.Hour})
	require.NoError(t, err)
	previous := f.Transactions()[0]
	next := recoveryReplacement(t, key, previous)
	require.ErrorIs(t, s.recovery.prepareReplacement(t.Context(), previous, next), eth.ErrUneconomicalRedemption)
	var identities int
	require.NoError(t, s.db.QueryRow("SELECT count(*) FROM redemption_transactions").Scan(&identities))
	require.Equal(t, 1, identities, "withholding a replacement preserves the predecessor")
}
