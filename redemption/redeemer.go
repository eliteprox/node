package redemption

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/pm"
)

func redeemTicket(ticket *pm.SignedTicket) eth.RedeemTicket {
	return eth.RedeemTicket{Recipient: ticket.Recipient, Sender: ticket.PayerAddress, FaceValue: ticket.FaceValue, WinProb: ticket.WinProb, SenderNonce: ticket.TicketNonce, RecipientRandHash: ticket.RecipientRandHash, AuxData: ticket.AuxData(), Signature: ticket.Sig, RecipientRand: ticket.RecipientRand}
}
func (s *RedeemerDB) setTicketPhase(ctx context.Context, sig []byte, phase string, observation ...eth.ChainSnapshot) error {
	if terminalRedemption(phase) {
		return s.finishTicket(ctx, sig, phase, "", observation...)
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO redemption_attempts(sig,attempted_at,phase) VALUES(?,?,?) ON CONFLICT(sig) DO UPDATE SET phase=excluded.phase WHERE redemption_attempts.phase IN ('retryable','awaiting_finality') AND redemption_attempts.phase!=excluded.phase`, sig, time.Now().UTC().Format(time.RFC3339Nano), phase)
	if err == nil {
		if n, _ := result.RowsAffected(); n > 0 {
			s.observeTransition(phase, "", nil)
		}
	}
	return err
}

type ReceiptReader interface {
	FinalizedReceipt(context.Context, common.Hash) (*types.Receipt, error)
}

// ReconcileSubmitted checks every retained identity, including old retry groups.
// A finalized success wins over sibling failures; failed attempts alone cannot
// release liability while another signed identity might still redeem the ticket.
func ReconcileSubmitted(ctx context.Context, store *RedeemerDB, chain ReceiptReader) []error {
	if store == nil || store.binding == nil {
		return []error{errors.New("bound redemption store required")}
	}
	items, err := store.submittedRedemptions(ctx, true)
	if err != nil {
		return []error{err}
	}
	var failures []error
	successes := map[string]bool{}
	reverts := map[string]common.Hash{}
	pending := map[common.Hash]bool{}
	visited := 0
	for _, item := range items {
		if ctx.Err() != nil {
			failures = append(failures, ctx.Err())
			break
		}
		visited++
		store.receiptCursor.after.Store(item.rowid)
		receipt, err := chain.FinalizedReceipt(ctx, item.Hash)
		id := string(item.Signature)
		if err != nil {
			failures = append(failures, fmt.Errorf("receipt %s: %w", item.Hash.Hex(), err))
			continue
		}
		if receipt != nil && receipt.Status == types.ReceiptStatusSuccessful {
			successes[id] = true
			err = store.finishTicket(ctx, item.Signature, "confirmed", item.Hash.Hex(), eth.ChainSnapshot{Number: receipt.BlockNumber, BlockHash: receipt.BlockHash})
		} else if receipt != nil && receipt.Status == types.ReceiptStatusFailed {
			reverts[id] = item.Hash
			err = store.finishTicket(ctx, item.Signature, "reverted", item.Hash.Hex())
		} else {
			pending[item.Hash] = true
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	store.receiptCursor.finish(visited == len(items) && len(items) < 128)
	for id, hash := range reverts {
		if successes[id] {
			continue
		}
		failures = append(failures, fmt.Errorf("transaction failed: redemption %s reverted", hash.Hex()))
	}
	if reader, ok := chain.(finalizedNonceReader); ok {
		failures = append(failures, store.reconcileFinalizedNonces(ctx, reader, pending)...)
	}
	return failures
}
