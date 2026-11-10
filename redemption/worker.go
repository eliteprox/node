// Queue scheduling and the face-value/cost guard adapt go-livepeer pm/queue.go
// and pm/sendermonitor.go at afaaa36e743d39e6f460e7993d516d52b3a81341.
// Original authors: Nico Vergauwen, Yondon Fu and Rafał Leszko. MIT: LICENSE.
package redemption

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/pm"
)

// Destination acknowledges submission/handoff, never financial settlement.
type Destination interface {
	Submit(context.Context, *pm.SignedTicket) error
}

type ChainDestination struct {
	store   *RedeemerDB
	chain   eth.PaymentChain
	key     *eth.Key
	chainID *big.Int
}

func NewChainDestination(ctx context.Context, s *RedeemerDB, c eth.PaymentChain, k *eth.Key, id *big.Int, options RedemptionOptions) (*ChainDestination, error) {
	if id == nil || id.Sign() <= 0 || (s != nil && s.binding != nil && s.binding.ChainID != id.String()) {
		return nil, errors.New("invalid redemption chain ID")
	}
	if err := StartRedemptionRecovery(ctx, s, c, k, options); err != nil {
		return nil, err
	}
	return &ChainDestination{s, c, k, new(big.Int).Set(id)}, nil
}

func (d *ChainDestination) Submit(ctx context.Context, t *pm.SignedTicket) error {
	r := d.store.recovery
	return d.chain.SubmitRedemption(ctx, r.nonces, r.manager, d.key, d.chainID, redeemTicket(t), func(tx eth.SignedTransaction) error { return d.store.RecordPrepared(ctx, t, tx) })
}

type Worker struct {
	store       *RedeemerDB
	chain       eth.PaymentChain
	destination Destination
	mu          sync.Mutex
}

func NewWorker(s *RedeemerDB, c eth.PaymentChain, d Destination) (*Worker, error) {
	if s == nil || s.binding == nil || c.Contracts == nil || d == nil || c.Contracts.Controller != s.binding.Controller {
		return nil, errors.New("bound redemption store, chain and destination required")
	}
	w := &Worker{store: s, chain: c, destination: d}
	s.worker = w
	return w, nil
}

// Process performs a bounded, rotating pass. Inclusion recovery belongs to the
// daemon lifetime; a slow payer cannot monopolize the next pass's starting point.
func (w *Worker) Process(ctx context.Context) []error {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.store
	failures := ReconcileSubmitted(ctx, s, w.chain)
	_, local := w.destination.(*ChainDestination)
	if local {
		failures = append(failures, s.recovery.resume(ctx)...)
	}
	latest, err := w.chain.RedemptionSnapshot(ctx, "latest")
	if err != nil {
		return append(failures, err)
	}
	page, err := s.page(ctx, 128)
	if err != nil {
		return append(failures, err)
	}

	if len(page) == 0 {
		return failures
	}
	finalized, finalErr := w.chain.RedemptionSnapshot(ctx, "finalized")
	if finalErr != nil {
		failures = append(failures, finalErr)
	}
	type activity struct {
		active bool
		err    error
	}
	recipients := map[common.Address]activity{}
	for _, item := range page {
		if ctx.Err() != nil {
			return append(failures, ctx.Err())
		}
		t := item.ticket
		if finalErr == nil {
			used, e := w.chain.TicketUseAt(ctx, t.Hash(), finalized)
			if e != nil {
				failures = append(failures, e)
				continue
			}
			phase := ""
			if used {
				phase = "used"
			} else if finalized.TicketExpired(t.CreationRound) {
				phase = "expired"
			}
			if phase != "" {
				if e = s.settle(ctx, t, phase, finalized); e != nil {
					failures = append(failures, e)
				}
				continue
			}
		}
		if item.delivery == "acknowledged" || item.delivery == "rejected" {
			if e := s.postpone(ctx, t, 15*time.Second); e != nil {
				failures = append(failures, e)
			}
			continue
		}
		if latest.TicketExpired(t.CreationRound) || !latest.ValidTicketRound(t.CreationRound) || latest.CurrentRound.Cmp(latest.Round) != 0 || t.ParamsExpirationBlock.Cmp(latest.Block) > 0 {
			w.hold(ctx, item)
			continue
		}
		used, e := w.chain.TicketUseAt(ctx, t.Hash(), latest)
		if e != nil {
			failures = append(failures, e)
			w.hold(ctx, item)
			continue
		}
		if used {
			w.hold(ctx, item)
			continue
		}
		roundHash, e := w.chain.RoundHashAt(ctx, t.CreationRound, latest)
		if e != nil {
			failures = append(failures, e)
			w.hold(ctx, item)
			continue
		}
		if roundHash != t.CreationRoundBlockHash {
			w.hold(ctx, item)
			continue
		}
		a, ok := recipients[t.Recipient]
		if !ok {
			a.active, a.err = w.chain.IsActiveAt(ctx, t.Recipient, latest)
			recipients[t.Recipient] = a
		}
		if a.err != nil {
			failures = append(failures, a.err)
		}
		if !a.active || a.err != nil {
			w.hold(ctx, item)
			continue
		}
		if local {
			var active bool
			e = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM winning_tickets w JOIN redemption_attempts a ON a.sig=w.sig WHERE w.payer_address=? AND a.phase IN ('prepared','broadcast','submitted'))`, t.PayerAddress.Hex()).Scan(&active)
			if e != nil {
				failures = append(failures, e)
				continue
			}
			if active {
				w.hold(ctx, item)
				continue
			}
			var phase string
			e = s.db.QueryRowContext(ctx, "SELECT phase FROM redemption_attempts WHERE sig=?", t.Sig).Scan(&phase)
			if e == nil && phase != "retryable" {
				w.hold(ctx, item)
				continue
			}
		}
		if e = s.delivery(ctx, t, "sending", item.failures); e != nil {
			failures = append(failures, e)
			continue
		}
		e = w.destination.Submit(ctx, t)
		phase := "acknowledged"
		count := 0
		if local {
			phase = "submitted"
		}
		if e != nil {
			phase = "retryable"
			count = item.failures + 1
			var rejected *Rejection
			if errors.As(e, &rejected) {
				phase = "rejected"
			}
			if !errors.Is(e, eth.ErrUneconomicalRedemption) && !errors.Is(e, errRecoveringNonce) {
				failures = append(failures, e)
			}
		}
		if err := s.delivery(ctx, t, phase, count); err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}

func (w *Worker) hold(ctx context.Context, item queuedTicket) {
	if item.delivery == "held" {
		if err := w.store.postpone(ctx, item.ticket, 5*time.Second); err != nil {
			w.store.observeTransition("error", "", err)
		}
		return
	}
	if err := w.store.delivery(ctx, item.ticket, "held", 0); err != nil {
		w.store.observeTransition("error", "", err)
	}
}
