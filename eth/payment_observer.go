// Adapted from livepeer/go-livepeer eth/watchers/timewatcher.go and
// eth/blockwatch/block_watcher.go at 597f882cb9f4d42ad8430b0f592afd5929af4a6b:
// Nico Vergauwen (b4605f705, 3aec3aa16), Yondon Fu (0acd88bbd), and
// Rafał Leszko (4b6ede31f, 8c7ec0155). Immutable pinned observations and age
// refusal also adapt Mike Zupper's BlueClawNetwork/livepeer-node eth/chainview.go
// (2d95d40b8, 74f41b11f). Copyright (c) 2017 Livepeer; MIT notice in LICENSE.
package eth

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var ErrObservationUnavailable = errors.New("chain observation unavailable")

// PaymentObserver publishes complete canonical observations. Gaps and reorgs
// rebuild state at the new hash instead of replaying uncertain event history.
type PaymentObserver struct {
	OnRefresh         func(time.Duration, error) // Install before Run; called serially.
	chain             PaymentChain
	interval, maxAge  time.Duration
	now               func() time.Time
	mu                sync.RWMutex
	current           ChainSnapshot
	header            *miniHeader
	generation        uint64
	requireL1         bool
	contact, progress time.Time
	l1Progress        time.Time
	l1HighWater       *big.Int
	recipient         common.Address
	lifecycle         context.Context
	payers            map[payerKey]*payerEntry
	payerReads        sync.WaitGroup
	activeReads       int
}

func NewPaymentObserver(chain PaymentChain, requireL1 bool, recipient common.Address) *PaymentObserver {
	chain.Observer = nil
	return &PaymentObserver{chain: chain, interval: 5 * time.Second, maxAge: 30 * time.Second, now: time.Now, requireL1: requireL1 || chain.Contracts.RPC.requireL1, recipient: recipient}
}

// Run has one service-owned caller. Every RPC inherits the service lifetime.
func (o *PaymentObserver) Run(ctx context.Context) {
	ctx = WithRPCWork(ctx, "background")
	o.mu.Lock()
	lifecycle, cancelReads := context.WithCancel(ctx)
	o.lifecycle = lifecycle
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		cancelReads()
		o.mu.Unlock()
		o.payerReads.Wait()
	}()
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		start := time.Now()
		read, cancel := context.WithTimeout(ctx, o.maxAge)
		err := o.refresh(read)
		cancel()
		if o.OnRefresh != nil {
			o.OnRefresh(time.Since(start), err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ObserverStatus separates successful contact from accepted state and progress.
// A single provider cannot establish an absolute bound on protocol L1 lag.
type ObserverStatus struct {
	ObservedAt, ContactAt, HeadProgressAt, L1ProgressAt time.Time
	Healthy, RecipientActive                            bool
	RecipientTracked, RecipientFailed                   bool
	PayerEntries, PayerReads, PayerFailures             int
}

func (o *PaymentObserver) Status() ObserverStatus {
	o.mu.RLock()
	defer o.mu.RUnlock()
	healthy := o.validate(o.current) == nil
	status := ObserverStatus{
		ObservedAt: o.current.observedAt, ContactAt: o.contact, HeadProgressAt: o.progress, L1ProgressAt: o.l1Progress,
		RecipientActive: healthy && o.recipient != (common.Address{}) && o.current.recipientErr == nil && o.current.recipientActive, PayerEntries: len(o.payers), PayerReads: o.activeReads, Healthy: healthy, RecipientTracked: o.recipient != (common.Address{}), RecipientFailed: o.current.recipientErr != nil,
	}
	for _, entry := range o.payers {
		if !entry.valid && entry.failure != nil && entry.failedAt == o.current.BlockHash && entry.failedGeneration == o.current.generation {
			status.PayerFailures++
		}
	}
	return status
}

func (o *PaymentObserver) Snapshot(ctx context.Context) (ChainSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ChainSnapshot{}, err
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if err := o.validate(o.current); err != nil {
		return ChainSnapshot{}, err
	}
	return o.current.clone(), nil
}

func (s ChainSnapshot) clone() ChainSnapshot {
	for _, value := range []**big.Int{&s.Block, &s.Number, &s.Round, &s.CurrentRound} {
		*value = new(big.Int).Set(*value)
	}
	return s
}

func (s ChainSnapshot) ObservedAt() time.Time {
	if s.observedAt.IsZero() { // Injected direct chains have no retained observation.
		return time.Now()
	}
	return s.observedAt
}

// Validate also rejects snapshots retained across an invalidation or shutdown.
// Direct reads and injected test chains retain their existing behavior.
func (s ChainSnapshot) Validate() error {
	if s.observer == nil {
		return nil
	}
	s.observer.mu.RLock()
	defer s.observer.mu.RUnlock()
	return s.observer.validate(s)
}

func (o *PaymentObserver) validate(s ChainSnapshot) error {
	if (o.lifecycle != nil && o.lifecycle.Err() != nil) || s.generation != o.generation || o.expired(s, o.l1Progress) {
		return ErrObservationUnavailable
	}
	return nil
}

func (o *PaymentObserver) expired(s ChainSnapshot, l1Progress time.Time) bool {
	now := o.now()
	return s.observedAt.IsZero() || now.Sub(s.observedAt) >= o.maxAge || now.Sub(s.blockTime) >= o.maxAge || now.Sub(l1Progress) >= o.maxAge
}

// Only Run calls refresh.
func (o *PaymentObserver) refresh(ctx context.Context) error {
	at := o.now()
	head, err := o.chain.Contracts.RPC.header(ctx, "latest")
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.contact = o.now()
	prior := o.header
	gap, continuous := false, false
	if head.Hash == (common.Hash{}) || head.Number == nil {
		o.mu.Unlock()
		return ErrObservationUnavailable
	}
	if prior != nil {
		distance := new(big.Int).Sub((*big.Int)(head.Number), (*big.Int)(prior.Number))
		if distance.Sign() < 0 {
			o.generation++
			o.mu.Unlock()
			return ErrObservationUnavailable
		}
		gap = head.Hash != prior.Hash && distance.Cmp(big.NewInt(1)) > 0
		continuous = distance.Cmp(big.NewInt(1)) == 0 && head.ParentHash == prior.Hash
		if head.Hash != prior.Hash && !gap && (distance.Sign() == 0 || head.ParentHash != prior.Hash) {
			o.generation++
		}
	}
	// Usable identity conflicts invalidate even when other header metadata is
	// missing or stale. Such metadata must not restore a rejected observation.
	if head.Timestamp == nil || (o.requireL1 && head.L1BlockNumber == nil) {
		o.mu.Unlock()
		return ErrObservationUnavailable
	}
	blockTime := time.Unix(int64(*head.Timestamp), 0)
	if at.Sub(blockTime) >= o.maxAge || blockTime.After(at.Add(o.maxAge)) {
		o.mu.Unlock()
		return ErrObservationUnavailable
	}
	if prior != nil && head.Hash == prior.Hash {
		// A lagging backend may return N, N-1, then N again. Restore the
		// immutable observation without renewing any freshness deadline.
		o.current.generation = o.generation
		err := o.validate(o.current)
		o.mu.Unlock()
		return err
	}
	o.mu.Unlock()
	if gap {
		// A polling gap does not itself orphan the retained observation. Prove
		// its anchor before keeping it available during the pinned backfill.
		anchor, err := o.chain.Contracts.RPC.header(ctx, hexutil.EncodeBig((*big.Int)(prior.Number)))
		continuous = err == nil && anchor.Hash == prior.Hash && anchor.Number != nil && (*big.Int)(anchor.Number).Cmp((*big.Int)(prior.Number)) == 0
		if !continuous {
			o.invalidate()
		}
	}
	s, err := o.chain.snapshotAt(ctx, head, true)
	if err != nil {
		return err
	}
	// Local admission follows go-livepeer/core/orchestrator.go isActive at
	// 597f882c (Nico Vergauwen, 5bfee8f7; MIT, see LICENSE). Retain node's
	// canonical contract predicate rather than duplicating round arithmetic.
	// Per-entry failure isolation adapts Mike Zupper's PR #1 eth/chainview.go,
	// 68c7834921c7218d16dbd883e2b7e87f6d7804d5 (MIT, see LICENSE).
	if o.recipient != (common.Address{}) {
		s.recipientActive, s.recipientErr = o.chain.activeAt(ctx, s.addresses.bonding, o.recipient, s)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	changed, reset := o.payerChanges(ctx, prior, head, s, continuous)
	if gap && reset {
		o.invalidate()
	}
	// Hash-pinned logs can outlive their canonical block, even on a reset.
	canonical, err := o.chain.Contracts.RPC.header(ctx, hexutil.EncodeBig((*big.Int)(head.Number)))
	if err != nil || canonical.Hash != head.Hash {
		o.invalidate()
		return ErrObservationUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	progress, l1Progress, highWater := o.progress, o.l1Progress, o.l1HighWater
	if prior == nil || (*big.Int)(head.Number).Cmp((*big.Int)(prior.Number)) > 0 {
		progress = at
	}
	if highWater == nil || s.Block.Cmp(highWater) > 0 {
		l1Progress, highWater = at, s.Block
	}
	s.observer, s.generation, s.observedAt, s.blockTime = o, o.generation, at, blockTime
	if (o.lifecycle != nil && o.lifecycle.Err() != nil) || o.expired(s, l1Progress) {
		return ErrObservationUnavailable
	}
	for key, entry := range o.payers {
		if _, affected := changed[key.payer]; reset || affected {
			entry.version++
			entry.valid, entry.failure = false, nil
		}
	}
	o.current, o.header = s, head
	o.progress, o.l1Progress, o.l1HighWater = progress, l1Progress, highWater
	o.requireL1 = o.requireL1 || head.L1BlockNumber != nil
	return nil
}
