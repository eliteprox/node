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
	chain             PaymentChain
	interval, maxAge  time.Duration
	now               func() time.Time
	mu                sync.RWMutex
	current           ChainSnapshot
	header            *miniHeader
	generation        uint64
	requireL1         bool
	invalid, stopped  bool
	contact, progress time.Time
	l1Progress        time.Time
	l1HighWater       *big.Int
}

func NewPaymentObserver(chain PaymentChain, requireL1 bool) *PaymentObserver {
	chain.Observer = nil
	return &PaymentObserver{chain: chain, interval: 5 * time.Second, maxAge: 30 * time.Second, now: time.Now, requireL1: requireL1 || chain.Contracts.RPC.requireL1}
}

// Run has one service-owned caller. Every RPC inherits the service lifetime.
func (o *PaymentObserver) Run(ctx context.Context) {
	defer func() { o.mu.Lock(); o.stopped = true; o.mu.Unlock() }()
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		read, cancel := context.WithTimeout(ctx, o.maxAge)
		_ = o.refresh(read)
		cancel()
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
	Healthy                                             bool
}

func (o *PaymentObserver) Status() ObserverStatus {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return ObserverStatus{o.current.observedAt, o.contact, o.progress, o.l1Progress, o.validate(o.current) == nil}
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
	s := o.current
	s.Block, s.Round, s.CurrentRound = new(big.Int).Set(s.Block), new(big.Int).Set(s.Round), new(big.Int).Set(s.CurrentRound)
	return s, nil
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
	if o.stopped || o.invalid || s.generation != o.generation || o.expired(s, o.l1Progress) {
		return ErrObservationUnavailable
	}
	return nil
}

func (o *PaymentObserver) expired(s ChainSnapshot, l1Progress time.Time) bool {
	now := o.now()
	return s.observedAt.IsZero() || now.Sub(s.observedAt) >= o.maxAge || now.Sub(s.blockTime) >= o.maxAge || now.Sub(l1Progress) >= o.maxAge
}

// Only Run calls refresh. Previous/current identity is the extension point for
// complete hash-pinned logs or reset; no partial delta authorizes publication.
func (o *PaymentObserver) refresh(ctx context.Context) error {
	at := o.now()
	head, err := o.chain.Contracts.RPC.header(ctx, "latest")
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.contact = o.now()
	prior := o.header
	gap := false
	if head.Hash == (common.Hash{}) || head.Number == nil {
		o.mu.Unlock()
		return ErrObservationUnavailable
	}
	if prior != nil {
		distance := new(big.Int).Sub((*big.Int)(head.Number), (*big.Int)(prior.Number))
		if distance.Sign() < 0 {
			o.invalid = true
			o.generation++
			o.mu.Unlock()
			return ErrObservationUnavailable
		}
		gap = head.Hash != prior.Hash && distance.Cmp(big.NewInt(1)) > 0
		if head.Hash != prior.Hash && !gap && (distance.Sign() == 0 || head.ParentHash != prior.Hash) {
			o.invalid = true
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
		o.current.generation, o.invalid = o.generation, false
		err := o.validate(o.current)
		o.mu.Unlock()
		return err
	}
	o.mu.Unlock()
	if gap {
		// A polling gap does not itself orphan the retained observation. Prove
		// its anchor before keeping it available during the pinned backfill.
		anchor, err := o.chain.Contracts.RPC.header(ctx, hexutil.EncodeBig((*big.Int)(prior.Number)))
		if err != nil || anchor.Hash != prior.Hash || anchor.Number == nil || (*big.Int)(anchor.Number).Cmp((*big.Int)(prior.Number)) != 0 {
			o.mu.Lock()
			o.invalid = true
			o.generation++
			o.mu.Unlock()
		}
	}
	s, err := o.chain.snapshotAt(ctx, head, true)
	if err != nil {
		return err
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
	if o.stopped || o.expired(s, l1Progress) {
		return ErrObservationUnavailable
	}
	o.current, o.header, o.invalid = s, head, false
	o.progress, o.l1Progress, o.l1HighWater = progress, l1Progress, highWater
	o.requireL1 = o.requireL1 || head.L1BlockNumber != nil
	return nil
}
