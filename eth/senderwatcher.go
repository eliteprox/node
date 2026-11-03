// Sender-key event invalidation and pinned recovery adapt livepeer/go-livepeer
// eth/watchers/senderwatcher.go (341b39a0c, 57e288706), by Nico Vergauwen.
// Immutable results, idle eviction and shared cold reads adapt Mike Zupper's
// BlueClawNetwork/livepeer-node eth/chainview.go (898977754, 5f20cab0f).
// Copyright (c) 2017 Livepeer; MIT notice in LICENSE.
package eth

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

const payerEntries = 1024
const payerReadLimit = 4 // No distinct-key queue; cancelled jobs retain their slot until exit.
const payerIdle = 10 * time.Minute

type payerKey struct{ payer, recipient common.Address }
type payerEntry struct {
	info             SenderInfo
	valid            bool
	used             time.Time
	version          uint64
	failure          error
	failedAt         common.Hash
	failedGeneration uint64
	job              *payerRead
}
type payerRead struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	info    SenderInfo
	err     error
}

func (i SenderInfo) clone() SenderInfo {
	i.Snapshot = i.Snapshot.clone()
	i.Deposit, i.Reserve, i.WithdrawRound = new(big.Int).Set(i.Deposit), new(big.Int).Set(i.Reserve), new(big.Int).Set(i.WithdrawRound)
	return i
}

func (o *PaymentObserver) senderInfo(ctx context.Context, payer, recipient common.Address) (SenderInfo, error) {
	if err := ctx.Err(); err != nil {
		return SenderInfo{}, err
	}
	o.mu.Lock()
	if err := o.validate(o.current); err != nil {
		o.mu.Unlock()
		return SenderInfo{}, err
	}
	key, now := payerKey{payer, recipient}, o.now()
	entry := o.payers[key]
	if entry == nil {
		if o.activeReads >= payerReadLimit {
			o.mu.Unlock()
			return SenderInfo{}, ErrObservationUnavailable
		}
		var oldest payerKey
		var last time.Time
		for k, e := range o.payers {
			if e.job == nil && now.Sub(e.used) >= payerIdle {
				delete(o.payers, k)
				continue
			}
			if e.job == nil && (last.IsZero() || e.used.Before(last)) {
				oldest, last = k, e.used
			}
		}
		if len(o.payers) >= payerEntries {
			delete(o.payers, oldest)
		}
		if o.payers == nil {
			o.payers = make(map[payerKey]*payerEntry)
		}
		entry = &payerEntry{}
		o.payers[key] = entry
	}
	entry.used = now
	if entry.valid {
		info := entry.info
		info.Snapshot = o.current
		o.mu.Unlock()
		return info.clone(), nil
	}
	if entry.failure != nil && entry.failedAt == o.current.BlockHash && entry.failedGeneration == o.current.generation {
		err := entry.failure
		o.mu.Unlock()
		return SenderInfo{}, err
	}
	job := entry.job
	if job == nil {
		if o.activeReads >= payerReadLimit {
			o.mu.Unlock()
			return SenderInfo{}, ErrObservationUnavailable
		}
		read, cancel := context.WithTimeout(o.lifecycle, 15*time.Second)
		job = &payerRead{done: make(chan struct{}), cancel: cancel}
		entry.job = job
		o.activeReads++
		o.payerReads.Add(1)
		go o.readPayer(read, key, entry, job, entry.version, o.current)
	}
	job.waiters++
	o.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-job.done:
	}
	o.mu.Lock()
	job.waiters--
	if job.waiters == 0 {
		job.cancel()
	}
	o.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SenderInfo{}, err
	}
	if job.err != nil {
		if errors.Is(job.err, context.Canceled) {
			return SenderInfo{}, ErrObservationUnavailable
		}
		return SenderInfo{}, job.err
	}
	if err := job.info.Snapshot.Validate(); err != nil {
		return SenderInfo{}, err
	}
	return job.info.clone(), nil
}

func (o *PaymentObserver) readPayer(ctx context.Context, key payerKey, entry *payerEntry, job *payerRead, version uint64, snapshot ChainSnapshot) {
	defer o.payerReads.Done()
	defer job.cancel()
	info, err := o.chain.senderInfoAt(ctx, key.payer, key.recipient, snapshot)
	o.mu.Lock()
	defer o.mu.Unlock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = o.validate(snapshot)
	}
	if err == nil && entry.version != version {
		err = ErrObservationUnavailable
	}
	if err == nil {
		// Complete intervening logs left this entry unchanged; advance metadata too.
		info.Snapshot = o.current
		entry.info, entry.valid = info, true
		entry.failure = nil
	} else if ctx.Err() == nil && entry.version == version && !errors.Is(err, ErrObservationUnavailable) {
		entry.failure, entry.failedAt, entry.failedGeneration = err, snapshot.BlockHash, snapshot.generation
	}
	job.info, job.err = info, err
	entry.job = nil
	o.activeReads--
	close(job.done)
}

func (o *PaymentObserver) invalidate() {
	o.mu.Lock()
	o.invalid = true
	o.generation++
	o.mu.Unlock()
}

// Link each intervening block to the retained anchor and read its logs by hash.
// Incomplete history resets entries; refresh confirms the final canonical head.
func (o *PaymentObserver) payerChanges(ctx context.Context, prior, head *miniHeader, snapshot ChainSnapshot, continuous bool) (map[common.Address]bool, bool) {
	o.mu.RLock()
	reset := !continuous || (prior != nil && new(big.Int).Sub((*big.Int)(head.Number), (*big.Int)(prior.Number)).Cmp(big.NewInt(128)) > 0) ||
		o.current.CurrentRound.Cmp(snapshot.CurrentRound) != 0 || o.current.addresses != snapshot.addresses
	o.mu.RUnlock()
	if reset {
		return nil, true
	}
	addresses := []common.Address{snapshot.addresses.broker, snapshot.addresses.bonding, o.chain.Contracts.Controller}
	changed := make(map[common.Address]bool)
	broker, bonding := o.chain.Contracts.abis["ticketBroker"], o.chain.Contracts.abis["bondingManager"]
	block := head
	for remaining := new(big.Int).Sub((*big.Int)(head.Number), (*big.Int)(prior.Number)).Int64(); remaining > 0; remaining-- {
		if remaining == 1 && block.ParentHash != prior.Hash {
			return nil, true
		}
		if bloom := block.LogsBloom; bloom == nil ||
			bloom.Test(addresses[0].Bytes()) ||
			bloom.Test(addresses[1].Bytes()) ||
			bloom.Test(addresses[2].Bytes()) {
			logs, err := o.chain.Contracts.RPC.ethereum.FilterLogs(ctx, ethereum.FilterQuery{BlockHash: &block.Hash, Addresses: addresses})
			if err != nil {
				return nil, true
			}
			for _, log := range logs {
				if log.Removed || log.BlockHash != block.Hash || log.BlockNumber != (*big.Int)(block.Number).Uint64() || len(log.Topics) == 0 {
					return nil, true
				}
				if log.Address == addresses[2] {
					// Proxy targets can change while all three public addresses stay fixed.
					if log.Topics[0] == o.chain.Contracts.abis["controller"].Events["SetContractInfo"].ID {
						return nil, true
					}
					continue
				}
				if log.Address == addresses[1] {
					for _, name := range []string{"TranscoderActivated", "TranscoderDeactivated", "ParameterUpdate", "SetController"} {
						if log.Topics[0] == bonding.Events[name].ID {
							return nil, true
						}
					}
					continue
				}
				for _, name := range []string{"DepositFunded", "ReserveFunded", "ReserveClaimed", "Withdrawal", "WinningTicketTransfer", "Unlock", "UnlockCancelled"} {
					if log.Topics[0] == broker.Events[name].ID {
						if len(log.Topics) < 2 {
							return nil, true
						}
						changed[common.BytesToAddress(log.Topics[1].Bytes())] = true
					}
				}
				if log.Topics[0] == broker.Events["SetController"].ID {
					return nil, true
				}
			}
		}
		if remaining > 1 {
			number := new(big.Int).Sub((*big.Int)(block.Number), big.NewInt(1))
			parent, err := o.chain.Contracts.RPC.header(ctx, hexutil.EncodeBig(number))
			if err != nil || parent.Number == nil || (*big.Int)(parent.Number).Cmp(number) != 0 || parent.Hash != block.ParentHash {
				return nil, true
			}
			block = parent
		}
	}
	return changed, false
}
