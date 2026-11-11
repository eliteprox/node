package eth

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth/contracts"
)

var ErrUneconomicalRedemption = errors.New("ticket face value does not cover redemption cost")
var ErrRedeemerIsOrchestrator = errors.New("redeemer wallet is registered or active as an orchestrator")

// RedemptionSnapshot pins eligibility and expiry to one execution-chain block.
// Block remains the protocol L1 clock, which is distinct on Arbitrum.
func (c PaymentChain) RedemptionSnapshot(ctx context.Context, tag string) (ChainSnapshot, error) {
	if tag != "latest" && tag != "finalized" {
		return ChainSnapshot{}, errors.New("invalid redemption block tag")
	}
	header, err := c.Contracts.RPC.header(ctx, tag)
	if err != nil {
		return ChainSnapshot{}, err
	}
	return c.snapshotAt(ctx, header)
}

func (s ChainSnapshot) TicketExpired(creation int64) bool {
	return s.CurrentRound != nil && s.TicketValidityPeriod != nil && new(big.Int).Add(big.NewInt(creation), s.TicketValidityPeriod).Cmp(s.CurrentRound) <= 0
}

func (s ChainSnapshot) ValidTicketRound(creation int64) bool {
	return creation >= 0 && s.Round != nil && s.Round.IsInt64() && creation <= s.Round.Int64() && s.CurrentRound != nil && s.TicketValidityPeriod != nil && new(big.Int).Add(big.NewInt(creation), s.TicketValidityPeriod).BitLen() <= 256 && !s.TicketExpired(creation)
}

func (c PaymentChain) TicketUseAt(ctx context.Context, hash common.Hash, s ChainSnapshot) (bool, error) {
	if err := s.Validate(); err != nil {
		return false, err
	}
	if s.blockReference == nil || s.source == nil || s.source.Controller != c.Contracts.Controller {
		return false, errors.New("canonical redemption snapshot required")
	}
	broker, err := contracts.NewTicketBrokerCaller(s.addresses.broker, c.Contracts.callerAt(s.blockReference))
	if err != nil {
		return false, err
	}
	return broker.UsedTickets(&bind.CallOpts{Context: ctx}, hash)
}

func (c PaymentChain) RoundHashAt(ctx context.Context, round int64, s ChainSnapshot) (common.Hash, error) {
	if err := s.Validate(); err != nil {
		return common.Hash{}, err
	}
	if s.blockReference == nil || s.source == nil || s.source.Controller != c.Contracts.Controller {
		return common.Hash{}, errors.New("canonical redemption snapshot required")
	}
	if s.Round != nil && s.Round.IsInt64() && round == s.Round.Int64() {
		return s.RoundHash, nil
	}
	rounds, err := contracts.NewRoundsManagerCaller(s.addresses.rounds, c.Contracts.callerAt(s.blockReference))
	if err != nil {
		return common.Hash{}, err
	}
	hash, err := rounds.BlockHashForRound(&bind.CallOpts{Context: ctx}, big.NewInt(round))
	return common.Hash(hash), err
}

// CheckRedeemerWallet is policy, not an atomic restriction in TicketBroker.
func (c PaymentChain) CheckRedeemerWallet(ctx context.Context, account common.Address) error {
	s, err := c.RedemptionSnapshot(ctx, "latest")
	if err != nil {
		return err
	}
	caller, err := contracts.NewBondingManagerCaller(s.addresses.bonding, c.Contracts.callerAt(s.blockReference))
	if err != nil {
		return err
	}
	opts := &bind.CallOpts{Context: ctx}
	registered, err := caller.IsRegisteredTranscoder(opts, account)
	if err != nil {
		return err
	}
	active, err := caller.IsActiveTranscoder(opts, account)
	if err != nil {
		return err
	}
	if registered || active {
		return ErrRedeemerIsOrchestrator
	}
	return nil
}
