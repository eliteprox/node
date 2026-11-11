package eth

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth/contracts"
)

// PaymentChain adapts the retained Livepeer contracts to pm's narrow chain
// interface. Ethereum contract reads and redemption stay in eth.
type PaymentChain struct {
	Contracts *Contracts
	Observer  *PaymentObserver // Nil for direct transaction/redemption reads.
}

func (c PaymentChain) IsActive(ctx context.Context, recipient ethcommon.Address) (bool, error) {
	snapshot, err := c.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	return c.IsActiveAt(ctx, recipient, snapshot)
}

func (c PaymentChain) IsActiveAt(ctx context.Context, recipient ethcommon.Address, snapshot ChainSnapshot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := snapshot.Validate(); err != nil {
		return false, err
	}
	if c.Observer != nil || snapshot.observer != nil {
		if snapshot.observer != c.Observer || c.Contracts != c.Observer.chain.Contracts || recipient == (ethcommon.Address{}) || recipient != c.Observer.recipient {
			return false, ErrObservationUnavailable
		}
		return snapshot.recipientActive, snapshot.recipientErr
	}
	if snapshot.blockReference == nil {
		return false, errors.New("canonical snapshot required")
	}
	if snapshot.source != nil && snapshot.source.Controller != c.Contracts.Controller {
		return false, ErrObservationUnavailable
	}
	if snapshot.addresses.bonding != (ethcommon.Address{}) {
		return c.activeAt(ctx, snapshot.addresses.bonding, recipient, snapshot)
	}
	address, err := c.Contracts.ResolveAt(ctx, snapshot.blockReference, "bondingManager")
	if err != nil {
		return false, err
	}
	return c.activeAt(ctx, address, recipient, snapshot)
}

func (c PaymentChain) activeAt(ctx context.Context, address, recipient ethcommon.Address, snapshot ChainSnapshot) (bool, error) {
	bonding, err := contracts.NewBondingManagerCaller(address, c.Contracts.callerAt(snapshot.blockReference))
	if err != nil {
		return false, err
	}
	return bonding.IsActiveTranscoder(&bind.CallOpts{Context: ctx}, recipient)
}

// FinalizedReceipt retains canonical inclusion evidence for durable collateral admission.
func (c PaymentChain) FinalizedReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	receipt, err := c.Contracts.RPC.TransactionReceipt(ctx, hash)
	if errors.Is(err, ethereum.NotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	finalized, err := c.Contracts.RPC.header(ctx, "finalized")
	if err != nil {
		return nil, err
	}
	if finalized.Number == nil {
		return nil, errors.New("invalid finalized header")
	}
	if (*big.Int)(finalized.Number).Cmp(receipt.BlockNumber) < 0 {
		return nil, nil
	}
	canonical, err := c.Contracts.RPC.header(ctx, hexutil.EncodeBig(receipt.BlockNumber))
	if err != nil {
		return nil, err
	}
	if canonical.Hash != receipt.BlockHash {
		return nil, nil
	}
	return receipt, nil
}

// Address retention adapts livepeer/go-livepeer eth/client.go setContracts at
// 597f882cb9f4d42ad8430b0f592afd5929af4a6b (Nico Vergauwen, 829cd9047c46aef03ff5bf07b3e34be810694b74;
// Rafał Leszko, 3209dd946bb0376308044055b6caf262fd25cf61). Confirmation before
// dependent reads adapts Mike Zupper's BlueClawNetwork/livepeer-node
// eth/chainview.go, 2d95d40b8aa4699c7ca40838b6a9c312704c93db.
// Copyright (c) 2017 Livepeer; MIT notice in LICENSE.
type paymentContracts struct{ rounds, broker, bonding ethcommon.Address }

type ChainSnapshot struct {
	Block                *big.Int // Protocol L1 block, including on Arbitrum.
	Number               *big.Int // Execution-chain height, distinct from the L1 clock.
	BlockHash            ethcommon.Hash
	Round                *big.Int // Last initialized round.
	RoundHash            ethcommon.Hash
	TicketValidityPeriod *big.Int // TicketBroker validity period at this block.
	CurrentRound         *big.Int // Protocol round; may exceed the initialized Round.
	source               *Contracts
	blockReference       map[string]any
	addresses            paymentContracts
	observer             *PaymentObserver
	generation           uint64
	observedAt           time.Time
	blockTime            time.Time
	recipientActive      bool
	recipientErr         error
}

func (c PaymentChain) Snapshot(ctx context.Context) (ChainSnapshot, error) {
	if c.Observer != nil {
		return c.Observer.Snapshot(ctx)
	}
	at := time.Now()
	header, err := c.Contracts.RPC.header(ctx, "latest")
	if err != nil {
		return ChainSnapshot{}, err
	}
	snapshot, err := c.snapshotAt(ctx, header)
	snapshot.observedAt = at
	return snapshot, err
}

func (c PaymentChain) snapshotAt(ctx context.Context, header *miniHeader) (ChainSnapshot, error) {
	if header.Hash == (ethcommon.Hash{}) || header.Number == nil {
		return ChainSnapshot{}, errors.New("invalid chain header")
	}
	if c.Contracts.RPC.requireL1 && header.L1BlockNumber == nil {
		return ChainSnapshot{}, ErrObservationUnavailable
	}
	clock := header.Number
	if header.L1BlockNumber != nil {
		clock = header.L1BlockNumber
	}
	block := new(big.Int).Set((*big.Int)(clock))
	// Bind every read to the canonical block, including Controller resolution.
	ref := map[string]any{"blockHash": header.Hash.Hex(), "requireCanonical": true}
	var addresses paymentContracts
	registry := []struct {
		name string
		to   *ethcommon.Address
	}{{"roundsManager", &addresses.rounds}, {"ticketBroker", &addresses.broker}, {"bondingManager", &addresses.bonding}}
	for _, entry := range registry { // Confirm the complete scope before dependent reads.
		address, err := c.Contracts.ResolveAt(ctx, ref, entry.name)
		if err != nil {
			return ChainSnapshot{}, err
		}
		*entry.to = address
	}
	rounds, err := contracts.NewRoundsManagerCaller(addresses.rounds, c.Contracts.callerAt(ref))
	if err != nil {
		return ChainSnapshot{}, err
	}
	opts := &bind.CallOpts{Context: ctx}
	round, err := rounds.LastInitializedRound(opts)
	if err != nil {
		return ChainSnapshot{}, err
	}
	if !round.IsInt64() || round.Sign() <= 0 {
		return ChainSnapshot{}, errors.New("invalid initialized round")
	}
	hash, err := rounds.BlockHashForRound(opts, round)
	if err != nil {
		return ChainSnapshot{}, err
	}
	if hash == ([32]byte{}) {
		return ChainSnapshot{}, errors.New("invalid round block hash")
	}
	snapshot := ChainSnapshot{source: c.Contracts, Block: block, Number: new(big.Int).Set((*big.Int)(header.Number)), BlockHash: header.Hash, Round: round, RoundHash: ethcommon.Hash(hash), blockReference: ref, addresses: addresses}
	snapshot.CurrentRound, err = rounds.CurrentRound(opts)
	if err != nil {
		return ChainSnapshot{}, err
	}
	if snapshot.CurrentRound.Cmp(round) < 0 {
		return ChainSnapshot{}, errors.New("current round precedes initialized round")
	}
	broker, err := contracts.NewTicketBrokerCaller(addresses.broker, c.Contracts.callerAt(ref))
	if err != nil {
		return ChainSnapshot{}, err
	}
	snapshot.TicketValidityPeriod, err = broker.TicketValidityPeriod(opts)
	if err != nil {
		return ChainSnapshot{}, err
	}
	if snapshot.TicketValidityPeriod == nil || snapshot.TicketValidityPeriod.Sign() <= 0 || snapshot.TicketValidityPeriod.BitLen() > 256 {
		return ChainSnapshot{}, errors.New("invalid ticket validity period")
	}

	return snapshot, nil
}

// SenderInfo is a coherent view of a sender's collateral and the amount of
// reserve this particular recipient can claim in the initialized round.
// A zero recipient requests total remaining reserve, for sender readiness.
// Sender/reserve semantics follow go-livepeer/eth/client_ticketbroker.go,
// originally by Yondon Fu and Nico Vergauwen, with L1-round handling by
// Rafał Leszko (4b6ede31f040a084ff9e55bd798a0d6fffee1e1b).
type SenderInfo struct {
	Snapshot                        ChainSnapshot
	Deposit, Reserve, WithdrawRound *big.Int
}

func (c PaymentChain) PayerFunds(ctx context.Context, sender, recipient ethcommon.Address) (SenderInfo, error) {
	if c.Observer != nil {
		if c.Contracts != c.Observer.chain.Contracts {
			return SenderInfo{}, errors.New("payment observer belongs to another contract client")
		}
		return c.Observer.senderInfo(ctx, sender, recipient)
	}
	snapshot, err := c.Snapshot(ctx)
	if err != nil {
		return SenderInfo{}, err
	}
	return c.senderInfoAt(ctx, sender, recipient, snapshot)
}

func (c PaymentChain) senderInfoAt(ctx context.Context, sender, recipient ethcommon.Address, snapshot ChainSnapshot) (SenderInfo, error) {
	address := snapshot.addresses.broker
	if address == (ethcommon.Address{}) {
		var err error
		address, err = c.Contracts.ResolveAt(ctx, snapshot.blockReference, "ticketBroker")
		if err != nil {
			return SenderInfo{}, err
		}
	}
	broker, err := contracts.NewTicketBrokerCaller(address, c.Contracts.callerAt(snapshot.blockReference))
	if err != nil {
		return SenderInfo{}, err
	}
	opts := &bind.CallOpts{Context: ctx}
	info, err := broker.GetSenderInfo(opts, sender)
	if err != nil {
		return SenderInfo{}, err
	}
	reserve := info.Reserve.FundsRemaining
	if recipient != (ethcommon.Address{}) {
		reserve, err = broker.ClaimableReserve(opts, sender, recipient)
		if err != nil {
			return SenderInfo{}, err
		}
	}
	return SenderInfo{Snapshot: snapshot, Deposit: info.Sender.Deposit, Reserve: reserve, WithdrawRound: info.Sender.WithdrawRound}, snapshot.Validate()
}

// The redemption tuple follows Yondon Fu's go-livepeer/eth/client_ticketbroker.go
// RedeemWinningTicket (ffcefd6341d14696e2718c1a42875428cd2cc953).
type RedeemTicket struct {
	Recipient         ethcommon.Address
	Sender            ethcommon.Address
	FaceValue         *big.Int
	WinProb           *big.Int
	SenderNonce       uint32
	RecipientRandHash ethcommon.Hash
	AuxData           []byte
	Signature         []byte
	RecipientRand     *big.Int
}

func (c PaymentChain) PrepareRedemption(ctx context.Context, redeemer *Key, chainID *big.Int, t RedeemTicket) (SignedTransaction, error) {
	plan, err := c.redemptionPlan(ctx, redeemer, t)
	if err != nil {
		return SignedTransaction{}, err
	}
	return c.Contracts.Prepare(ctx, plan, redeemer, chainID)
}

func (c PaymentChain) redemptionPlan(ctx context.Context, redeemer *Key, t RedeemTicket) (TransactionPlan, error) {
	if redeemer == nil || t.Recipient == (ethcommon.Address{}) || t.FaceValue == nil || t.WinProb == nil || t.RecipientRand == nil {
		return TransactionPlan{}, errors.New("incomplete redemption configuration or ticket")
	}
	address, err := c.Contracts.Resolve(ctx, "ticketBroker")
	if err != nil {
		return TransactionPlan{}, err
	}
	coreTicket := contracts.MTicketBrokerCoreTicket{Recipient: t.Recipient, Sender: t.Sender, FaceValue: t.FaceValue, WinProb: t.WinProb, SenderNonce: new(big.Int).SetUint64(uint64(t.SenderNonce)), RecipientRandHash: t.RecipientRandHash, AuxData: t.AuxData}
	data, err := c.Contracts.Pack("ticketBroker", "redeemWinningTicket", coreTicket, t.Signature, t.RecipientRand)
	if err != nil {
		return TransactionPlan{}, err
	}
	plan, err := c.Contracts.PlanTransaction(ctx, redeemer.Address(), address, data, big.NewInt(0))
	if err == nil && c.Contracts.CheckRedemptionCost {
		fee, ok := new(big.Int).SetString(plan.FeeCapWei, 10)
		if !ok {
			return TransactionPlan{}, errors.New("invalid redemption fee")
		}
		if t.FaceValue.Cmp(new(big.Int).Mul(new(big.Int).SetUint64(plan.GasLimit), fee)) <= 0 {
			return TransactionPlan{}, ErrUneconomicalRedemption
		}
	}
	return plan, err
}

// SubmitRedemption ports go-livepeer/eth/backend.go's Next/send/Update sequence.
// Persistence happens before send, but only an acknowledged send advances Next.
func (c PaymentChain) SubmitRedemption(ctx context.Context, nonces *NonceManager, manager *TransactionManager, redeemer *Key, chainID *big.Int, t RedeemTicket, save func(SignedTransaction) error) error {
	nonces.Lock(redeemer.Address())
	defer nonces.Unlock(redeemer.Address())
	plan, err := c.redemptionPlan(ctx, redeemer, t)
	if err != nil {
		return err
	}
	nonce, err := nonces.NextContext(ctx, redeemer.Address())
	if err != nil {
		return err
	}
	prepared, err := c.Contracts.prepareAtNonce(ctx, plan, redeemer, chainID, save, &nonce)
	if err != nil {
		return err
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(prepared.Raw); err != nil {
		return err
	}
	if err := manager.SendTransaction(ctx, &tx); err != nil {
		return err
	}
	nonces.Update(redeemer.Address(), nonce)
	return nil
}

// TicketUse adapts go-livepeer/eth/client_ticketbroker.go's IsUsedTicket.
// It returns the pinned finalized identity alongside ticket-use evidence.
func (c PaymentChain) TicketUse(ctx context.Context, hash ethcommon.Hash, block string) (bool, ChainSnapshot, error) {
	var ref any = block
	var observation ChainSnapshot
	if block == "finalized" {
		header, err := c.Contracts.RPC.header(ctx, "finalized")
		if err != nil {
			return false, ChainSnapshot{}, err
		}
		if header.Hash == (ethcommon.Hash{}) || header.Number == nil {
			return false, ChainSnapshot{}, errors.New("invalid finalized header")
		}
		ref = map[string]any{"blockHash": header.Hash, "requireCanonical": true}
		observation = ChainSnapshot{Number: new(big.Int).Set((*big.Int)(header.Number)), BlockHash: header.Hash}
	}
	address, err := c.Contracts.ResolveAt(ctx, ref, "ticketBroker")
	if err != nil {
		return false, ChainSnapshot{}, err
	}
	broker, err := contracts.NewTicketBrokerCaller(address, c.Contracts.callerAt(ref))
	if err != nil {
		return false, ChainSnapshot{}, err
	}
	used, err := broker.UsedTickets(&bind.CallOpts{Context: ctx}, hash)
	return used, observation, err
}

// FinalizedNonce pins account state to the same canonical finalized reference
// used for ticket-use settlement. A missing/malformed count is not evidence.
func (c PaymentChain) FinalizedNonce(ctx context.Context, address ethcommon.Address) (uint64, error) {
	header, err := c.Contracts.RPC.header(ctx, "finalized")
	if err != nil {
		return 0, err
	}
	if header.Hash == (ethcommon.Hash{}) || header.Number == nil || !(*big.Int)(header.Number).IsUint64() {
		return 0, errors.New("invalid finalized header")
	}
	var nonce *hexutil.Uint64
	ref := map[string]any{"blockHash": header.Hash, "requireCanonical": true}
	if err := c.Contracts.RPC.ethereum.Client().CallContext(ctx, &nonce, "eth_getTransactionCount", address, ref); err != nil {
		return 0, safeRPCError(err)
	}
	if nonce == nil {
		return 0, errors.New("invalid finalized transaction count")
	}
	return uint64(*nonce), nil
}

// InclusionReceipt retains node's canonical-block check without treating an
// unfinalized inclusion as settled. The ported manager only waits for inclusion.
func (c PaymentChain) InclusionReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	receipt, err := c.Contracts.RPC.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, err
	}
	header, err := c.Contracts.RPC.header(ctx, hexutil.EncodeBig(receipt.BlockNumber))
	if err != nil {
		return nil, err
	}
	if header.Hash != receipt.BlockHash {
		return nil, ethereum.NotFound
	}
	return receipt, nil
}

func (c PaymentChain) Redeem(ctx context.Context, redeemer *Key, chainID *big.Int, t RedeemTicket) (ethcommon.Hash, error) {
	tx, err := c.PrepareRedemption(ctx, redeemer, chainID, t)
	if err != nil {
		return ethcommon.Hash{}, err
	}
	return tx.Hash, c.Contracts.Broadcast(ctx, tx)
}
