package eth

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

// Registry replacement cases adapt Mike Zupper's BlueClawNetwork/livepeer-node
// eth/chainview_test.go, 7e3a51e7076aef558cffcbadd01149b727618005 (MIT; see LICENSE).
func TestObserverConfirmedPaymentContracts(t *testing.T) {
	for _, tc := range []struct {
		contract string
		round    uint64
	}{{"RoundsManager", 5}, {"TicketBroker", 5}, {"BondingManager", 5}, {"RoundsManager", 9}} {
		t.Run(tc.contract+big.NewInt(int64(tc.round)).String(), func(t *testing.T) {
			f, o, now := newTestObserver(t)
			o.recipient = common.HexToAddress("0x3000")
			require.NoError(t, o.refresh(t.Context()))
			before, err := o.Snapshot(t.Context())
			require.NoError(t, err)
			old := f.State().Contracts[tc.contract]
			moved := common.HexToAddress("0xb1")
			advanceObserver(f, *now)
			f.Update(func(s *test.ChainState) {
				s.Contracts[tc.contract] = moved
				s.Round, s.CurrentRound = tc.round, tc.round+1
				s.Results = map[test.ChainCall]string{}
				switch tc.contract {
				case "RoundsManager":
					s.RoundHash = common.HexToHash("0x9876")
					s.Results[test.ChainCall{To: old, Function: "RoundsManager.lastInitializedRound"}] = "0x" + word(5)
					s.Results[test.ChainCall{To: old, Function: "RoundsManager.currentRound"}] = "0x" + word(5)
					s.Results[test.ChainCall{To: old, Function: "RoundsManager.blockHashForRound"}] = before.RoundHash.Hex()
				case "TicketBroker":
					s.Results[test.ChainCall{To: moved, Function: "TicketBroker.getSenderInfo"}] = "0x" + word(7) + word(0) + word(8) + word(0)
					s.Results[test.ChainCall{To: moved, Function: "TicketBroker.claimableReserve"}] = "0x" + word(6)
				case "BondingManager":
					s.Results[test.ChainCall{To: moved, Function: "BondingManager.isActiveTranscoder"}] = "0x" + word(0)
				}
			})
			f.Reset()
			require.NoError(t, o.refresh(t.Context()))
			require.Equal(t, 3, f.Stats().Calls["Controller.getContract"])
			after, err := o.Snapshot(t.Context())
			require.NoError(t, err)
			require.NotEqual(t, before.addresses, after.addresses)
			require.EqualValues(t, tc.round, after.Round.Uint64())
			require.EqualValues(t, tc.round+1, after.CurrentRound.Uint64(), "protocol round can advance before initialization")
			require.Equal(t, f.State().RoundHash, after.RoundHash)
			chain := o.chain
			chain.Observer = o
			f.Reset()
			for range 3 {
				funds, err := chain.SenderInfo(t.Context(), common.HexToAddress("0x2000"), common.HexToAddress("0x3000"))
				require.NoError(t, err)
				require.Equal(t, after, funds.Snapshot)
				active, err := chain.IsActiveAt(t.Context(), common.HexToAddress("0x3000"), funds.Snapshot)
				require.NoError(t, err)
				require.Equal(t, tc.contract != "BondingManager", active)
				if tc.contract == "TicketBroker" {
					require.Equal(t, big.NewInt(7), funds.Deposit)
					require.Equal(t, big.NewInt(6), funds.Reserve)
				}
			}
			require.Zero(t, f.Stats().Calls["Controller.getContract"], "warm payment reads retain confirmed addresses")
			require.Zero(t, f.Stats().Calls["BondingManager.isActiveTranscoder"], "replacement activity belongs to the supplied observation")
		})
	}
}

func TestObserverRejectsIncompleteRegistry(t *testing.T) {
	for _, failure := range []string{"RPC failure", "zero address", "malformed address", "reorg"} {
		t.Run(failure, func(t *testing.T) {
			f, o, now := newTestObserver(t)
			require.NoError(t, o.refresh(t.Context()))
			before, err := o.Snapshot(t.Context())
			require.NoError(t, err)
			advanceObserver(f, *now)
			var calls atomic.Int32
			f.Update(func(s *test.ChainState) {
				switch failure {
				case "RPC failure":
					s.Fail = "Controller.getContract"
				case "zero address":
					s.Contracts["TicketBroker"] = common.Address{}
				case "malformed address":
					s.Results = map[test.ChainCall]string{{To: f.Controller, Function: "Controller.getContract"}: "0x01"}
				case "reorg":
					s.Before = func(_ context.Context, name string) {
						if name == "Controller.getContract" && calls.Add(1) == 3 {
							f.Update(func(s *test.ChainState) { s.Hash = common.HexToHash("0xdead") })
						}
					}
				}
			})
			f.Reset()
			require.Error(t, o.refresh(t.Context()))
			after, err := o.Snapshot(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, after, "a failed registry candidate cannot publish any state or renew age")
			require.Zero(t, f.Stats().Calls["RoundsManager.lastInitializedRound"], "confirm the entire registry before dependent reads")
			f.Update(func(s *test.ChainState) {
				s.Fail, s.Results, s.Before = "", nil, nil
				s.Contracts["TicketBroker"] = common.HexToAddress("0xa3")
			})
			require.NoError(t, o.refresh(t.Context()), "a later complete candidate recovers")
		})
	}
}

func TestObserverRegistryScopeAndFinalizedReads(t *testing.T) {
	f, o, now := newTestObserver(t)
	require.NoError(t, o.refresh(t.Context()))
	s, err := o.Snapshot(t.Context())
	require.NoError(t, err)
	other, err := NewContracts(o.chain.Contracts.RPC, common.HexToAddress("0xc002"))
	require.NoError(t, err)
	f.Reset()
	_, err = (PaymentChain{Contracts: other}).IsActiveAt(t.Context(), common.HexToAddress("0x3000"), s)
	require.ErrorIs(t, err, ErrObservationUnavailable)
	require.Zero(t, f.Stats().RoundTrips)
	advanceObserver(f, *now)
	f.Update(func(state *test.ChainState) {
		state.Contracts["TicketBroker"] = common.HexToAddress("0xb3")
		state.Results = map[test.ChainCall]string{{To: s.addresses.broker, Function: "TicketBroker.usedTickets"}: "0x" + word(1)}
	})
	chain := o.chain
	chain.Observer = o // Its retained registry is deliberately still the old one.
	used, _, err := chain.TicketUse(t.Context(), common.HexToHash("0x9999"), "finalized")
	require.NoError(t, err)
	require.False(t, used, "finalized settlement resolves its own canonical registry")
	require.Equal(t, 1, f.Stats().Calls["Controller.getContract"])
}
