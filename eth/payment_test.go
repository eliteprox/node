package eth

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestRedemptionSnapshotRequiresArbitrumClock(t *testing.T) {
	fixture := test.NewChainRPC(t)
	fixture.Update(func(s *test.ChainState) { s.ChainID = 42161; s.OmitL1 = true })
	rpc, err := OpenRPC(fixture.URL)
	require.NoError(t, err)
	defer rpc.Close()
	require.NoError(t, rpc.CheckChainID(t.Context(), big.NewInt(42161)))
	contracts, err := NewContracts(rpc, fixture.Controller)
	require.NoError(t, err)
	chain := PaymentChain{Contracts: contracts}
	_, err = chain.RedemptionSnapshot(t.Context(), "latest")
	require.ErrorIs(t, err, ErrObservationUnavailable)
	fixture.Update(func(s *test.ChainState) { s.OmitL1 = false })
	snapshot, err := chain.RedemptionSnapshot(t.Context(), "latest")
	require.NoError(t, err)
	require.EqualValues(t, 50, snapshot.Block.Int64())
	require.EqualValues(t, 3, snapshot.TicketValidityPeriod.Int64())
	require.True(t, snapshot.ValidTicketRound(5))
	require.True(t, snapshot.TicketExpired(2))
	snapshot.TicketValidityPeriod = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	require.False(t, snapshot.ValidTicketRound(5), "Solidity uint256 expiry arithmetic must not overflow")
}

func TestCanonicalPaymentSnapshotAndCollateral(t *testing.T) {
	for _, l2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "ethereum", true: "arbitrum"}[l2], func(t *testing.T) {
			sender, recipient := ethcommon.HexToAddress("0x2000"), ethcommon.HexToAddress("0x3000")
			fixture := test.NewChainRPC(t)
			fixture.Update(func(s *test.ChainState) {
				s.OmitL1 = !l2
				s.Payer, s.Recipient = sender, recipient
				s.Deposit, s.Reserve, s.Claimable, s.WithdrawRound = 10, 50, 7, 9
			})
			blockHash := fixture.State().RoundHash.Hex()
			rpc, err := OpenRPC(fixture.URL)
			require.NoError(t, err)
			defer rpc.Close()
			contracts, err := NewContracts(rpc, fixture.Controller)
			require.NoError(t, err)
			chain := PaymentChain{Contracts: contracts}
			snapshot, err := chain.Snapshot(t.Context())
			require.NoError(t, err)
			expected := big.NewInt(0x100000)
			if l2 {
				expected = big.NewInt(50)
			}
			require.Equal(t, expected, snapshot.Block)
			require.Equal(t, big.NewInt(5), snapshot.Round)
			require.Equal(t, ethcommon.HexToHash(blockHash), snapshot.RoundHash)
			active, err := chain.IsActive(t.Context(), recipient)
			require.NoError(t, err)
			require.True(t, active)
			funds, err := chain.PayerFunds(t.Context(), sender, recipient)
			require.NoError(t, err)
			require.Equal(t, snapshot.Block, funds.Snapshot.Block)
			require.Equal(t, snapshot.Round, funds.Snapshot.Round)
			require.Equal(t, snapshot.RoundHash, funds.Snapshot.RoundHash)
			require.Equal(t, snapshot.blockReference, funds.Snapshot.blockReference)
			require.Equal(t, big.NewInt(10), funds.Deposit)
			require.Equal(t, big.NewInt(9), funds.WithdrawRound)
			require.Equal(t, big.NewInt(7), funds.Reserve, "acceptance uses this recipient's claimable reserve")
			readiness, err := chain.PayerFunds(t.Context(), sender, ethcommon.Address{})
			require.NoError(t, err)
			require.Equal(t, big.NewInt(50), readiness.Reserve, "readiness uses total remaining reserve")
			fixture.Update(func(s *test.ChainState) {
				s.Before = func(_ context.Context, name string) {
					if name == "Controller.getContract" {
						fixture.Update(func(s *test.ChainState) { s.Hash = ethcommon.HexToHash("0xdead") })
					}
				}
			})
			_, err = chain.Snapshot(t.Context())
			require.Error(t, err, "a reorg must not produce mixed observations")
		})
	}
}

func TestRedemptionReceiptRequiresCanonicalFinality(t *testing.T) {
	for _, scenario := range []string{"pending", "unfinalized", "reorg", "confirmed", "reverted", "invalid-missing-status", "invalid-null-status", "invalid-status", "invalid-hash"} {
		t.Run(scenario, func(t *testing.T) {
			hash := ethcommon.HexToHash("0xabcd").Hex()
			rpc := testRPC(t, func(method string, params []json.RawMessage) any {
				if method == "eth_getTransactionReceipt" {
					if scenario == "pending" {
						return nil
					}
					status := uint64(1)
					if scenario == "reverted" {
						status = 0
					}
					fields := map[string]any{"transactionHash": ethcommon.HexToHash("0x1234"), "status": hexutil.EncodeUint64(status), "blockNumber": "0x10", "blockHash": hash,
						"cumulativeGasUsed": "0x0", "gasUsed": "0x0", "logsBloom": types.Bloom{}, "logs": []any{}}
					switch scenario {
					case "invalid-missing-status":
						delete(fields, "status")
					case "invalid-null-status":
						fields["status"] = nil
					case "invalid-status":
						fields["status"] = "0x2"
					case "invalid-hash":
						fields["transactionHash"] = hash
					}
					return fields
				}
				var block string
				require.NoError(t, json.Unmarshal(params[0], &block))
				if block == "finalized" {
					number := "0x10"
					if scenario == "unfinalized" {
						number = "0xf"
					}
					return map[string]string{"number": number}
				}
				canonical := hash
				if scenario == "reorg" {
					canonical = ethcommon.HexToHash("0xffff").Hex()
				}
				return map[string]string{"hash": canonical}
			})
			contracts := &Contracts{RPC: rpc}
			receipt, err := (PaymentChain{Contracts: contracts}).FinalizedReceipt(t.Context(), ethcommon.HexToHash("0x1234"))
			if strings.HasPrefix(scenario, "invalid") {
				require.ErrorContains(t, err, "invalid transaction receipt")
				_, err = contracts.WaitReceipt(t.Context(), ethcommon.HexToHash("0x1234"))
				require.ErrorContains(t, err, "invalid transaction receipt")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, scenario == "confirmed", receipt != nil && receipt.Status == types.ReceiptStatusSuccessful)
			require.Equal(t, scenario == "reverted", receipt != nil && receipt.Status == types.ReceiptStatusFailed)
			if receipt != nil {
				require.Equal(t, big.NewInt(16), receipt.BlockNumber)
				require.Equal(t, hash, receipt.BlockHash.Hex())
			}
		})
	}
}
