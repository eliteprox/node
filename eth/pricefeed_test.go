package eth

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth/contracts/chainlink"
	"github.com/stretchr/testify/require"
)

func TestPriceFeedConversionAndRejection(t *testing.T) {
	for _, scenario := range []string{"valid", "stale", "future", "negative", "incomplete", "wrong-pair"} {
		t.Run(scenario, func(t *testing.T) {
			contract, err := chainlink.AggregatorV3InterfaceMetaData.GetAbi()
			require.NoError(t, err)
			rpc := testRPC(t, func(rpcMethod string, params []json.RawMessage) any {
				if rpcMethod == "eth_getBlockByNumber" {
					return map[string]string{"hash": ethcommon.HexToHash("0x1234").Hex()}
				}
				var call map[string]string
				require.NoError(t, json.Unmarshal(params[0], &call))
				data, err := hex.DecodeString(strings.TrimPrefix(call["input"], "0x"))
				require.NoError(t, err)
				method, err := contract.MethodById(data[:4])
				require.NoError(t, err)
				var args []any
				switch method.Name {
				case "description":
					pair := "ETH / USD"
					if scenario == "wrong-pair" {
						pair = "BTC / USD"
					}
					args = []any{pair}
				case "decimals":
					args = []any{uint8(8)}
				case "latestRoundData":
					answer, updated, answered := big.NewInt(2000_00000000), time.Now().Add(-time.Minute).Unix(), big.NewInt(10)
					switch scenario {
					case "stale":
						updated = time.Now().Add(-3 * time.Hour).Unix()
					case "future":
						updated = time.Now().Add(time.Hour).Unix()
					case "negative":
						answer.Neg(answer)
					case "incomplete":
						answered = big.NewInt(9)
					}
					args = []any{big.NewInt(10), answer, big.NewInt(updated), big.NewInt(updated), answered}
				}
				packed, err := method.Outputs.Pack(args...)
				require.NoError(t, err)
				return "0x" + hex.EncodeToString(packed)
			})
			contracts := &Contracts{RPC: rpc}
			rate, until, err := contracts.WeiPerUSD(t.Context(), ethcommon.HexToAddress("0x2000"), 2*time.Hour)
			if scenario != "valid" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, big.NewRat(500_000_000_000_000, 1), rate)
			require.True(t, until.After(time.Now()))
		})
	}
}
