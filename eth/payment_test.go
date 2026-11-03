package eth

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

func TestCanonicalPaymentSnapshotAndCollateral(t *testing.T) {
	for _, l2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "ethereum", true: "arbitrum"}[l2], func(t *testing.T) {
			blockHash := ethcommon.HexToHash("0xabcd").Hex()
			sender, recipient := ethcommon.HexToAddress("0x2000"), ethcommon.HexToAddress("0x3000")
			var reorg atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Method string
					Params []json.RawMessage
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				var result any
				switch request.Method {
				case "eth_getBlockByNumber":
					header := map[string]string{"number": "0x100000", "hash": blockHash}
					if l2 {
						header["l1BlockNumber"] = "0x32"
					}
					result = header
				case "eth_call":
					var ref map[string]any
					require.NoError(t, json.Unmarshal(request.Params[1], &ref))
					require.Equal(t, map[string]any{"blockHash": blockHash, "requireCanonical": true}, ref)
					if reorg.Load() {
						_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32000}})
						return
					}
					var call map[string]string
					require.NoError(t, json.Unmarshal(request.Params[0], &call))
					selector := func(sig string) bool {
						return strings.HasPrefix(call["input"], "0x"+hex.EncodeToString(crypto.Keccak256([]byte(sig))[:4]))
					}
					switch {
					case selector("getContract(bytes32)"):
						result = "0x" + hex.EncodeToString(ethcommon.LeftPadBytes([]byte{1}, 32))
					case selector("lastInitializedRound()"):
						result = "0x" + hex.EncodeToString(ethcommon.LeftPadBytes([]byte{5}, 32))
					case selector("blockHashForRound(uint256)"):
						require.True(t, strings.HasSuffix(call["input"], strings.Repeat("0", 63)+"5"))
						result = blockHash
					case selector("isActiveTranscoder(address)"):
						require.Equal(t, calldata("isActiveTranscoder(address)", addressWord(recipient)), call["input"])
						result = "0x" + word(1)
					case selector("getSenderInfo(address)"):
						require.Equal(t, calldata("getSenderInfo(address)", addressWord(sender)), call["input"])
						result = "0x" + word(10) + word(9) + word(50) + word(0)
					case selector("claimableReserve(address,address)"):
						require.Equal(t, calldata("claimableReserve(address,address)", addressWord(sender), addressWord(recipient)), call["input"])
						result = "0x" + word(7)
					default:
						t.Errorf("unexpected call %s", call["input"])
					}
				default:
					t.Errorf("unexpected RPC %s", request.Method)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
			}))
			defer server.Close()
			rpc, err := OpenRPC(server.URL)
			require.NoError(t, err)
			defer rpc.Close()
			contracts, err := OpenContracts(rpc, ethcommon.HexToAddress("0x1000").Hex())
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
			funds, err := chain.SenderInfo(t.Context(), sender, recipient)
			require.NoError(t, err)
			require.Equal(t, snapshot.Block, funds.Snapshot.Block)
			require.Equal(t, snapshot.Round, funds.Snapshot.Round)
			require.Equal(t, snapshot.RoundHash, funds.Snapshot.RoundHash)
			require.Equal(t, snapshot.blockReference, funds.Snapshot.blockReference)
			require.Equal(t, big.NewInt(10), funds.Deposit)
			require.Equal(t, big.NewInt(9), funds.WithdrawRound)
			require.Equal(t, big.NewInt(7), funds.Reserve, "acceptance uses this recipient's claimable reserve")
			readiness, err := chain.SenderInfo(t.Context(), sender, ethcommon.Address{})
			require.NoError(t, err)
			require.Equal(t, big.NewInt(50), readiness.Reserve, "readiness uses total remaining reserve")
			reorg.Store(true)
			_, err = chain.Snapshot(t.Context())
			require.Error(t, err, "a reorg must not produce mixed observations")
		})
	}
}

func TestRedemptionReceiptRequiresCanonicalFinality(t *testing.T) {
	for _, scenario := range []string{"pending", "unfinalized", "reorg", "confirmed", "reverted", "invalid-missing-status", "invalid-null-status", "invalid-status", "invalid-hash"} {
		t.Run(scenario, func(t *testing.T) {
			hash := ethcommon.HexToHash("0xabcd").Hex()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Method string
					Params []json.RawMessage
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				var result any
				if request.Method == "eth_getTransactionReceipt" {
					if scenario != "pending" {
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
						result = fields
					}
				} else {
					var block string
					require.NoError(t, json.Unmarshal(request.Params[0], &block))
					if block == "finalized" {
						number := "0x10"
						if scenario == "unfinalized" {
							number = "0xf"
						}
						result = map[string]string{"number": number}
					} else {
						canonical := hash
						if scenario == "reorg" {
							canonical = ethcommon.HexToHash("0xffff").Hex()
						}
						result = map[string]string{"hash": canonical}
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
			}))
			defer server.Close()
			rpc, err := OpenRPC(server.URL)
			require.NoError(t, err)
			defer rpc.Close()
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
