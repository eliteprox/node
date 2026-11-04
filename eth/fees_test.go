package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestFeeOptionsAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name               string
		missingBaseFee     bool
		priorityError      int
		price              string
		tip                *big.Int
		ceiling            int64
		gas                uint64
		expectedTip, error string
	}{
		{name: "suggestion", expectedTip: "3"},
		{name: "missing base fee", missingBaseFee: true, error: "missing base fee"},
		{name: "unsupported priority", priorityError: -32601, price: "0x69", expectedTip: "5"},
		{name: "unsupported provider method", priorityError: -32004, price: "0x69", expectedTip: "5"},
		{name: "negative fallback", priorityError: -32601, price: "0x63", error: "invalid suggested"},
		{name: "provider failure", priorityError: -32000, error: "RPC error -32000"},
		{name: "explicit tip bypasses suggestion", priorityError: -32000, tip: big.NewInt(9), gas: 30000, expectedTip: "9"},
		{name: "ceiling fails", ceiling: 202, error: "exceeds maximum"},
		{name: "ceiling equality", ceiling: 203, expectedTip: "3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var estimates, suggestions int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ID     json.RawMessage
					Method string
					Params []json.RawMessage
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				var result any
				switch request.Method {
				case "eth_call":
					result = "0x"
				case "eth_estimateGas":
					estimates++
					result = "0x5208"
				case "eth_getBlockByNumber":
					header := &types.Header{Number: big.NewInt(1), Difficulty: new(big.Int)}
					if !tc.missingBaseFee {
						header.BaseFee = big.NewInt(100)
					}
					result = header
				case "eth_maxPriorityFeePerGas":
					require.False(t, tc.missingBaseFee)
					suggestions++
					if tc.priorityError != 0 {
						require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": tc.priorityError, "message": "method unavailable"}}))
						return
					}
					result = "0x3"
				case "eth_gasPrice":
					result = tc.price
				default:
					t.Fatalf("planning must not reserve a nonce or send; unexpected %s", request.Method)
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}))
			}))
			defer server.Close()
			rpc, err := OpenRPC(server.URL)
			require.NoError(t, err)
			defer rpc.Close()
			c, err := NewContracts(rpc, common.HexToAddress("0x100"))
			require.NoError(t, err)
			if tc.ceiling != 0 {
				c.MaxFeePerGas = big.NewInt(tc.ceiling)
			}
			plan, err := c.PlanTransactionWithOptions(t.Context(), common.HexToAddress("0x200"), common.HexToAddress("0x300"), []byte{1}, big.NewInt(0), FeeOptions{GasLimit: tc.gas, PriorityFee: tc.tip})
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedTip, plan.TipCapWei)
			tip, ok := new(big.Int).SetString(tc.expectedTip, 10)
			require.True(t, ok)
			require.Equal(t, new(big.Int).Add(big.NewInt(200), tip).String(), plan.FeeCapWei)
			if tc.gas == 0 {
				require.Equal(t, 1, estimates)
				require.EqualValues(t, 21000, plan.GasLimit)
			} else {
				require.Zero(t, estimates)
				require.Equal(t, tc.gas, plan.GasLimit)
			}
			if tc.tip != nil {
				require.Zero(t, suggestions)
			}
		})
	}
}

func TestCLIReplacementConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name                                                                             string
		limit                                                                            uint64
		fresh, originalWins, replacementWins, cancel, malformed, revert, providerTimeout bool
		tip                                                                              *big.Int
		ceiling                                                                          int64
		error                                                                            string
		attempts                                                                         int
	}{
		{name: "default limit", error: "timed out", attempts: 1},
		{name: "exhaustion waits after final", limit: 2, error: "3 attempt(s)", attempts: 3},
		{name: "provider timeout", limit: 2, providerTimeout: true, error: "deadline exceeded", attempts: 1},
		{name: "replacement inclusion", limit: 2, replacementWins: true, attempts: 2},
		{name: "explicit tip is bumped", limit: 1, replacementWins: true, tip: big.NewInt(20), attempts: 2},
		{name: "original inclusion", limit: 2, originalWins: true, attempts: 2},
		{name: "fresh suggestions", limit: 1, fresh: true, originalWins: true, attempts: 2},
		{name: "ceiling", limit: 1, ceiling: 203, error: "exceeds maximum", attempts: 1},
		{name: "cancellation", limit: 2, cancel: true, error: "canceled", attempts: 1},
		{name: "malformed receipt", limit: 2, malformed: true, error: "invalid transaction receipt", attempts: 1},
		{name: "reverted receipt", limit: 2, revert: true, error: "reverted", attempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				keyFile, passwordFile := test.WriteFixedKeystore(t)
				key, err := OpenKeystoreFile(keyFile, passwordFile)
				require.NoError(t, err)
				var mu sync.Mutex
				var sent []*types.Transaction
				nonceReads := 0
				rpc := testRPC(t, func(method string, params []json.RawMessage) any {
					mu.Lock()
					defer mu.Unlock()
					switch method {
					case "eth_call":
						return "0x"
					case "eth_estimateGas":
						return "0x5208"
					case "eth_getTransactionCount":
						nonceReads++
						return "0x7"
					case "eth_getBlockByNumber":
						base := int64(100)
						if tc.fresh && len(sent) > 0 {
							base = 200
						}
						return &types.Header{Number: big.NewInt(1), Difficulty: new(big.Int), BaseFee: big.NewInt(base)}
					case "eth_maxPriorityFeePerGas":
						if tc.fresh && len(sent) > 0 {
							return "0x9"
						}
						return "0x3"
					case "eth_sendRawTransaction":
						var data hexutil.Bytes
						require.NoError(t, json.Unmarshal(params[0], &data))
						tx := new(types.Transaction)
						require.NoError(t, tx.UnmarshalBinary(data))
						sent = append(sent, tx)
						return tx.Hash().Hex()
					case "eth_getTransactionReceipt":
						var hash common.Hash
						require.NoError(t, json.Unmarshal(params[0], &hash))
						if tc.malformed {
							return map[string]any{"transactionHash": hash, "blockNumber": "0x10", "blockHash": common.Hash{31: 1}, "logs": []any{}, "cumulativeGasUsed": "0x0", "gasUsed": "0x0", "logsBloom": types.Bloom{}}
						}
						if tc.revert || (tc.originalWins && len(sent) > 1 && hash == sent[0].Hash()) || (tc.replacementWins && len(sent) > 1 && hash == sent[len(sent)-1].Hash()) {
							status := uint64(1)
							if tc.revert {
								status = 0
							}
							return &types.Receipt{TxHash: hash, Status: status, BlockNumber: big.NewInt(16), BlockHash: common.Hash{31: 1}, Logs: []*types.Log{}}
						}
						return nil
					default:
						t.Fatalf("unexpected %s", method)
						return nil
					}
				})
				c, err := NewContracts(rpc, common.HexToAddress("0x100"))
				require.NoError(t, err)
				if tc.ceiling != 0 {
					c.MaxFeePerGas = big.NewInt(tc.ceiling)
				}
				plan, err := c.PlanTransactionWithOptions(t.Context(), key.Address(), common.HexToAddress("0x200"), []byte{1, 2, 3}, big.NewInt(5), FeeOptions{PriorityFee: tc.tip})
				require.NoError(t, err)
				signed, err := c.Prepare(t.Context(), plan, key, big.NewInt(42161))
				require.NoError(t, err)
				require.NoError(t, c.Broadcast(t.Context(), signed))
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tc.cancel {
					cancel()
				}
				if tc.providerTimeout {
					rpc.http.Transport = receiptDeadlineTransport{rpc.http.Transport}
				}
				var reported []common.Hash
				start := time.Now()
				result, err := c.WaitConfirmation(ctx, signed, key, WaitOptions{Timeout: 50 * time.Millisecond, MaxReplacements: tc.limit}, func(tx SignedTransaction, p TransactionPlan) error {
					reported = append(reported, tx.Hash)
					require.Equal(t, signed.Nonce, tx.Nonce)
					require.Equal(t, plan.GasLimit, p.GasLimit)
					return nil
				})
				if tc.error != "" {
					require.ErrorContains(t, err, tc.error)
				} else {
					require.NoError(t, err)
					winner := signed.Hash
					if tc.replacementWins {
						winner = result.Attempts[len(result.Attempts)-1]
					}
					require.Equal(t, winner, result.Hash)
					require.EqualValues(t, 16, result.Block)
				}
				require.Len(t, result.Attempts, tc.attempts)
				require.Len(t, reported, tc.attempts-1)
				if tc.name == "exhaustion waits after final" {
					require.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
				}
				mu.Lock()
				defer mu.Unlock()
				require.Len(t, sent, tc.attempts)
				require.Equal(t, 1, nonceReads, "replacements must not reserve another nonce")
				for i, tx := range sent {
					require.Equal(t, result.Attempts[i], tx.Hash())
					require.EqualValues(t, 7, tx.Nonce())
					require.Equal(t, sent[0].To(), tx.To())
					require.Equal(t, sent[0].Value(), tx.Value())
					require.Equal(t, sent[0].Data(), tx.Data())
					require.Equal(t, sent[0].Gas(), tx.Gas())
					if i > 0 {
						require.GreaterOrEqual(t, tx.GasTipCap().Cmp(bumpFee(sent[i-1].GasTipCap())), 0)
						require.GreaterOrEqual(t, tx.GasFeeCap().Cmp(bumpFee(sent[i-1].GasFeeCap())), 0)
					}
				}
				if tc.fresh {
					require.Equal(t, "9", sent[1].GasTipCap().String())
					require.Equal(t, "409", sent[1].GasFeeCap().String())
				} else if tc.tip != nil {
					require.Equal(t, "23", sent[1].GasTipCap().String())
					require.Equal(t, "245", sent[1].GasFeeCap().String())
				} else if len(sent) > 1 {
					require.Equal(t, "4", sent[1].GasTipCap().String())
					require.Equal(t, "226", sent[1].GasFeeCap().String())
				}
			})
		})
	}
}
func TestFeeRoundingAndIdentityValidation(t *testing.T) {
	for _, tc := range []struct{ value, want int64 }{{0, 1}, {1, 2}, {100, 111}, {101, 113}} {
		require.Equal(t, big.NewInt(tc.want), bumpFee(big.NewInt(tc.value)))
	}
	c := &Contracts{}
	_, _, err := c.replacement(t.Context(), SignedTransaction{}, nil)
	require.ErrorContains(t, err, "identity")
	result, err := c.WaitConfirmation(t.Context(), SignedTransaction{}, nil, WaitOptions{}, nil)
	require.Error(t, err)
	require.Len(t, result.Attempts, 1)
	require.False(t, errors.Is(err, context.Canceled))
}

// An HTTP/provider timeout can expire while the confirmation context is live.
type receiptDeadlineTransport struct{ http.RoundTripper }

func (tr receiptDeadlineTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var request struct{ Method string }
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	if request.Method == "eth_getTransactionReceipt" {
		return nil, context.DeadlineExceeded
	}
	return tr.RoundTripper.RoundTrip(r)
}
