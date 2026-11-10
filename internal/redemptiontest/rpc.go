package redemptiontest

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/stretchr/testify/require"
)

// recoveryRPC models independent clients against one account's pending Pool.
// All mutable RPC state is synchronized so the same scenarios run under -race.
type RedemptionRPC struct {
	StaleNonceValue                         uint64
	PrepareError                            string
	Wallet                                  common.Address
	Registered, WalletActive                bool
	RegisteredAccount                       common.Address
	Validity, FinalizedRound                int64
	SnapshotError, ReceiptMissingStatus     bool
	Block, Round                            int64
	Inactive, ActiveError                   bool
	ActiveRecipient                         common.Address
	OnSend                                  func(*types.Transaction)
	Mu                                      sync.Mutex
	Tickets                                 []*pm.SignedTicket
	Sent                                    []*types.Transaction
	Pool                                    map[uint64]*types.Transaction
	Receipts                                map[common.Hash]*types.Receipt
	Used                                    map[common.Hash]bool
	Finalized                               bool
	ReceiptError, ReceiptMissing            bool
	StaleNonce                              bool
	FinalizedNonce                          uint64
	FinalizedNonceReads                     int
	FinalizedNonceError, FinalizedNonceNull bool
	RejectNext, LoseNextResponse            bool
	MineAfter                               int
	NonceReads                              int
	ContractCalls                           int
	RejectSend                              int
	NonceBarrier                            chan struct{}
	ReceiptHeader                           *types.Header
	Server                                  *httptest.Server
}

// CollateralSnapshot is the same canonical execution identity served by the fixture.
func (f *RedemptionRPC) CollateralSnapshot() eth.ChainSnapshot {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	header := &types.Header{Number: big.NewInt(f.Block), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}
	return eth.ChainSnapshot{Number: header.Number, BlockHash: header.Hash()}
}

func NewRedemptionRPC(t *testing.T) *RedemptionRPC {
	t.Helper()
	f := &RedemptionRPC{Pool: map[uint64]*types.Transaction{}, Receipts: map[common.Hash]*types.Receipt{}, Used: map[common.Hash]bool{}, Finalized: true, Block: 100, Round: 1, Validity: 3, FinalizedRound: 1, ReceiptHeader: &types.Header{Number: big.NewInt(10), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.Serve))
	t.Cleanup(f.Server.Close)
	return f
}
func (f *RedemptionRPC) Serve(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ID     json.RawMessage
		Method string
		Params []json.RawMessage
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	f.Mu.Lock()
	var result any = "0x1"
	var failure string
	var barrier chan struct{}
	switch request.Method {
	case "eth_call":
		f.ContractCalls++
		var call struct{ Input string }
		_ = json.Unmarshal(request.Params[0], &call)
		selector := func(signature string) bool {
			return strings.HasPrefix(call.Input, fmt.Sprintf("0x%x", crypto.Keccak256([]byte(signature))[:4]))
		}
		result = fmt.Sprintf("0x%064x", 1)
		if selector("lastInitializedRound()") || selector("currentRound()") {
			round := f.Round
			if !f.Finalized && strings.Contains(string(request.Params[1]), (&types.Header{Number: big.NewInt(9), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}).Hash().Hex()) {
				round = f.FinalizedRound
			}
			result = fmt.Sprintf("0x%064x", round)
		}
		if selector("ticketValidityPeriod()") {
			result = fmt.Sprintf("0x%064x", f.Validity)
		}
		if selector("isActiveTranscoder(address)") {
			f.ActiveRecipient = common.HexToAddress(call.Input[len(call.Input)-40:])
			if f.ActiveError {
				failure = "active lookup unavailable"
			}
			if f.Inactive || (f.ActiveRecipient == f.Wallet && !f.WalletActive) {
				result = fmt.Sprintf("0x%064x", 0)
			}
		}
		if selector("isRegisteredTranscoder(address)") {
			f.RegisteredAccount = common.HexToAddress(call.Input[len(call.Input)-40:])
			result = fmt.Sprintf("0x%064x", 0)
			if f.Registered {
				result = fmt.Sprintf("0x%064x", 1)
			}
		}
		if selector("usedTickets(bytes32)") {
			hash := common.HexToHash("0x" + call.Input[10:])
			Used := f.Used[hash]
			// Finalized reads use an EIP-1898 canonical hash, not the moving latest tag.
			if !f.Finalized && strings.Contains(string(request.Params[1]), (&types.Header{Number: big.NewInt(9), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}).Hash().Hex()) {
				Used = false
			}
			result = fmt.Sprintf("0x%064x", 0)
			if Used {
				result = fmt.Sprintf("0x%064x", 1)
			}
		}
	case "eth_chainId":
		result = "0x1"
	case "eth_getBalance":
		result = "0x100000000000000000"
	case "eth_getBlockByNumber":
		var Block string
		_ = json.Unmarshal(request.Params[0], &Block)
		if Block == "latest" && f.SnapshotError {
			failure = "snapshot unavailable"
		}
		header := &types.Header{Number: big.NewInt(f.Block), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}
		if Block == "0xa" {
			header = f.ReceiptHeader
		}
		if Block == "finalized" && !f.Finalized {
			header = &types.Header{Number: big.NewInt(9), Difficulty: new(big.Int), BaseFee: big.NewInt(100)}
		}
		result = header
	case "eth_estimateGas":
		failure = f.PrepareError
		result = "0x5208"
	case "eth_maxPriorityFeePerGas":
		result = "0x3"
	case "eth_getTransactionCount":
		nonce := f.FinalizedNonce
		if string(request.Params[1]) != `"pending"` {
			f.FinalizedNonceReads++
			result = fmt.Sprintf("0x%x", nonce)
			if f.FinalizedNonceError {
				failure = "Finalized nonce lookup unavailable"
			}
			if f.FinalizedNonceNull {
				result = nil
			}
			break
		}
		for f.Pool[nonce] != nil {
			nonce++
		}
		if f.StaleNonce {
			nonce = f.StaleNonceValue
		}
		result = fmt.Sprintf("0x%x", nonce)
		f.NonceReads++
		if f.NonceBarrier != nil && f.NonceReads <= 2 {
			barrier = f.NonceBarrier
			if f.NonceReads == 2 {
				close(barrier)
			}
		}
	case "eth_sendRawTransaction":
		var encoded string
		_ = json.Unmarshal(request.Params[0], &encoded)
		raw, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
		if err != nil {
			failure = "bad raw transaction"
			break
		}
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(raw); err != nil {
			failure = "bad transaction"
			break
		}
		f.Sent = append(f.Sent, tx)
		if f.OnSend != nil {
			f.OnSend(tx)
		}
		if tx.Nonce() < f.FinalizedNonce {
			failure = "nonce too low"
			break
		}
		if f.RejectNext || len(f.Sent) == f.RejectSend {
			f.RejectNext = false
			failure = "insufficient funds"
			break
		}
		old := f.Pool[tx.Nonce()]
		if old != nil && old.Hash() != tx.Hash() && (tx.GasFeeCap().Cmp(old.GasFeeCap()) <= 0 || tx.GasTipCap().Cmp(old.GasTipCap()) <= 0) {
			failure = "replacement transaction underpriced"
			break
		}
		f.Pool[tx.Nonce()] = tx
		result = tx.Hash().Hex()
		if f.MineAfter > 0 && len(f.Sent) >= f.MineAfter {
			f.MineLocked(tx, true)
		}
		if f.LoseNextResponse {
			f.LoseNextResponse = false
			f.Mu.Unlock()
			http.Error(w, "lost response", http.StatusServiceUnavailable)
			return
		}
	case "eth_getTransactionReceipt":
		var hash common.Hash
		_ = json.Unmarshal(request.Params[0], &hash)
		result = f.Receipts[hash]
		if f.Receipts[hash] != nil && f.ReceiptMissingStatus {
			encoded, _ := json.Marshal(result)
			var fields map[string]any
			_ = json.Unmarshal(encoded, &fields)
			delete(fields, "status")
			result = fields
		}
		if f.ReceiptMissing {
			result = nil
		}
		if f.ReceiptError {
			failure = "receipt lookup unavailable"
		}
	case "eth_getTransactionByHash":
		var hash common.Hash
		_ = json.Unmarshal(request.Params[0], &hash)
		result = nil
		for _, tx := range f.Sent {
			pending := f.Pool[tx.Nonce()]
			if tx.Hash() == hash && (f.Receipts[hash] != nil || (pending != nil && pending.Hash() == hash)) {
				encoded, _ := json.Marshal(tx)
				var fields map[string]any
				_ = json.Unmarshal(encoded, &fields)
				if receipt := f.Receipts[hash]; receipt != nil {
					fields["blockNumber"] = "0xa"
					fields["blockHash"] = f.ReceiptHeader.Hash()
				}
				result = fields
				break
			}
		}
	default:
		failure = "unexpected RPC " + request.Method
	}
	f.Mu.Unlock()
	if barrier != nil {
		select {
		case <-barrier:
		case <-r.Context().Done():
			return
		}
	}
	response := map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}
	if failure != "" {
		delete(response, "result")
		response["error"] = map[string]any{"code": -32000, "message": failure}
	}
	_ = json.NewEncoder(w).Encode(response)
}
func (f *RedemptionRPC) MineLocked(tx *types.Transaction, success bool) {
	status := uint64(0)
	if success {
		status = 1
	}
	f.Receipts[tx.Hash()] = &types.Receipt{TxHash: tx.Hash(), Status: status, BlockNumber: big.NewInt(10), BlockHash: f.ReceiptHeader.Hash(), Logs: []*types.Log{}}
	if success {
		for _, ticket := range f.Tickets {
			if strings.Contains(hex.EncodeToString(tx.Data()), hex.EncodeToString(ticket.RecipientRandHash[:])) {
				f.Used[ticket.Hash()] = true
			}
		}
	}
}
func (f *RedemptionRPC) Transactions() []*types.Transaction {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	return append([]*types.Transaction(nil), f.Sent...)
}
func (f *RedemptionRPC) Mine(tx *types.Transaction, success bool) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	f.MineLocked(tx, success)
}
func (f *RedemptionRPC) Chain(t *testing.T) eth.PaymentChain {
	t.Helper()
	rpc, err := eth.OpenRPC(f.Server.URL)
	require.NoError(t, err)
	t.Cleanup(rpc.Close)
	c, err := eth.NewContracts(rpc, common.HexToAddress("0x1000"))
	require.NoError(t, err)
	return eth.PaymentChain{Contracts: c}
}
func RedemptionKey(t *testing.T) *eth.Key {
	t.Helper()
	path, password := test.WriteFixedKeystore(t)
	key, err := eth.OpenKeystoreFile(path, password)
	require.NoError(t, err)
	return key
}
