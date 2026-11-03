package test

import (
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

// Adapted from BlueClawNetwork/livepeer-node internal/test/chainrpc.go,
// 285eda984d7cce4e3030ef0510d63ae142534dd7, by Mike Zupper (MIT; see LICENSE).
// ChainRPC serves the payment read methods at one canonical block. Other tests
// can extend this fixture without duplicating the payment RPC protocol.
type ChainRPC struct {
	URL        string
	Controller common.Address
	mu         sync.Mutex
	stats      ChainRPCStats
}

type ChainRPCStats struct {
	RoundTrips int
	Calls      map[string]int
}

func NewChainRPC(t testing.TB) *ChainRPC {
	t.Helper()
	c := &ChainRPC{Controller: common.HexToAddress("0xc001"), stats: ChainRPCStats{Calls: map[string]int{}}}
	blockHash := common.HexToHash("0xabcd").Hex()
	word := func(value string) string { return strings.Repeat("0", 64-len(value)) + value }
	addresses := map[string]string{}
	for name, address := range map[string]string{"RoundsManager": "a1", "BondingManager": "a2", "TicketBroker": "a3"} {
		addresses[hex.EncodeToString(crypto.Keccak256([]byte(name)))] = word(address)
	}
	methods := map[string]string{}
	for signature, name := range map[string]string{
		"getContract(bytes32)": "Controller.getContract", "lastInitializedRound()": "RoundsManager.lastInitializedRound",
		"blockHashForRound(uint256)": "RoundsManager.blockHashForRound", "isActiveTranscoder(address)": "BondingManager.isActiveTranscoder",
		"getSenderInfo(address)": "TicketBroker.getSenderInfo", "claimableReserve(address,address)": "TicketBroker.claimableReserve",
	} {
		methods[hex.EncodeToString(crypto.Keccak256([]byte(signature))[:4])] = name
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage
			Method string
			Params []json.RawMessage
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		name := request.Method
		var result any
		switch request.Method {
		case "eth_getBlockByNumber":
			result = map[string]string{"number": "0x100000", "l1BlockNumber": "0x32", "hash": blockHash}
		case "eth_call":
			require.Len(t, request.Params, 2)
			var ref map[string]any
			require.NoError(t, json.Unmarshal(request.Params[1], &ref))
			require.Equal(t, map[string]any{"blockHash": blockHash, "requireCanonical": true}, ref)
			var call struct{ Input string }
			require.NoError(t, json.Unmarshal(request.Params[0], &call))
			require.GreaterOrEqual(t, len(call.Input), 10)
			name = methods[call.Input[2:10]]
			switch name {
			case "Controller.getContract":
				require.Contains(t, addresses, call.Input[10:])
				result = "0x" + addresses[call.Input[10:]]
			case "RoundsManager.lastInitializedRound":
				result = "0x" + word("5")
			case "RoundsManager.blockHashForRound":
				result = common.HexToHash("0x1234").Hex()
			case "BondingManager.isActiveTranscoder":
				result = "0x" + word("1")
			case "TicketBroker.getSenderInfo":
				result = "0x" + word("de0b6b3a7640000") + word("0") + word("de0b6b3a7640000") + word("0")
			case "TicketBroker.claimableReserve":
				result = "0x" + word("de0b6b3a7640000")
			default:
				t.Errorf("unexpected payment contract call")
			}
		default:
			t.Errorf("unexpected payment RPC method %q", request.Method)
		}
		c.mu.Lock()
		c.stats.RoundTrips++
		c.stats.Calls[name]++
		c.mu.Unlock()
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}))
	}))
	t.Cleanup(server.Close)
	c.URL = server.URL
	return c
}

func (c *ChainRPC) Stats() ChainRPCStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ChainRPCStats{RoundTrips: c.stats.RoundTrips, Calls: maps.Clone(c.stats.Calls)}
}
