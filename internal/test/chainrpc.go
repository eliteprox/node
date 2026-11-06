package test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

// Adapted from BlueClawNetwork/livepeer-node internal/test/chainrpc.go,
// 285eda984d7cce4e3030ef0510d63ae142534dd7, by Mike Zupper (MIT; see LICENSE).
// ChainRPC serves payment reads at canonical blocks. Other tests
// can extend this fixture without duplicating the payment RPC protocol.
type ChainRPC struct {
	URL        string
	Controller common.Address
	mu         sync.Mutex
	stats      ChainRPCStats
	state      ChainState
	history    map[common.Hash]ChainState
}

type ChainState struct {
	Number, L1Block, Timestamp uint64
	ChainID                    uint64
	Hash, Parent, RoundHash    common.Hash
	Round, CurrentRound        uint64
	Recipient                  common.Address
	Activation, Deactivation   uint64
	OmitL1, OmitTimestamp      bool
	Fail                       string
	Before                     func(context.Context, string)
	Contracts                  map[string]common.Address
	Results                    map[ChainCall]string
}

type ChainCall struct {
	To       common.Address
	Function string
}

type ChainRPCStats struct {
	RoundTrips int
	Calls      map[string]int
}

func NewChainRPC(t testing.TB) *ChainRPC {
	t.Helper()
	c := &ChainRPC{Controller: common.HexToAddress("0xc001"), stats: ChainRPCStats{Calls: map[string]int{}}}
	c.state = ChainState{Number: 0x100000, L1Block: 50, Timestamp: uint64(time.Now().Unix()), Hash: common.HexToHash("0xabcd"), RoundHash: common.HexToHash("0x1234"), Round: 5, CurrentRound: 5, Deactivation: ^uint64(0)}
	word := func(value string) string { return strings.Repeat("0", 64-len(value)) + value }
	contractNames := map[string]string{}
	c.state.Contracts = map[string]common.Address{}
	for name, address := range map[string]string{"RoundsManager": "a1", "BondingManager": "a2", "TicketBroker": "a3"} {
		contractNames[hex.EncodeToString(crypto.Keccak256([]byte(name)))] = name
		c.state.Contracts[name] = common.HexToAddress(address)
	}
	c.history = map[common.Hash]ChainState{c.state.Hash: c.state}
	methods := map[string]string{}
	for signature, name := range map[string]string{
		"getContract(bytes32)": "Controller.getContract", "lastInitializedRound()": "RoundsManager.lastInitializedRound",
		"currentRound()":             "RoundsManager.currentRound",
		"blockHashForRound(uint256)": "RoundsManager.blockHashForRound", "isActiveTranscoder(address)": "BondingManager.isActiveTranscoder",
		"getSenderInfo(address)": "TicketBroker.getSenderInfo", "claimableReserve(address,address)": "TicketBroker.claimableReserve",
		"usedTickets(bytes32)": "TicketBroker.usedTickets",
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
		var call struct {
			Input string
			To    common.Address
		}
		if name == "eth_call" {
			require.Len(t, request.Params, 2)
			require.NoError(t, json.Unmarshal(request.Params[0], &call))
			require.GreaterOrEqual(t, len(call.Input), 10)
			name = methods[call.Input[2:10]]
		}
		c.mu.Lock()
		c.stats.RoundTrips++
		c.stats.Calls[name]++
		before := c.state.Before
		c.mu.Unlock()
		if before != nil {
			before(r.Context(), name)
		}
		state := c.State()
		fail := func() {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32000}})
		}
		if state.Fail == name {
			fail()
			return
		}
		var result any
		switch request.Method {
		case "eth_chainId":
			result = quantity(state.ChainID)
		case "eth_getBlockByNumber":
			var ref string
			require.NoError(t, json.Unmarshal(request.Params[0], &ref))
			if ref != "latest" && ref != "finalized" {
				var ok bool
				state, ok = c.stateAt(ref)
				if !ok {
					break
				}
			}
			header := map[string]any{"number": quantity(state.Number), "hash": state.Hash, "parentHash": state.Parent}
			if !state.OmitTimestamp {
				header["timestamp"] = quantity(state.Timestamp)
			}
			if !state.OmitL1 {
				header["l1BlockNumber"] = quantity(state.L1Block)
			}
			result = header
		case "eth_call":
			var ref map[string]any
			require.NoError(t, json.Unmarshal(request.Params[1], &ref))
			require.Equal(t, true, ref["requireCanonical"])
			var ok bool
			state, ok = c.stateAt(ref["blockHash"].(string))
			if !ok {
				fail()
				return
			}
			switch name {
			case "Controller.getContract":
				require.Equal(t, c.Controller, call.To)
				require.Contains(t, contractNames, call.Input[10:])
				result = "0x" + word(state.Contracts[contractNames[call.Input[10:]]].Hex()[2:])
			case "RoundsManager.lastInitializedRound":
				result = "0x" + word(quantity(state.Round)[2:])
			case "RoundsManager.currentRound":
				result = "0x" + word(quantity(state.CurrentRound)[2:])
			case "RoundsManager.blockHashForRound":
				result = state.RoundHash.Hex()
			case "BondingManager.isActiveTranscoder":
				if state.Recipient != (common.Address{}) {
					require.Equal(t, word(hex.EncodeToString(state.Recipient.Bytes())), call.Input[10:])
				}
				result = "0x" + word("0")
				if state.Activation <= state.CurrentRound && state.CurrentRound < state.Deactivation {
					result = "0x" + word("1")
				}
			case "TicketBroker.getSenderInfo":
				result = "0x" + word("de0b6b3a7640000") + word("0") + word("de0b6b3a7640000") + word("0")
			case "TicketBroker.claimableReserve":
				result = "0x" + word("de0b6b3a7640000")
			case "TicketBroker.usedTickets":
				result = "0x" + word("0")
			default:
				t.Errorf("unexpected payment contract call")
			}
			if override, ok := state.Results[ChainCall{call.To, name}]; ok {
				result = override
			}
		default:
			t.Errorf("unexpected payment RPC method %q", request.Method)
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}))
	}))
	t.Cleanup(server.Close)
	c.URL = server.URL
	return c
}

func (c *ChainRPC) State() ChainState {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.state
	s.Contracts, s.Results = maps.Clone(s.Contracts), maps.Clone(s.Results)
	return s
}

func (c *ChainRPC) Update(update func(*ChainState)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Contracts = maps.Clone(c.state.Contracts)
	c.state.Results = maps.Clone(c.state.Results)
	update(&c.state)
	for h, state := range c.history {
		if state.Number >= c.state.Number {
			delete(c.history, h)
		}
	}
	c.history[c.state.Hash] = c.state
}

func (c *ChainRPC) stateAt(ref string) (ChainState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, state := range c.history {
		if ref == state.Hash.Hex() || ref == quantity(state.Number) {
			return state, true
		}
	}
	return ChainState{}, false
}

func quantity(n uint64) string { return fmt.Sprintf("0x%x", n) }

func (c *ChainRPC) Stats() ChainRPCStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ChainRPCStats{RoundTrips: c.stats.RoundTrips, Calls: maps.Clone(c.stats.Calls)}
}

func (c *ChainRPC) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = ChainRPCStats{Calls: map[string]int{}}
}
