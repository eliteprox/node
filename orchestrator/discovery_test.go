package orchestrator

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

func TestDiscoveryLocalInventory(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile("../docs/discovery.schema.json")
	require.NoError(t, err)
	for _, inventory := range []string{"empty", "offchain", "onchain"} {
		t.Run(inventory, func(t *testing.T) {
			registry := NewRegistry("bootstrap", "https://orch.example/deployment", time.Second, time.Minute)
			if inventory == "onchain" {
				registry.SetWeiPerUSD(big.NewRat(3600, 1))
			}
			if inventory != "empty" {
				require.NoError(t, registry.AddStatic(StaticRunner{
					ID: "local", RunnerURL: "http://127.0.0.1:9000", App: "local", Mode: "single-shot", Capacity: 2,
					Version: "v1", Metadata: "model-x", GPU: &runnerGPU{ID: "0", Name: "H100", VRAMMB: 80000},
					PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "hour"},
				}))
			}
			policy, err := destination.New("test", nil)
			require.NoError(t, err)
			s := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
			t.Cleanup(s.Close)
			server := httptest.NewServer(s)
			t.Cleanup(server.Close)
			response, err := server.Client().Get(server.URL + "/deployment/discovery")
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, "application/json", response.Header.Get("Content-Type"))
			data, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			var value any
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&value))
			require.ErrorIs(t, decoder.Decode(&value), io.EOF)
			require.NoError(t, schema.Validate(value))
			require.NotContains(t, string(data), `"nodes"`)
			require.NotContains(t, string(data), `"app_id"`)
			require.NotContains(t, string(data), `"runner_id"`)
			var entries []discoveryEntry
			require.NoError(t, json.Unmarshal(data, &entries))
			require.Len(t, entries, 1)
			require.Equal(t, registry.service, entries[0].Address)
			if inventory == "empty" {
				require.NotNil(t, entries[0].Runners)
				require.Empty(t, entries[0].Runners)
				return
			}
			require.Len(t, entries[0].Runners, 1)
			runner := entries[0].Runners[0]
			require.Equal(t, "https://orch.example/deployment/apps/local/app", runner.URL)
			require.Equal(t, "local", runner.App)
			require.Equal(t, "v1", runner.Version)
			require.Equal(t, "model-x", runner.Metadata)
			require.Equal(t, &runnerGPU{ID: "0", Name: "H100", VRAMMB: 80000}, runner.GPU)
			require.Equal(t, "single-shot", runner.Mode)
			require.Equal(t, 2, runner.Capacity)
			require.Zero(t, runner.CapacityUsed)
			require.Equal(t, 2, runner.CapacityAvailable)
			if inventory == "onchain" {
				require.Equal(t, priceInfo{Price: "1", PriceUSD: "0.000277777777777777777777777777777778", Currency: "wei", Unit: "seconds"}, runner.PriceInfo)
			} else {
				require.NotContains(t, string(data), `"price_info"`)
			}
		})
	}
}
