package discovery

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

func discoverySchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile("../docs/discovery.schema.json")
	require.NoError(t, err)
	return schema
}

func validateDiscoveryJSON(t *testing.T, schema *jsonschema.Schema, data []byte) error {
	t.Helper()
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&value))
	require.ErrorIs(t, decoder.Decode(&value), io.EOF)
	return schema.Validate(value)
}

func TestDiscoverySchema(t *testing.T) {
	schema := discoverySchema(t)
	const runner = `{"url":"https://orch.example/app","app":"example","observed_at":"2026-10-22T18:05:00.123+01:00"}`
	const metadata = `[{"nodes":[{"url":"https://orch.example","queried_at":"2026-10-22T18:05:00Z"}]}]`
	envelope := func(record string) string {
		return `[{"address":"https://orch.example/deployment","runners":[` + record + `]}]`
	}
	for _, test := range []struct {
		name, body string
		valid      bool
	}{
		{"empty", `[]`, true},
		{"empty runners", envelope(""), true},
		{"metadata", metadata, true},
		{"metadata with empty runners", `[{"nodes":[],"runners":[]}]`, true},
		{"minimal runner", envelope(runner), true},
		{"IPv6 with port", strings.ReplaceAll(envelope(runner), "orch.example", "[::1]:8935"), true},
		{"mixed-case schemes", strings.ReplaceAll(envelope(runner), "https://", "hTtPs://"), true},
		{"mixed-case node scheme", strings.ReplaceAll(metadata, "https://", "HtTp://"), true},
		{"runner URL components", envelope(strings.ReplaceAll(runner, "https://orch.example/app", "https://user:pass@orch.example/app?x=1#part")), true},
		{"unknown properties", envelope(strings.TrimSuffix(runner, "}") + `,"extension":{"count":9007199254740993}}`), true},
		{"extensible price", envelope(strings.TrimSuffix(runner, "}") + `,"mode":"future","price_info":{"price":123456789012345678901234567890,"price_usd":0.123456789012345678901234567890123456,"currency":"credits","unit":"tokens","extension":true}}`), true},
		{"USD price omitted", envelope(strings.TrimSuffix(runner, "}") + `,"price_info":{"price":1,"currency":"wei","unit":"fixed"}}`), true},
		{"price string", envelope(strings.TrimSuffix(runner, "}") + `,"price_info":{"price":"1"}}`), false},
		{"USD price string", envelope(strings.TrimSuffix(runner, "}") + `,"price_info":{"price":1,"price_usd":"0.1"}}`), false},
		{"missing URL", envelope(strings.ReplaceAll(runner, `"url":"https://orch.example/app",`, "")), false},
		{"missing app", envelope(strings.ReplaceAll(runner, `"app":"example",`, "")), false},
		{"missing observation", envelope(`{"url":"https://orch.example/app","app":"example"}`), false},
		{"empty app", envelope(strings.ReplaceAll(runner, `"app":"example"`, `"app":""`)), false},
		{"invalid observation", envelope(strings.ReplaceAll(runner, "2026-10-22T18:05:00.123+01:00", "yesterday")), false},
		{"invalid query time", `[{"nodes":[{"url":"https://orch.example","queried_at":"yesterday"}]}]`, false},
		{"missing query time", `[{"nodes":[{"url":"https://orch.example"}]}]`, false},
		{"missing node URL", `[{"nodes":[{"queried_at":"2026-10-22T18:05:00Z"}]}]`, false},
		{"metadata with address", `[{"nodes":[],"address":"https://orch.example"}]`, false},
		{"metadata with runners", `[{"nodes":[],"runners":[` + runner + `]}]`, false},
		{"metadata with non-array runners", `[{"nodes":[],"runners":{}}]`, false},
		{"missing runners", `[{"address":"https://orch.example"}]`, false},
		{"non-HTTP address", strings.ReplaceAll(envelope(runner), "https://orch.example/deployment", "ftp://orch.example"), false},
		{"non-HTTP runner URL", envelope(strings.ReplaceAll(runner, "https://orch.example/app", "ftp://orch.example/app")), false},
		{"address credentials", strings.ReplaceAll(envelope(runner), "https://orch.example/deployment", "https://user:pass@orch.example/deployment"), false},
		{"address query", strings.ReplaceAll(envelope(runner), "https://orch.example/deployment", "https://orch.example/deployment?x=1"), false},
		{"address empty query", strings.ReplaceAll(envelope(runner), "https://orch.example/deployment", "https://orch.example/deployment?"), false},
		{"address fragment", strings.ReplaceAll(envelope(runner), "https://orch.example/deployment", "https://orch.example/deployment#part"), false},
		{"node credentials", strings.ReplaceAll(metadata, "https://orch.example", "https://user:pass@orch.example"), false},
		{"node query", strings.ReplaceAll(metadata, "https://orch.example", "https://orch.example?x=1"), false},
		{"node fragment", strings.ReplaceAll(metadata, "https://orch.example", "https://orch.example#part"), false},
		{"hostless address", strings.ReplaceAll(envelope(runner), "https://orch.example/deployment", "https://:8935/deployment"), false},
		{"hostless node", strings.ReplaceAll(metadata, "https://orch.example", "https://:8935"), false},
		{"userinfo-only runner", envelope(strings.ReplaceAll(runner, "https://orch.example/app", "https://user:pass@/app")), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateDiscoveryJSON(t, schema, []byte(test.body))
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestDiscoverySchemaHTTPExample(t *testing.T) {
	doc, err := os.ReadFile("../docs/http-api.md")
	require.NoError(t, err)
	_, section, ok := strings.Cut(string(doc), "## Discovery HTTP\n")
	require.True(t, ok, "Discovery HTTP section must exist")
	section, _, _ = strings.Cut(section, "\n## ")
	_, block, ok := strings.Cut(section, "```json\n")
	require.True(t, ok, "Discovery HTTP must contain a JSON example")
	example, _, ok := strings.Cut(block, "\n```")
	require.True(t, ok, "JSON example must have a closing fence")
	require.NoError(t, validateDiscoveryJSON(t, discoverySchema(t), []byte(example)))
}
