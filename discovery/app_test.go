package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/stretchr/testify/require"
)

func freeAddress(t *testing.T) netip.AddrPort {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().(*net.TCPAddr).AddrPort()
	require.NoError(t, l.Close())
	return address
}

func TestServeWithoutRunnerOrPaymentDependencies(t *testing.T) {
	for _, source := range []string{"", "http://127.0.0.1:1"} {
		t.Run(source, func(t *testing.T) {
			listen, metrics := freeAddress(t), freeAddress(t)
			baseURL := "http://" + listen.String()
			p := Params{Listen: listen, MetricsListen: metrics,
				AdvertiseNodes: true, AdvertiseRunners: true, RefreshInterval: time.Minute, MaxFailedRefreshes: 6}
			if source != "" {
				p.Nodes = []boa.Text[*url.URL]{{Value: mustURL(t, source)}}
				p.Grants = []string{"127.0.0.1:1"}
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, p, io.Discard) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(12 * time.Second):
					t.Error("discovery did not shut down")
				}
			})
			client := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), Timeout: time.Second}
			defer client.CloseIdleConnections()
			require.Eventually(t, func() bool {
				response, err := client.Get("http://" + metrics.String() + "/readyz")
				if err != nil {
					return false
				}
				defer response.Body.Close()
				return response.StatusCode == 200
			}, 5*time.Second, 10*time.Millisecond)
			for path, want := range map[string]string{
				"http://" + metrics.String() + "/healthz": "ok\n",
				"http://" + metrics.String() + "/metrics": "livepeer_discovery_up 1\n",
				baseURL + "/discovery":                    "[]\n",
			} {
				response, err := client.Get(path)
				require.NoError(t, err)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				require.Equal(t, 200, response.StatusCode)
				if strings.HasSuffix(path, "/metrics") {
					require.Contains(t, string(body), want)
				} else {
					require.Equal(t, want, string(body))
				}
			}
		})
	}
}

func TestDiscoveryConfigDefaultsAndPrecedence(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	out, err := execute(t, "--config=", "--print-config")
	require.NoError(t, err)
	for _, value := range []string{`Listen = "127.0.0.1:8939"`, `MetricsListen = "127.0.0.1:8940"`, `AdvertiseNodes = true`, `AdvertiseRunners = true`, `MaxFailedRefreshes = 6`, `RefreshInterval = "5m0s"`} {
		require.Contains(t, out, value)
	}
	configDir := filepath.Join(os.Getenv("HOME"), ".lpData", "offchain", "discovery")
	require.NoError(t, os.MkdirAll(configDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte("RefreshInterval = '1m'\nAdvertiseNodes = false\nAdvertiseRunners = false\nMaxFailedRefreshes = 0\nNodes = ['https://child.example']\nGrants = ['127.0.0.1']\n"), 0600))
	out, err = execute(t, "--print-config")
	require.NoError(t, err)
	for _, value := range []string{`RefreshInterval = "1m0s"`, "AdvertiseNodes = false", "AdvertiseRunners = false", "MaxFailedRefreshes = 0", `Nodes = ["https://child.example"]`, `Grants = ["127.0.0.1"]`} {
		require.Contains(t, out, value)
	}
	t.Setenv("LIVEPEER_DISCOVERY_REFRESH_INTERVAL", "2m")
	out, err = execute(t, "--print-config")
	require.NoError(t, err)
	require.Contains(t, out, `RefreshInterval = "2m0s"`)
	out, err = execute(t, "--refresh-interval=3m", "--advertise-nodes=true", "--print-config")
	require.NoError(t, err)
	require.Contains(t, out, `RefreshInterval = "3m0s"`)
	require.Contains(t, out, "AdvertiseNodes = true")
}

func TestDiscoveryInvalidConfig(t *testing.T) {
	for _, args := range [][]string{
		{"--refresh-interval=0"}, {"--max-failed-refreshes=-1"}, {"--grants=bad"},
		{"--metrics-listen=0.0.0.0:8940"},
		{"--nodes=ftp://child.example"}, {"--nodes=https://user:secret@child.example"},
		{"--nodes=https://child.example?token=secret"}, {"--nodes=https://child.example#fragment"}, {"--runner-config=unused"},
	} {
		for _, mode := range []string{"--print-config", "--dump"} {
			out, err := execute(t, append(args, "--config=", mode)...)
			require.Error(t, err, "%v", args)
			require.Empty(t, out)
		}
	}
}

func TestDiscoveryDumpResults(t *testing.T) {
	for _, tc := range []struct {
		name           string
		sources        []string
		nodes, runners bool
		denied         bool
		calls, groups  int
		metadata       int
		warnings       int
		failed         bool
	}{
		{name: "inventory and normalized duplicates", sources: []string{"/deployment", "/deployment/"}, nodes: true, runners: true, calls: 1, groups: 2, metadata: 1},
		{name: "nodes only", sources: []string{"/deployment"}, nodes: true, calls: 1, metadata: 1},
		{name: "runners only", sources: []string{"/deployment"}, runners: true, calls: 1, groups: 2},
		{name: "disabled", sources: []string{"/deployment"}},
		{name: "no sources", nodes: true, runners: true},
		{name: "empty success with metadata", sources: []string{"/empty"}, nodes: true, runners: true, calls: 1, metadata: 1},
		{name: "empty success without metadata", sources: []string{"/empty"}, runners: true, calls: 1},
		{name: "partial failure", sources: []string{"/failed", "/deployment"}, nodes: true, runners: true, calls: 2, groups: 2, metadata: 1, warnings: 1},
		{name: "all sources fail", sources: []string{"/failed"}, nodes: true, runners: true, calls: 1, warnings: 1, failed: true},
		{name: "redirect is not followed", sources: []string{"/redirect"}, nodes: true, runners: true, calls: 1, warnings: 1, failed: true},
		{name: "private source denied", sources: []string{"/deployment"}, nodes: true, runners: true, denied: true, warnings: 1, failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch r.URL.Path {
				case "/deployment/discovery":
					_, _ = io.WriteString(w, childInventory)
				case "/empty/discovery":
					_, _ = io.WriteString(w, "[]")
				case "/failed/discovery":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "/redirect/discovery":
					http.Redirect(w, r, "/deployment/discovery", http.StatusFound)
				default:
					t.Errorf("unexpected discovery request: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer upstream.Close()
			args := []string{"--dump", "--config=", fmt.Sprintf("--advertise-nodes=%t", tc.nodes), fmt.Sprintf("--advertise-runners=%t", tc.runners)}
			if !tc.denied {
				args = append(args, "--grants", strings.TrimPrefix(upstream.URL, "http://"))
			}
			for _, source := range tc.sources {
				args = append(args, "--nodes", upstream.URL+source)
			}
			var out, logs bytes.Buffer
			cmd := Root(&out, &logs)
			cmd.SetArgs(args)
			err := cmd.ExecuteContext(t.Context())
			if tc.failed {
				require.ErrorContains(t, err, "all discovery sources failed")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int32(tc.calls), calls.Load())
			require.Equal(t, tc.warnings, strings.Count(logs.String(), "discovery refresh failed"))
			var entries []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(out.Bytes(), &entries))
			require.NotNil(t, entries)
			require.True(t, strings.HasSuffix(out.String(), "\n"))
			require.Equal(t, 1, strings.Count(out.String(), "\n"))
			metadata := 0
			if tc.metadata > 0 {
				metadata = 1
				var nodes []discoveryNode
				require.NoError(t, json.Unmarshal(entries[0]["nodes"], &nodes))
				require.Len(t, nodes, tc.metadata)
				require.Equal(t, upstream.URL+strings.TrimRight(tc.sources[len(tc.sources)-1], "/"), nodes[0].URL, "metadata contains the normalized source URL")
				require.False(t, nodes[0].QueriedAt.IsZero())
				require.NotContains(t, entries[0], "address")
			}
			require.Len(t, entries, metadata+tc.groups)
			for _, entry := range entries[metadata:] {
				require.Len(t, entry, 2)
				require.Contains(t, entry, "address")
				require.Contains(t, entry, "runners")
			}
		})
	}
}

func TestDiscoveryDumpConfiguration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	paths := make(chan string, 3)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		_, _ = io.WriteString(w, "[]")
	}))
	defer upstream.Close()
	config := filepath.Join(t.TempDir(), "discovery.toml")
	require.NoError(t, os.WriteFile(config, fmt.Appendf(nil, "Nodes = [%q]\nGrants = [%q]\nAdvertiseNodes = false\n", upstream.URL+"/file", strings.TrimPrefix(upstream.URL, "http://")), 0600))
	for _, source := range []string{"file", "env", "cli"} {
		if source == "env" {
			t.Setenv("LIVEPEER_DISCOVERY_NODES", upstream.URL+"/env")
		}
		args := []string{"--dump", "--config", config}
		if source == "cli" {
			args = append(args, "--nodes", upstream.URL+"/cli")
		}
		out, err := execute(t, args...)
		require.NoError(t, err)
		require.Equal(t, "[]\n", out)
		select {
		case path := <-paths:
			require.Equal(t, "/"+source+"/discovery", path)
		default:
			t.Fatal("configured source was not queried")
		}
	}

	t.Setenv("LIVEPEER_DISCOVERY_DUMP", "true")
	out, err := execute(t, "--config=", "--print-config")
	require.NoError(t, err, "environment cannot enable dump mode")
	require.NotContains(t, out, "Dump")
	out, err = execute(t, "--config=", "--dump", "--print-config")
	require.ErrorContains(t, err, "mutually exclusive")
	require.Empty(t, out)
	require.NoError(t, os.WriteFile(config, []byte("Dump = true\n"), 0600))
	out, err = execute(t, "--dump", "--config", config)
	require.ErrorContains(t, err, "Dump")
	require.Empty(t, out)
}

func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := Root(&output, &output)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}
