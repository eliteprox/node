package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestObserverMetricsAreLocalAndLogTransitions(t *testing.T) {
	f := test.NewChainRPC(t)
	rpc, err := eth.OpenRPC(f.URL)
	require.NoError(t, err)
	defer rpc.Close()
	contracts, err := eth.NewContracts(rpc, f.Controller)
	require.NoError(t, err)
	o := eth.NewPaymentObserver(eth.PaymentChain{Contracts: contracts}, true, common.HexToAddress("0x1234"))
	m := observability.New("test")
	rpc.SetObserver(m.RPC)
	f.Update(func(s *test.ChainState) { s.Fail = "BondingManager.isActiveTranscoder" })
	var logs bytes.Buffer
	m.PaymentObserver(o, slog.New(slog.NewTextHandler(&logs, nil)))
	require.Empty(t, test.MetricSamples(t, m.Registry, "chain_view_age_seconds", nil))
	stopped := make(chan struct{})
	hook := o.OnRefresh
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { defer close(stopped); o.Run(ctx) }()
	require.Eventually(t, func() bool {
		return test.MetricSum(t, m.Registry, "chain_view_refresh_duration_seconds", nil) == 1
	}, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "chain_view_healthy", nil))
	require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "chain_view_refresh_duration_seconds", nil))
	require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "chain_view_refreshes_total", map[string]string{"outcome": "partial"}))
	require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "chain_view_failed_entries", map[string]string{"kind": "recipient"}))
	require.EqualValues(t, f.Stats().RoundTrips, test.MetricSum(t, m.Registry, "rpc_work_total", map[string]string{"work": "background", "kind": "transport"}))
	f.Update(func(s *test.ChainState) { s.FailPayer = common.HexToAddress("0xbad") })
	_, err = (eth.PaymentChain{Contracts: contracts, Observer: o}).PayerFunds(eth.WithRPCWork(t.Context(), "foreground"), common.HexToAddress("0xbad"), common.Address{})
	require.Error(t, err)
	require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "chain_view_failed_entries", map[string]string{"kind": "payer"}))
	before := f.Stats()
	for range 3 {
		require.NotEmpty(t, test.MetricSamples(t, m.Registry, "chain_view_age_seconds", nil))
	}
	require.Equal(t, before, f.Stats(), "scraping must not perform chain reads")
	cancel()
	<-stopped
	hook(time.Millisecond, errors.New("private-provider-detail"))
	length := logs.Len()
	hook(time.Millisecond, errors.New("private-provider-detail"))
	require.Equal(t, length, logs.Len(), "unchanged failure must not log again")
	require.NotContains(t, logs.String(), "private-")
	require.EqualValues(t, 2, test.MetricSum(t, m.Registry, "chain_view_refreshes_total", map[string]string{"outcome": "error"}))
}

func TestRPCAccountingPreservesAggregateLabels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ ID json.RawMessage }
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32000, "message": "private-provider-text"}})
	}))
	defer server.Close()
	rpc, err := eth.OpenRPC(server.URL)
	require.NoError(t, err)
	defer rpc.Close()
	m := observability.New("test")
	rpc.SetObserver(m.RPC)
	contracts, err := eth.NewContracts(rpc, common.Address{})
	require.NoError(t, err)
	for _, tc := range []struct{ work, contract, function string }{
		{"foreground", "ticketBroker", "usedTickets"},
		{"background", "roundsManager", "currentRound"},
		{"unbounded-private-value", "roundsManager", "roundLength"},
	} {
		ctx := eth.WithRPCWork(t.Context(), tc.work)
		var args []any
		if tc.function == "usedTickets" {
			args = []any{common.Hash{}}
		}
		_, err := contracts.CallAt(ctx, "latest", tc.contract, common.Address{}, tc.function, args...)
		require.Error(t, err)
	}
	require.EqualValues(t, 3, test.MetricSum(t, m.Registry, "rpc_requests_total", map[string]string{"method": "eth_call", "outcome": "error"}))
	for _, work := range []string{"foreground", "background", "other"} {
		for _, kind := range []string{"transport", "contract_call"} {
			require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "rpc_work_total", map[string]string{"work": work, "kind": kind}))
		}
	}
	require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "rpc_contract_calls_total", map[string]string{"contract": "unknown", "function": "unknown"}))
	families, err := m.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, sample := range family.Metric {
			var names []string
			for _, label := range sample.Label {
				names = append(names, label.GetName())
			}
			switch family.GetName() {
			case "livepeer_rpc_requests_total":
				require.ElementsMatch(t, []string{"component", "method", "outcome"}, names)
			case "livepeer_rpc_request_duration_seconds":
				require.ElementsMatch(t, []string{"component", "method"}, names)
			}
		}
		require.NotContains(t, family.String(), "private-")
	}
}

// ResponseRecorder commits informational responses; a real server permits a
// later final response. Preserve that behavior in this controlled writer.
type provisionalRecorder struct{ *httptest.ResponseRecorder }

func (w provisionalRecorder) WriteHeader(status int) {
	if status >= 200 {
		w.ResponseRecorder.WriteHeader(status)
	}
}

func TestHTTPStreamingAndFailures(t *testing.T) {
	for _, outcome := range []string{"success", "stream_error", "client_cancelled", "session_stopped"} {
		t.Run(outcome, func(t *testing.T) {
			m := observability.New("test")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := httptest.NewRequest("POST", "/private-path?token=private", strings.NewReader("upload")).WithContext(ctx)
			serve := func(w http.ResponseWriter, r *http.Request) {
				observability.Operation(r, "application")
				observability.App(r, observability.Identity{App: "registered", Runner: "runner", Mode: "persistent"})
				_, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)
				require.EqualValues(t, 6, test.MetricSum(t, m.Registry, "stream_transfer_bytes_total", map[string]string{"direction": "in"}))
				_, hijack := w.(http.Hijacker)
				require.False(t, hijack, "must preserve the underlying interface set")
				w.WriteHeader(103)
				require.Zero(t, test.MetricSum(t, m.Registry, "app_response_header_duration_seconds", nil))
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(200)
				require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "app_response_header_duration_seconds", nil))
				require.NoError(t, http.NewResponseController(w).Flush())
				_, err = w.Write([]byte("data: hello\n\n"))
				require.NoError(t, err)
				require.EqualValues(t, 13, test.MetricSum(t, m.Registry, "stream_transfer_bytes_total", map[string]string{"direction": "out", "protocol": "sse"}))
				require.Zero(t, test.MetricSum(t, m.Registry, "app_calls_total", nil), "completion is recorded only after the stream ends")
				switch outcome {
				case "stream_error":
					panic(http.ErrAbortHandler)
				case "client_cancelled":
					cancel()
				case "session_stopped":
					observability.Outcome(r, "session_stopped")
				}
			}
			if outcome == "stream_error" {
				require.Panics(t, func() { m.HTTP(provisionalRecorder{httptest.NewRecorder()}, r, serve) })
			} else {
				m.HTTP(provisionalRecorder{httptest.NewRecorder()}, r, serve)
			}
			require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "app_calls_total", map[string]string{"outcome": outcome, "protocol": "sse"}))
			require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "app_response_header_duration_seconds", nil))
			require.Zero(t, test.MetricSum(t, m.Registry, "http_requests_in_flight", nil))
			require.Zero(t, test.MetricSum(t, m.Registry, "app_calls_in_flight", nil))
			require.EqualValues(t, len("data: hello\n\n"), test.MetricSum(t, m.Registry, "stream_transfer_bytes_total", map[string]string{"direction": "out"}))
		})
	}
}

type failedResponseWriter struct{ *httptest.ResponseRecorder }

func (w failedResponseWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestFailedResponsePreservesOutcome(t *testing.T) {
	for _, outcome := range []string{"transport_error", "capacity", "rejected", "session_stopped"} {
		for _, cancelled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancelled=%t", outcome, cancelled), func(t *testing.T) {
				m := observability.New("test")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if cancelled {
					cancel()
				}
				m.HTTP(failedResponseWriter{httptest.NewRecorder()}, httptest.NewRequest("GET", "/", nil).WithContext(ctx), func(w http.ResponseWriter, r *http.Request) {
					observability.App(r, observability.Identity{App: "model", Runner: "runner"})
					observability.Outcome(r, outcome)
					w.WriteHeader(502)
					_, _ = w.Write([]byte("failure"))
				})
				want := outcome
				if cancelled && outcome != "session_stopped" {
					want = "client_cancelled"
				}
				require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "app_calls_total", map[string]string{"outcome": want}))
			})
		}
	}
}

type readFromRecorder struct{ provisionalRecorder }

func (w readFromRecorder) ReadFrom(r io.Reader) (int64, error) { return io.Copy(w.ResponseRecorder, r) }

type readFunc func([]byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) { return f(p) }

func TestStreamingReadFromProgress(t *testing.T) {
	m := observability.New("test")
	m.HTTP(readFromRecorder{provisionalRecorder{httptest.NewRecorder()}}, httptest.NewRequest("GET", "/events", nil), func(w http.ResponseWriter, r *http.Request) {
		observability.App(r, observability.Identity{App: "model", Runner: "runner", Mode: "persistent"})
		w.Header().Set("Content-Type", "text/event-stream")
		reads := 0
		_, err := io.Copy(w, readFunc(func(p []byte) (int, error) {
			reads++
			if reads == 1 {
				return copy(p, "data: hello\n\n"), nil
			}
			require.EqualValues(t, 13, test.MetricSum(t, m.Registry, "stream_transfer_bytes_total", map[string]string{"direction": "out"}))
			require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "app_response_header_duration_seconds", nil))
			require.Zero(t, test.MetricSum(t, m.Registry, "app_calls_total", nil))
			return 0, io.EOF
		}))
		require.NoError(t, err)
	})
	require.EqualValues(t, 1, test.MetricSum(t, m.Registry, "app_calls_total", nil))
	require.EqualValues(t, 13, test.MetricSum(t, m.Registry, "stream_transfer_bytes_total", map[string]string{"direction": "out"}))
}

func TestUnknownInputsDoNotCreateSeries(t *testing.T) {
	m := observability.New("test")
	for i := range 50 {
		m.HTTP(httptest.NewRecorder(), httptest.NewRequest(fmt.Sprintf("PRIVATE-%d", i), fmt.Sprintf("/private/%d?secret=private", i), nil), func(w http.ResponseWriter, r *http.Request) {
			observability.App(r, observability.Identity{})
			observability.Stream(r, observability.Identity{}, "trickle")
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(404)
			_, _ = w.Write([]byte("unowned"))
		})
	}
	require.Empty(t, test.MetricSamples(t, m.Registry, "stream_transfer_bytes_total", nil))
	require.Empty(t, test.MetricSamples(t, m.Registry, "app_calls_total", nil))
	families, err := m.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, sample := range family.Metric {
			for _, label := range sample.Label {
				require.NotContains(t, label.GetValue(), "private")
				require.NotContains(t, label.GetValue(), "PRIVATE")
			}
		}
	}
	require.EqualValues(t, 50, test.MetricSum(t, m.Registry, "http_requests_total", map[string]string{"operation": "unknown", "method": "other"}))
	other := observability.New("test")
	require.Zero(t, test.MetricSum(t, other.Registry, "http_requests_total", nil), "registries must be isolated")
}
