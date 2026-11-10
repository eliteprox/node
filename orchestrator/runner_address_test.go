package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/destination"
	"github.com/stretchr/testify/require"
)

func TestRunnerFacingAddressKeepsPublicAndInternalURLsSeparate(t *testing.T) {
	controlHeaders := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		controlHeaders <- r.Header.Get("Livepeer-Session-Control")
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	policy, err := destination.New("runner", []string{strings.TrimPrefix(upstream.URL, "http://")})
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "https://public.example/external", time.Second, time.Minute)
	registry.runnerService = "http://runner.internal/internal"
	s := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Close()

	heartbeat := httptest.NewRequest(http.MethodPost, "http://runner.internal/internal/runners/heartbeat", strings.NewReader(`{"runner_id":"r","runner_url":"`+upstream.URL+`","app":"test","capacity":1}`))
	heartbeat.Header.Set("Authorization", "bootstrap")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, heartbeat)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var registration heartbeatResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &registration))
	require.Equal(t, "http://runner.internal/internal", registration.Orchestrator)
	require.Equal(t, "http://runner.internal/internal/ai/trickle/"+registration.O2R.ChannelName, registration.O2R.URL)
	require.Empty(t, registration.O2R.InternalURL)
	heartbeat = httptest.NewRequest(http.MethodPost, registration.Orchestrator+"/runners/heartbeat", strings.NewReader(`{"runner_id":"r","runner_url":"`+upstream.URL+`","app":"test","capacity":1}`))
	heartbeat.Header.Set("Authorization", registration.HeartbeatSecret)
	w = httptest.NewRecorder()
	s.ServeHTTP(w, heartbeat)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "https://public.example/external/discovery", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var discovered []discoveryEntry
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &discovered))
	require.Equal(t, "https://public.example/external", discovered[0].Address)

	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "https://public.example/external/apps/r/session", nil))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var reservation struct {
		SessionID  string `json:"session_id"`
		AppURL     string `json:"app_url"`
		ControlURL string `json:"control_url"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reservation))
	require.Equal(t, "https://public.example/external/apps/r/session/"+reservation.SessionID+"/app", reservation.AppURL)
	require.Equal(t, "https://public.example/external/apps/r/session/"+reservation.SessionID, reservation.ControlURL)

	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, reservation.AppURL+"/hello", nil))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	controlHeader := <-controlHeaders
	require.Equal(t, "http://runner.internal/internal/runner/r/session/"+reservation.SessionID, controlHeader)
	_, token, _, err := registry.sessionTarget("r", reservation.SessionID)
	require.NoError(t, err)
	create := httptest.NewRequest(http.MethodPost, controlHeader+"/channels", strings.NewReader(`{"channels":[{"name":"events"}]}`))
	create.Header.Set("Livepeer-Session-Token", token)
	w = httptest.NewRecorder()
	s.ServeHTTP(w, create)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var created struct {
		Channels []trickleChannel `json:"channels"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))
	require.Len(t, created.Channels, 1)
	channel := created.Channels[0]
	require.Equal(t, "https://public.example/external/ai/trickle/"+channel.ChannelName, channel.URL)
	require.Equal(t, "http://runner.internal/internal/ai/trickle/"+channel.ChannelName, channel.InternalURL)

	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, channel.InternalURL+"/0", bytes.NewBufferString("message")))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, channel.URL+"/0", nil))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, "message", w.Body.String())
}

func TestO2RKeepaliveRepeatsUntilShutdown(t *testing.T) {
	policy, err := destination.New("runner", nil)
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	s := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Close()
	w := httptest.NewRecorder()
	heartbeat := httptest.NewRequest(http.MethodPost, "/runners/heartbeat", strings.NewReader(`{"runner_id":"r","runner_url":"https://runner.example","app":"test"}`))
	heartbeat.Header.Set("Authorization", "bootstrap")
	s.ServeHTTP(w, heartbeat)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var registration heartbeatResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &registration))

	server := httptest.NewServer(s)
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		s.runO2RKeepalives(ctx, 10*time.Millisecond)
		close(done)
	}()
	defer func() { cancel(); <-done }()
	client := &http.Client{Timeout: time.Second}
	for seq := range 2 {
		response, err := client.Get(server.URL + "/ai/trickle/" + registration.O2R.ChannelName + "/" + strconv.Itoa(seq))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		var message map[string]string
		require.NoError(t, json.NewDecoder(response.Body).Decode(&message))
		require.NoError(t, response.Body.Close())
		require.Equal(t, map[string]string{"keep": "alive"}, message)
	}
	_, err = registry.Unregister(registration.RunnerID, registration.HeartbeatSecret)
	require.NoError(t, err)
	response, err := client.Get(server.URL + "/ai/trickle/" + registration.O2R.ChannelName + "/2")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestRunnerServiceURLValidation(t *testing.T) {
	p := Params{
		Listen: boa.HTTPBind{AddrPort: netip.MustParseAddrPort("[::]:8935")}, MetricsListen: boa.HTTPBind{AddrPort: netip.MustParseAddrPort("[::1]:8936")},
		ServiceURL: boa.Text[*url.URL]{Value: mustURL(t, "https://public.example/external")}, RunnerServiceURL: boa.Text[*url.URL]{Value: mustURL(t, "http://runner.internal/internal")},
		BootstrapSecret: "bootstrap", HeartbeatInterval: time.Second, HeartbeatTTL: time.Minute,
	}
	require.NoError(t, p.Validate())
	p.RunnerServiceURL.Value = mustURL(t, "http://user:secret@runner.internal/internal")
	require.ErrorContains(t, p.Validate(), "runner-service-url")
	p.RunnerServiceURL.Value = mustURL(t, "http://runner.internal/internal?token=secret")
	require.ErrorContains(t, p.Validate(), "runner-service-url")
}
