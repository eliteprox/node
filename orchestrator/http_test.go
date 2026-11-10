package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestDynamicRunnerSessionProxyAndCapacity(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/hello", r.URL.Path)
		require.Equal(t, "value", r.URL.Query().Get("q"))
		require.NotEmpty(t, r.Header.Get("Livepeer-Session-Id"))
		require.NotEmpty(t, r.Header.Get("Livepeer-Session-Token"))
		_, _ = io.WriteString(w, "proxied")
	}))
	defer upstream.Close()
	policy, err := destination.New("runner", []string{"127.0.0.0/8"})
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", 10*time.Millisecond, 50*time.Millisecond)
	srv := httptest.NewServer(NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	body := `{"runner_url":"` + upstream.URL + `/api","app":"test","mode":"persistent","capacity":1,"price_info":{"price":0,"currency":"usd","unit":"hour"}}`
	request, err := http.NewRequest(http.MethodPost, srv.URL+"/runners/heartbeat", bytes.NewBufferString(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "bootstrap")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var heartbeat heartbeatResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&heartbeat))
	require.NoError(t, response.Body.Close())
	require.Regexp(t, `^[a-z2-7]{52}$`, heartbeat.HeartbeatSecret)
	require.Regexp(t, `^app-[a-z2-7]{8}$`, heartbeat.RunnerID)
	require.NotNil(t, heartbeat.O2R)
	require.Regexp(t, "^"+heartbeat.RunnerID+`-[a-z2-7]{16}-o2r$`, heartbeat.O2R.ChannelName)
	// A heartbeat can move a runner to another port covered by the same grant.
	replacement := httptest.NewServer(upstream.Config.Handler)
	defer replacement.Close()
	upstream.Close()
	body = strings.Replace(body, upstream.URL, replacement.URL, 1)
	request, err = http.NewRequest(http.MethodPost, srv.URL+"/runners/heartbeat", bytes.NewBufferString(strings.Replace(body, `{"runner_url"`, `{"runner_id":"`+heartbeat.RunnerID+`","runner_url"`, 1)))
	require.NoError(t, err)
	request.Header.Set("Authorization", heartbeat.HeartbeatSecret)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response, err = http.Post(srv.URL+"/apps/"+heartbeat.RunnerID+"/session", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var reserved struct {
		SessionID string `json:"session_id"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&reserved))
	require.Regexp(t, `^session_[a-z2-7]{16}$`, reserved.SessionID)
	require.NoError(t, response.Body.Close())
	response, err = http.Get(srv.URL + "/ai/trickle/" + heartbeat.O2R.ChannelName + "/0")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var event map[string]string
	require.NoError(t, json.NewDecoder(response.Body).Decode(&event))
	require.Equal(t, "reserved", event["event"])
	require.Equal(t, reserved.SessionID, event["session"])
	require.True(t, registry.validSessionToken(heartbeat.RunnerID, reserved.SessionID, event["session_token"]))
	require.NoError(t, response.Body.Close())
	response, err = http.Post(srv.URL+"/apps/"+heartbeat.RunnerID+"/session", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response, err = http.Get(srv.URL + "/apps/" + heartbeat.RunnerID + "/session/" + reserved.SessionID + "/app/hello?q=value")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "proxied", string(data))
	require.NoError(t, response.Body.Close())
	response, err = http.Post(srv.URL+"/apps/"+heartbeat.RunnerID+"/session/"+reserved.SessionID+"/stop", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestSessionCallbackCredentials(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "r", RunnerURL: "http://127.0.0.1:1", App: "app", Capacity: 2}))
	sid, _, _, _, err := registry.Reserve("r")
	require.NoError(t, err)
	other, _, _, _, err := registry.Reserve("r")
	require.NoError(t, err)
	target, token, _, err := registry.sessionTarget("r", sid)
	require.NoError(t, err)
	_, otherToken, _, err := registry.sessionTarget("r", other)
	require.NoError(t, err)
	for _, operation := range []struct{ method, path string }{
		{http.MethodPost, "proxy"},
		{http.MethodPost, "channels"},
		{http.MethodDelete, "channels"},
		{http.MethodPost, "stop"},
	} {
		for name, credential := range map[string]string{"missing": "", "wrong": "wrong", "cross-session": otherToken} {
			t.Run(operation.method+" "+operation.path+"/"+name, func(t *testing.T) {
				req := httptest.NewRequest(operation.method, "/runner/r/session/"+sid+"/"+operation.path, strings.NewReader(`{}`))
				if credential != "" {
					req.Header.Set("Livepeer-Session-Token", credential)
				}
				w := httptest.NewRecorder()
				server.ServeHTTP(w, req)
				require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
			})
		}
	}
	for range 25 {
		_, err := registry.addProxy("r", sid, token, target)
		require.NoError(t, err)
	}
	req := httptest.NewRequest(http.MethodPost, "/runner/r/session/"+sid+"/proxy", strings.NewReader(`{}`))
	req.Header.Set("Livepeer-Session-Token", token)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Body.String(), "too many session proxies")
}

func TestRunnerRegistryHasBoundedCapacity(t *testing.T) {
	registry := NewRegistry("bootstrap", "https://orchestrator.example", time.Second, time.Minute)
	for i := range maxRunners {
		_, status, err := registry.Heartbeat(heartbeatRequest{
			RunnerID: fmt.Sprintf("runner%d", i), RunnerURL: "https://runner.example",
			App: "test", Mode: "persistent", Capacity: 1,
		}, "bootstrap")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
	}
	_, status, err := registry.Heartbeat(heartbeatRequest{
		RunnerID: "overflow", RunnerURL: "https://runner.example",
		App: "test", Mode: "persistent", Capacity: 1,
	}, "bootstrap")
	require.ErrorContains(t, err, "capacity")
	require.Equal(t, http.StatusServiceUnavailable, status)
}

func TestSessionProxyStreamsSSEAndWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: hello\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/ws":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			kind, message, err := conn.ReadMessage()
			if err == nil {
				_ = conn.WriteMessage(kind, message)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()
	grant := strings.TrimPrefix(upstream.URL, "http://")
	policy, err := destination.New("runner", []string{grant})
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	registration := heartbeatRequest{RunnerURL: upstream.URL, App: "stream", Mode: "persistent", Capacity: 1}
	registered, _, err := registry.Heartbeat(registration, "bootstrap")
	require.NoError(t, err)
	registration.RunnerID = registered.RunnerID
	_, _, err = registry.Heartbeat(registration, registered.HeartbeatSecret)
	require.NoError(t, err)
	server := httptest.NewServer(NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	sid, _, _, _, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	base := server.URL + "/apps/" + registered.RunnerID + "/session/" + sid + "/app"
	response, err := http.Get(base + "/events")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "text/event-stream", response.Header.Get("Content-Type"))
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "data: hello\n", line)
	require.NoError(t, response.Body.Close())
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/ws"
	conn, handshake, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	if handshake != nil {
		require.NoError(t, handshake.Body.Close())
	}
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte("echo")))
	kind, message, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, kind)
	require.Equal(t, "echo", string(message))
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool { return test.MetricSum(t, registry.metrics.Registry, "app_calls_total", nil) == 2 }, time.Second, 10*time.Millisecond)
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"protocol": "websocket"}))
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"protocol": "sse", "outcome": "client_cancelled"}))
	require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "app_calls_in_flight", nil))
}

func TestRunnerDestinationDeniedWithoutGrant(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer upstream.Close()
	policy, err := destination.New("runner", nil)
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	server := httptest.NewServer(NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	body := `{"runner_url":"` + upstream.URL + `","app":"test","mode":"single-shot","capacity":1,"price_info":{"price":0}}`
	request, err := http.NewRequest(http.MethodPost, server.URL+"/runners/heartbeat", bytes.NewBufferString(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "bootstrap")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	var heartbeat heartbeatResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&heartbeat))
	require.NoError(t, response.Body.Close())
	_, _, err = registry.Heartbeat(heartbeatRequest{RunnerID: heartbeat.RunnerID, RunnerURL: upstream.URL, App: "test", Mode: "single-shot", Capacity: 1}, heartbeat.HeartbeatSecret)
	require.NoError(t, err)
	response, err = http.Get(server.URL + "/apps/" + heartbeat.RunnerID + "/app")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadGateway, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestGeneratedProxyHasSeparateDestinationGrant(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer upstream.Close()
	grant := strings.TrimPrefix(upstream.URL, "http://")
	runnerPolicy, err := destination.New("runner", []string{"127.0.0.0/8"})
	require.NoError(t, err)
	proxyPolicy, err := destination.New("session-proxy", nil)
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	registration := heartbeatRequest{RunnerURL: upstream.URL, App: "test", Mode: "persistent", Capacity: 1}
	registered, status, err := registry.Heartbeat(registration, "bootstrap")
	require.NoError(t, err)
	registration.RunnerID = registered.RunnerID
	_, _, err = registry.Heartbeat(registration, registered.HeartbeatSecret)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	sid, _, _, status, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	_, token, status, err := registry.sessionTarget(registered.RunnerID, sid)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	srv := NewServer(registry, runnerPolicy, proxyPolicy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	path := "/runner/" + registered.RunnerID + "/session/" + sid + "/proxy"
	create := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{"target_url":"`+upstream.URL+`"}`))
	create.Header.Set("Livepeer-Session-Token", "bad")
	record := httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, http.StatusUnauthorized, record.Code)
	create = httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{"target_url":"`+upstream.URL+`"}`))
	create.Header.Set("Livepeer-Session-Token", token)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, http.StatusOK, record.Code)
	var created struct {
		ProxyID string `json:"proxy_id"`
		URL     string `json:"url"`
	}
	require.NoError(t, json.Unmarshal(record.Body.Bytes(), &created))
	require.NotEmpty(t, created.ProxyID)
	require.Equal(t, "http://orchestrator.example/run/"+created.ProxyID, created.URL)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/run/"+created.ProxyID, nil))
	require.Equal(t, http.StatusBadGateway, record.Code)
	granted, err := destination.New("session-proxy", []string{grant})
	require.NoError(t, err)
	srv = NewServer(registry, runnerPolicy, granted, slog.New(slog.NewTextHandler(io.Discard, nil)))
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/run/"+created.ProxyID, nil))
	require.Equal(t, http.StatusOK, record.Code)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/proxy/"+created.ProxyID, nil))
	require.Equal(t, http.StatusOK, record.Code)
	create = httptest.NewRequest("POST", "/runner/"+registered.RunnerID+"/session/"+sid+"/channels", strings.NewReader(`{"channels":[{"name":"events"}]}`))
	create.Header.Set("Livepeer-Session-Token", token)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, 200, record.Code)
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "trickle_channels", map[string]string{"kind": "session"}), "the replacement server owns the only snapshot")
	_, err = registry.metrics.Registry.Gather()
	require.NoError(t, err)
}

func TestRunnerExpires(t *testing.T) {
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Millisecond, 2*time.Millisecond)
	registered, _, err := registry.Heartbeat(heartbeatRequest{RunnerURL: "https://runner.example", App: "test", Mode: "persistent", Capacity: 1}, "bootstrap")
	require.NoError(t, err)
	require.NotEmpty(t, registered.RunnerID)
	require.Len(t, test.MetricSamples(t, registry.metrics.Registry, "runner_sessions", map[string]string{"runner_id": registered.RunnerID, "state": "active"}), 1)
	require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "runner_sessions", nil))
	time.Sleep(3 * time.Millisecond)
	registry.Expire()
	count, _ := registry.Counts()
	require.Zero(t, count)
	for _, name := range []string{"runner_registered", "runner_sessions", "runner_capacity_available"} {
		require.Empty(t, test.MetricSamples(t, registry.metrics.Registry, name, nil))
	}
}

func TestTrickleChannelsScopedToSession(t *testing.T) {
	policy, err := destination.New("runner", nil)
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	registration := heartbeatRequest{RunnerURL: "https://runner.example", App: "test", Mode: "persistent", Capacity: 2}
	registered, _, err := registry.Heartbeat(registration, "bootstrap")
	require.NoError(t, err)
	registration.RunnerID = registered.RunnerID
	_, _, err = registry.Heartbeat(registration, registered.HeartbeatSecret)
	require.NoError(t, err)
	srv := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sid1, _, _, _, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	sid2, _, _, _, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	_, token1, _, err := registry.sessionTarget(registered.RunnerID, sid1)
	require.NoError(t, err)
	_, token2, _, err := registry.sessionTarget(registered.RunnerID, sid2)
	require.NoError(t, err)
	path1 := "/runner/" + registered.RunnerID + "/session/" + sid1 + "/channels"
	create := httptest.NewRequest(http.MethodPost, path1, bytes.NewBufferString(`{"channels":[{"name":"events","mime_type":"application/json"}]}`))
	create.Header.Set("Livepeer-Session-Token", token1)
	record := httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, http.StatusOK, record.Code)
	var created struct {
		Channels []struct {
			ChannelName string `json:"channel_name"`
		} `json:"channels"`
	}
	require.NoError(t, json.Unmarshal(record.Body.Bytes(), &created))
	require.Len(t, created.Channels, 1)
	channelID := created.Channels[0].ChannelName
	path2 := "/runner/" + registered.RunnerID + "/session/" + sid2 + "/channels"
	deletion := httptest.NewRequest(http.MethodDelete, path2, bytes.NewBufferString(`{"channels":["`+channelID+`"]}`))
	deletion.Header.Set("Livepeer-Session-Token", token2)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, deletion)
	require.Equal(t, http.StatusOK, record.Code)
	require.JSONEq(t, `{"deleted":[]}`, record.Body.String())
	segment := "/ai/trickle/" + channelID + "/0"
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodPost, segment, bytes.NewBufferString(`{"x":1}`)))
	require.Equal(t, http.StatusOK, record.Code)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, segment, nil))
	require.Equal(t, http.StatusOK, record.Code)
	require.JSONEq(t, `{"x":1}`, record.Body.String())
	deletion = httptest.NewRequest(http.MethodDelete, path1, bytes.NewBufferString(`{"channels":["`+channelID+`"]}`))
	deletion.Header.Set("Livepeer-Session-Token", token1)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, deletion)
	require.Equal(t, http.StatusOK, record.Code)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, segment, nil))
	require.Equal(t, http.StatusNotFound, record.Code)
}

func TestRunnerGPUIsPreservedForPythonDiscovery(t *testing.T) {
	var request heartbeatRequest
	require.NoError(t, json.Unmarshal([]byte(`{"runner_id":"gpu-runner","runner_url":"https://runner.example","app":"retained","mode":"single-shot","status":"ready","capacity":1,"gpu":{"id":"0","name":"H100","vram_mb":80000}}`), &request))
	registry := NewRegistry("bootstrap", "https://orchestrator.example", time.Second, 30*time.Second)
	registered, status, err := registry.Heartbeat(request, "bootstrap")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	_, status, err = registry.Heartbeat(request, registered.HeartbeatSecret)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	entries := registry.Discovery()
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Runners, 1)
	require.Equal(t, "H100", entries[0].Runners[0].GPU.Name)
}

func registeredApp(id string) appRequest {
	return appRequest{ID: id, App: "repeated-name", RunnerURL: "https://runner.example/base", Capacity: 2}
}

func appHeartbeat(t *testing.T, registry *Registry, owner, credential string, apps ...appRequest) heartbeatResponse {
	t.Helper()
	response, status, err := registry.Heartbeat(heartbeatRequest{RunnerID: owner, Version: "runner-v1", Apps: append([]appRequest{}, apps...)}, credential)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	return response
}

func heartbeatHTTP(t *testing.T, server *Server, body any, credential string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/runners/heartbeat", bytes.NewReader(data))
	req.Header.Set("Authorization", credential)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, req)
	return response
}

func TestMultiAppHeartbeatRoutingAndVersions(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.NotEmpty(t, req.Header.Get("Livepeer-Session-Token"))
		require.Contains(t, req.Header.Get("Livepeer-Session-Control"), "/runner/"+req.Header.Get("Livepeer-Runner-Route")+"/session/")
		_, _ = io.WriteString(w, req.Header.Get("Livepeer-Runner-Route")+req.URL.Path)
	}))
	defer upstream.Close()
	registry, server := reviewServer(t, upstream.URL)
	defer server.Close()
	registry.proxyTemplate = "https://{proxy}.apps.example"
	persistent := registeredApp("persistent")
	persistent.RunnerURL, persistent.Version, persistent.Proxy = upstream.URL+"/persistent", "app-v1", true
	shot := registeredApp("")
	shot.RunnerURL, shot.Version, shot.Mode, shot.Proxy, shot.Capacity = upstream.URL+"/shot", "app-v2", "single-shot", true, 1
	payload := map[string]any{"runner_id": "owner", "version": "runner-v9", "apps": []appRequest{persistent, shot}}
	w := heartbeatHTTP(t, server, payload, "bootstrap")
	require.Equal(t, 200, w.Code, w.Body.String())
	var response heartbeatResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Apps, 2)
	require.NotNil(t, response.O2R)
	require.Equal(t, "persistent", response.Apps[0].ID)
	shot.ID = response.Apps[1].ID
	require.Regexp(t, `^app-[a-z2-7]{8}$`, shot.ID)
	require.Equal(t, "runner-v9", registry.runners["owner"].Version)
	require.JSONEq(t, `[]`, string(mustJSON(t, response.Apps[0].SessionIDs)))
	payload["apps"] = []appRequest{persistent, shot}
	require.Equal(t, 200, heartbeatHTTP(t, server, payload, response.HeartbeatSecret).Code)
	inventory := registry.Discovery()[0].Runners
	require.Len(t, inventory, 2)
	versions := map[string]string{}
	for _, item := range inventory {
		require.Equal(t, "repeated-name", item.App)
		versions[item.Mode] = item.Version
		if item.Mode == "single-shot" {
			require.Equal(t, "https://"+shot.ID+".apps.example", item.URL)
		}
	}
	require.Equal(t, map[string]string{"persistent": "app-v1", "single-shot": "app-v2"}, versions)
	sid, proxy, control, _, err := registry.Reserve("persistent")
	require.NoError(t, err)
	require.Contains(t, control, "/apps/persistent/session/"+sid)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, proxy+"/hello", nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, "persistent/persistent/hello", w.Body.String())
	w = httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, registry.proxyURL(shot.ID)+"/hello", nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, shot.ID+"/shot/hello", w.Body.String())
	payload["apps"] = []appRequest{persistent, shot}
	w = heartbeatHTTP(t, server, payload, response.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	var reconnect heartbeatResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reconnect))
	require.Empty(t, reconnect.HeartbeatSecret)
	require.Equal(t, response.O2R, reconnect.O2R)
	require.Equal(t, []string{sid}, reconnect.Apps[0].SessionIDs, "runner's empty session list must not release a session")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, response.O2R.URL+"/0", nil))
	require.Equal(t, 200, w.Code)
	var event map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &event))
	require.Equal(t, "persistent", event["app_id"])
	require.Equal(t, "reserved", event["event"])
	require.Equal(t, sid, event["session"])
	// Callback authorization and generated channels use app IDs, not owner IDs.
	req := httptest.NewRequest(http.MethodPost, "/runner/persistent/session/"+sid+"/channels", strings.NewReader(`{"channels":[{"name":"data"}]}`))
	req.Header.Set("Livepeer-Session-Token", event["session_token"])
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	var channels struct {
		Channels []trickleChannel `json:"channels"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &channels))
	require.Len(t, channels.Channels, 1)
	server.mu.Lock()
	server.publishSessionEvent(channels.Channels[0].ChannelName, []byte("still streaming"))
	server.mu.Unlock()
	payload["apps"] = []appRequest{shot}
	require.Equal(t, 200, heartbeatHTTP(t, server, payload, response.HeartbeatSecret).Code)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, channels.Channels[0].URL+"/0", nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, "still streaming", w.Body.String())
	w = httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, proxy+"/after-drain", nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, "persistent/persistent/after-drain", w.Body.String())
	registry.ReleaseBySession(sid)
	require.Empty(t, registry.sessions)
	require.Len(t, server.channels, 1, "only the owner O2R channel remains")
	require.EqualValues(t, 2, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"runner_id": "owner", "app_id": "persistent"}))
	// A direct O2R deletion is repaired on reconnect without changing credentials.
	w = httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, response.O2R.URL, nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	w = heartbeatHTTP(t, server, payload, response.HeartbeatSecret)
	require.Equal(t, 200, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reconnect))
	require.NotNil(t, reconnect.O2R)
	require.NotEqual(t, response.O2R.URL, reconnect.O2R.URL)
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}

func TestMultiAppSnapshotValidationIsAtomic(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	registry.SetWeiPerUSD(big.NewRat(3600, 1))
	first := registeredApp("first")
	first.PriceInfo = priceInfo{Price: "1", Currency: "usd", Unit: "hour"}
	initial := appHeartbeat(t, registry, "owner", "bootstrap", first)
	appHeartbeat(t, registry, initial.RunnerID, initial.HeartbeatSecret, first)
	before := registry.runners["owner"].Last
	invalids := []appRequest{
		{ID: "bad", App: "x", RunnerURL: "file:///bad"},
		{ID: "bad/id", App: "x", RunnerURL: "https://runner.example"},
		{ID: "bad", App: "x", RunnerURL: "https://runner.example", Capacity: -1},
		{ID: "bad", App: "x", RunnerURL: "https://runner.example", Mode: "unknown"},
		{ID: "bad", App: "x", RunnerURL: "https://runner.example", GPU: &runnerGPU{VRAMMB: -1}},
		registeredApp("first"),        // duplicate supplied ID
		registeredApp("new-unpriced"), // on-chain apps require positive prices
	}
	for _, invalid := range invalids {
		changed := first
		changed.PriceInfo.Price = "2"
		_, status, err := registry.Heartbeat(heartbeatRequest{RunnerID: "owner", Version: "changed", Apps: []appRequest{changed, invalid}}, initial.HeartbeatSecret)
		require.Error(t, err)
		require.Equal(t, 400, status)
		require.Equal(t, before, registry.runners["owner"].Last)
		require.Equal(t, "runner-v1", registry.runners["owner"].Version)
		price, _, err := registry.PriceForRunner("first")
		require.NoError(t, err)
		require.Equal(t, "1", price.Price.String())
		require.Len(t, registry.apps, 1)
	}
	for _, scalar := range []string{"app", "runner_url", "mode", "proxy", "capacity", "price_info", "status", "metadata", "gpu", "session_ids"} {
		for _, zero := range []any{nil, false, 0, ""} {
			body := map[string]any{"runner_id": "owner", "apps": []any{}, scalar: zero}
			w := heartbeatHTTP(t, server, body, initial.HeartbeatSecret)
			require.Equal(t, 400, w.Code, "%s=%v: %s", scalar, zero, w.Body.String())
			require.Equal(t, before, registry.runners["owner"].Last)
		}
	}
	for _, apps := range []any{nil, "", map[string]any{}, []any{nil}} {
		w := heartbeatHTTP(t, server, map[string]any{"runner_id": "owner", "apps": apps}, initial.HeartbeatSecret)
		require.Equal(t, 400, w.Code, w.Body.String())
	}
	oversized := map[string]any{"runner_id": "owner", "version": strings.Repeat("x", maxControlBody), "apps": []any{}}
	require.Equal(t, 400, heartbeatHTTP(t, server, oversized, initial.HeartbeatSecret).Code)
	require.Equal(t, before, registry.runners["owner"].Last)
	// Empty snapshots are valid and preserve an explicit empty apps array.
	w := heartbeatHTTP(t, server, map[string]any{"runner_id": "owner", "apps": []any{}}, initial.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"apps":[]`)
	require.Empty(t, registry.apps)
}

func TestMultiAppOwnershipDrainAndIDReuse(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	a, b := registeredApp("a"), registeredApp("b")
	owner := appHeartbeat(t, registry, "owner", "bootstrap", a, b)
	appHeartbeat(t, registry, owner.RunnerID, owner.HeartbeatSecret, a, b)
	other := appHeartbeat(t, registry, "other", "bootstrap")
	sid, _, _, _, err := registry.Reserve("a")
	require.NoError(t, err)
	_, token, _, err := registry.sessionTarget("a", sid)
	require.NoError(t, err)
	ctx, _ := registry.sessionContext("a", sid)
	// Releasing or reserving a manifest through another app cannot affect a.
	_, _, _, status, err := registry.ReserveWithID("b", sid)
	require.Error(t, err)
	require.Equal(t, 409, status)
	_, err = registry.release("b", sid, token, true)
	require.Error(t, err)
	require.NoError(t, ctx.Err())
	require.False(t, registry.validSessionToken("b", sid, token))
	require.False(t, registry.validSessionToken("owner", sid, token))
	drained := appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, b)
	require.Equal(t, []appResponse{{ID: "b", Status: "ready", SessionIDs: []string{}}, {ID: "a", Status: "draining", SessionIDs: []string{sid}}}, drained.Apps)
	require.Len(t, registry.Discovery()[0].Runners, 1)
	_, _, _, _, err = registry.Reserve("a")
	require.Error(t, err)
	_, _, status, err = registry.sessionTarget("a", sid)
	require.NoError(t, err)
	require.Equal(t, 200, status)
	for _, auth := range []string{"bootstrap", other.HeartbeatSecret} {
		_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "owner", Apps: []appRequest{a, b}}, auth)
		require.Error(t, err)
		require.Equal(t, 401, status)
	}
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "other", Apps: []appRequest{a}}, other.HeartbeatSecret)
	require.Error(t, err)
	require.Equal(t, 409, status, "draining IDs are still owned")
	require.Error(t, registry.AddStatic(StaticRunner{ID: "a", App: "static", RunnerURL: a.RunnerURL}))
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "a", App: "legacy", RunnerURL: a.RunnerURL}, "bootstrap")
	require.Error(t, err)
	require.Equal(t, 409, status)
	appHeartbeat(t, registry, "owner", owner.HeartbeatSecret)
	before := registry.runners["owner"].Last
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "owner", App: "compat", RunnerURL: a.RunnerURL}, owner.HeartbeatSecret)
	require.Error(t, err)
	require.Equal(t, 409, status, "scalar requests cannot replace even a draining sibling")
	require.Equal(t, before, registry.runners["owner"].Last)
	appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, a, b)
	require.False(t, registry.apps["a"].Removing)
	require.NoError(t, ctx.Err())
	appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, b)
	registry.ReleaseBySession(sid)
	require.NotContains(t, registry.apps, "a")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	appHeartbeat(t, registry, "other", other.HeartbeatSecret, a)
	require.Equal(t, "other", registry.apps["a"].owner.ID, "fully removed IDs may be reused")
	// Legacy and static routes occupy the same namespace.
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "static", App: "static", RunnerURL: a.RunnerURL}))
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "other", Apps: []appRequest{registeredApp("static")}}, other.HeartbeatSecret)
	require.Error(t, err)
	require.Equal(t, 409, status)
	scalar := heartbeatRequest{RunnerID: "scalar", App: "compat", RunnerURL: a.RunnerURL}
	registered, _, err := registry.Heartbeat(scalar, "bootstrap")
	require.NoError(t, err)
	_, _, err = registry.Heartbeat(scalar, registered.HeartbeatSecret)
	require.NoError(t, err)
	active, _, _, _, err := registry.Reserve("scalar")
	require.NoError(t, err)
	response := appHeartbeat(t, registry, "scalar", registered.HeartbeatSecret, scalar.scalarApp())
	require.Equal(t, []string{active}, response.Apps[0].SessionIDs)
	response, _, err = registry.Heartbeat(scalar, registered.HeartbeatSecret)
	require.NoError(t, err)
	require.Nil(t, response.Apps)
	require.Equal(t, []string{active}, response.SessionIDs)
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "owner", App: "legacy", RunnerURL: a.RunnerURL}, owner.HeartbeatSecret)
	require.Error(t, err)
	require.Equal(t, 409, status)
	// Unregister must use the owner identity and removes only its apps.
	_, err = registry.Unregister("b", owner.HeartbeatSecret)
	require.Error(t, err)
	_, err = registry.Unregister("owner", owner.HeartbeatSecret)
	require.NoError(t, err)
	require.Contains(t, registry.apps, "a")
	require.NotContains(t, registry.apps, "b")
}

func TestMultiAppPinningAndPendingAdmission(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	registry.SetWeiPerUSD(big.NewRat(3600, 1))
	a, b := registeredApp("a"), registeredApp("b")
	a.PriceInfo, b.PriceInfo = priceInfo{Price: "1", Unit: "hour"}, priceInfo{Price: "2", Unit: "fixed"}
	a.Proxy = true
	owner := appHeartbeat(t, registry, "owner", "bootstrap", a, b)
	appHeartbeat(t, registry, owner.RunnerID, owner.HeartbeatSecret, a, b)
	quote, _, err := registry.quoteForApp("a")
	require.NoError(t, err)
	sid, proxy, _, _, err := registry.reserveWithPrice("a", "pending", &quote, "persistent")
	require.NoError(t, err)
	a.RunnerURL, a.PriceInfo.Price, a.App, a.Mode, a.Proxy = "https://replacement.example", "3", "changed", "single-shot", false
	a.Capacity = 1
	appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, a, b)
	appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, b)
	require.True(t, registry.activateSession("a", sid), "pre-drain pending payments remain admitted")
	target, _, _, err := registry.sessionTarget("a", sid)
	require.NoError(t, err)
	require.Equal(t, "https://runner.example/base", target.String())
	price, _, err := registry.PriceForSession("a", sid)
	require.NoError(t, err)
	require.Equal(t, "1", price.Price.String())
	require.Equal(t, "repeated-name", registry.identity("a", sid).App)
	u, err := url.Parse(proxy)
	require.NoError(t, err)
	proxied, appID, _, _, ok := registry.proxyTarget(strings.TrimPrefix(u.Path, "/run/"))
	require.True(t, ok)
	require.Equal(t, "a", appID)
	require.Equal(t, target, proxied.target)
	appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, a, b)
	registry.ReleaseBySession(sid)
	next, _, _, _, err := registry.ReserveWithID("a", "new-call")
	require.NoError(t, err)
	target, _, _, err = registry.sessionTarget("a", next)
	require.NoError(t, err)
	require.Equal(t, "https://replacement.example", target.String())
	price, _, err = registry.PriceForSession("a", next)
	require.NoError(t, err)
	require.Equal(t, "3", price.Price.String())
	require.Equal(t, "changed", registry.identity("a", next).App)
}

func TestMultiAppExpiryAndRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry, server := reviewServer(t, "http://127.0.0.1:1")
		defer server.Close()
		a, b := registeredApp("a"), registeredApp("b")
		old := appHeartbeat(t, registry, "old", "bootstrap", a, b)
		appHeartbeat(t, registry, old.RunnerID, old.HeartbeatSecret, a, b)
		sid, _, _, _, err := registry.Reserve("a")
		require.NoError(t, err)
		ctx, _ := registry.sessionContext("a", sid)
		replacement := appHeartbeat(t, registry, "replacement", "bootstrap", registeredApp("replacement-app"))
		_, status, err := registry.Heartbeat(heartbeatRequest{RunnerID: "replacement", Apps: []appRequest{a}}, replacement.HeartbeatSecret)
		require.Error(t, err)
		require.Equal(t, 409, status)
		time.Sleep(time.Minute + time.Second)
		// Reconnect the replacement before running expiry reconciliation.
		appHeartbeat(t, registry, "replacement", replacement.HeartbeatSecret, registeredApp("replacement-app"))
		registry.Expire()
		require.ErrorIs(t, context.Cause(ctx), errRunnerUnavailable)
		require.NotContains(t, registry.apps, "a")
		require.NotContains(t, registry.apps, "b")
		_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "old", Apps: []appRequest{a, b}}, old.HeartbeatSecret)
		require.Error(t, err)
		require.Equal(t, 401, status)
		appHeartbeat(t, registry, "replacement", replacement.HeartbeatSecret, a, b)
		require.Empty(t, registry.sessions)
	})
}

func TestMultiAppLimitsAndConcurrentCapacity(t *testing.T) {
	registry := NewRegistry("bootstrap", "https://orch.example", time.Second, time.Minute)
	apps := func(prefix string, n int) []appRequest {
		result := make([]appRequest, n)
		for i := range result {
			result[i] = registeredApp(fmt.Sprintf("%s-%d", prefix, i))
		}
		return result
	}
	owner := appHeartbeat(t, registry, "owner", "bootstrap", apps("first", maxAppsPerRunner)...)
	appHeartbeat(t, registry, owner.RunnerID, owner.HeartbeatSecret, apps("first", maxAppsPerRunner)...)
	_, status, err := registry.Heartbeat(heartbeatRequest{RunnerID: "owner", Apps: apps("over", maxAppsPerRunner+1)}, owner.HeartbeatSecret)
	require.Error(t, err)
	require.Equal(t, 400, status)
	sid, _, _, _, err := registry.Reserve("first-0")
	require.NoError(t, err)
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "owner", Apps: apps("new", maxAppsPerRunner)}, owner.HeartbeatSecret)
	require.Error(t, err)
	require.Equal(t, 503, status, "omitted apps with sessions count toward the retained limit")
	registry.ReleaseBySession(sid)
	// Replacement snapshots can release idle registrations and fill the quota atomically.
	appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, apps("new", maxAppsPerRunner)...)
	for i := 1; i < maxApps/maxAppsPerRunner; i++ {
		appHeartbeat(t, registry, fmt.Sprintf("owner-%d", i), "bootstrap", apps(fmt.Sprintf("group%d", i), maxAppsPerRunner)...)
	}
	require.Len(t, registry.apps, maxApps)
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "overflow", Apps: []appRequest{registeredApp("overflow")}}, "bootstrap")
	require.Error(t, err)
	require.Equal(t, 503, status)
	require.NotContains(t, registry.runners, "overflow")
	require.Error(t, registry.AddStatic(StaticRunner{ID: "static-overflow", App: "x", RunnerURL: "https://runner.example"}))
	_, status, err = registry.Heartbeat(heartbeatRequest{RunnerID: "legacy-overflow", App: "x", RunnerURL: "https://runner.example"}, "bootstrap")
	require.Error(t, err)
	require.Equal(t, 503, status)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var admitted []string
	for range 20 {
		wg.Go(func() {
			sid, _, _, _, err := registry.Reserve("new-0")
			if err == nil {
				mu.Lock()
				admitted = append(admitted, sid)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	require.Len(t, admitted, 2)
	_, _, _, _, err = registry.Reserve("new-1")
	require.NoError(t, err, "sibling capacity remains available")
	require.Equal(t, []string{"new-0", "new-0"}, []string{registry.sessions[admitted[0]].ID, registry.sessions[admitted[1]].ID})
}

func TestGeneratedAppIDCollisions(t *testing.T) {
	registry := NewRegistry("bootstrap", "https://orch.example", time.Second, time.Minute)
	defer registry.Close()
	existing := registeredApp("app-aaaaaaaa")
	owner := appHeartbeat(t, registry, "", "bootstrap", existing)
	appHeartbeat(t, registry, owner.RunnerID, owner.HeartbeatSecret, existing)
	require.Regexp(t, `^app-[a-z2-7]{8}$`, owner.RunnerID)
	sid, _, _, _, err := registry.Reserve(existing.ID)
	require.NoError(t, err)
	// Seed an active proxy alias in the shared routing namespace.
	sess := registry.apps[existing.ID].Sessions[sid]
	sess.Proxies["app-aeaqcaib"] = sessionProxy{target: sess.target}
	registry.proxies["app-aeaqcaib"] = sess
	var entropy bytes.Buffer
	for _, candidate := range []byte{0, 1, 2, 3, 3, 4} {
		entropy.Write(bytes.Repeat([]byte{candidate}, 5))
	}
	original := rand.Reader
	defer func() { rand.Reader = original }()
	rand.Reader = io.MultiReader(bytes.NewReader(entropy.Bytes()), original)
	response := appHeartbeat(t, registry, owner.RunnerID, owner.HeartbeatSecret, registeredApp(""), registeredApp("app-aibaeaqc"), registeredApp(""))
	require.Len(t, response.Apps, 4)
	require.Equal(t, []string{"app-ambqgayd", "app-aibaeaqc", "app-aqcaibae", existing.ID}, []string{response.Apps[0].ID, response.Apps[1].ID, response.Apps[2].ID, response.Apps[3].ID})
	entropy.Write(bytes.Repeat([]byte{5}, 5))
	rand.Reader = io.MultiReader(bytes.NewReader(entropy.Bytes()), original)
	response, code, err := registry.Heartbeat(heartbeatRequest{App: existing.App, RunnerURL: existing.RunnerURL}, "bootstrap")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "app-aucqkbif", response.RunnerID)
}

func TestAppIDsCannotShadowGeneratedProxies(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(fmt.Sprintf("automatic=%t", automatic), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, r.URL.Path) }))
			defer upstream.Close()
			registry, server := reviewServer(t, upstream.URL)
			defer server.Close()
			victim := registeredApp("victim")
			victim.Proxy, victim.RunnerURL = automatic, upstream.URL+"/victim"
			owner := appHeartbeat(t, registry, "owner-a", "bootstrap", victim)
			appHeartbeat(t, registry, "owner-a", owner.HeartbeatSecret, victim)
			other := appHeartbeat(t, registry, "owner-b", "bootstrap")
			sid, proxyURL, _, _, err := registry.Reserve("victim")
			require.NoError(t, err)
			if !automatic {
				target, token, _, err := registry.sessionTarget("victim", sid)
				require.NoError(t, err)
				id, err := registry.addProxy("victim", sid, token, target)
				require.NoError(t, err)
				proxyURL = registry.proxyURL(id)
			}
			id := strings.TrimPrefix(proxyURL, registry.service+"/run/")
			hijacker := registeredApp(id)
			hijacker.Mode, hijacker.Proxy, hijacker.RunnerURL = "single-shot", true, upstream.URL+"/other-owner"
			w := heartbeatHTTP(t, server, map[string]any{"runner_id": "owner-b", "apps": []appRequest{hijacker}}, other.HeartbeatSecret)
			require.Equal(t, 409, w.Code, w.Body.String())
			_, status, err := registry.Heartbeat(heartbeatRequest{RunnerID: id, RunnerURL: upstream.URL, App: "legacy"}, "bootstrap")
			require.Error(t, err)
			require.Equal(t, 409, status)
			require.Error(t, registry.AddStatic(StaticRunner{ID: id, RunnerURL: upstream.URL, App: "static"}))
			w = httptest.NewRecorder()
			server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, proxyURL, nil))
			require.Equal(t, 200, w.Code, w.Body.String())
			require.Equal(t, "/victim/", w.Body.String())
			registry.ReleaseBySession(sid)
			appHeartbeat(t, registry, "owner-b", other.HeartbeatSecret, hijacker)
			w = httptest.NewRecorder()
			server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, proxyURL, nil))
			require.Equal(t, 200, w.Code, w.Body.String())
			require.Equal(t, "/other-owner/", w.Body.String())
		})
	}
}
