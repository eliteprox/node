package orchestrator

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/stretchr/testify/require"
)

func TestRunnerProxyRoutingMatrix(t *testing.T) {
	for _, static := range []bool{false, true} {
		for _, mode := range []string{"persistent", "single-shot"} {
			for _, proxy := range []bool{false, true} {
				for _, template := range []string{"", "https://{proxy}.apps.example", "https://orch.example/custom/{proxy}"} {
					t.Run(fmt.Sprintf("static=%t/%s/proxy=%t/%s", static, mode, proxy, template), func(t *testing.T) {
						upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							require.Equal(t, "/base/test/price%25.txt", r.URL.Path)
							require.Equal(t, "x=1", r.URL.RawQuery)
							require.NotEmpty(t, r.Header.Get("Livepeer-Session-Id"))
							require.NotEqual(t, "forged", r.Header.Get("Livepeer-Session-Token"))
							_, _ = io.WriteString(w, "ok")
						}))
						defer upstream.Close()
						policy, err := destination.New("runner", []string{strings.TrimPrefix(upstream.URL, "http://")})
						require.NoError(t, err)
						denied, err := destination.New("session-proxy", nil)
						require.NoError(t, err)
						registry := NewRegistry("secret", "https://orch.example/deployment", time.Second, time.Minute)
						registry.proxyTemplate = template
						s := NewServer(registry, policy, denied, slog.New(slog.NewTextHandler(io.Discard, nil)))
						defer s.Close()
						runnerID := "runner"
						if static {
							require.NoError(t, registry.AddStatic(StaticRunner{ID: "runner", RunnerURL: upstream.URL + "/base", App: "app", Mode: mode, Proxy: proxy}))
						} else {
							req := heartbeatRequest{RunnerURL: upstream.URL + "/base", App: "app", Mode: mode, Proxy: proxy}
							registered, _, err := registry.Heartbeat(req, "secret")
							require.NoError(t, err)
							runnerID, req.RunnerID = registered.RunnerID, registered.RunnerID
							require.Regexp(t, `^app-[a-z2-7]{8}$`, runnerID)
							require.Empty(t, registry.Discovery()[0].Runners)
							for _, path := range []string{
								registry.appURL(runnerID, "persistent", ""),
								registry.appURL(runnerID, "single-shot", ""),
								registry.appURL(runnerID, "persistent", "unknown"),
								registry.service + "/run/" + runnerID,
								registry.service + "/proxy/" + runnerID,
								registry.proxyURL(runnerID),
							} {
								w := httptest.NewRecorder()
								s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
								require.Equal(t, http.StatusNotFound, w.Code, path+": "+w.Body.String())
							}
							require.Empty(t, registry.sessions)
							_, code, err := registry.Heartbeat(req, registered.HeartbeatSecret)
							require.NoError(t, err)
							require.Equal(t, http.StatusOK, code)
						}
						discovery := httptest.NewRecorder()
						s.ServeHTTP(discovery, httptest.NewRequest("GET", "https://orch.example/deployment/discovery", nil))
						require.Equal(t, 200, discovery.Code)
						var entries []discoveryEntry
						require.NoError(t, json.Unmarshal(discovery.Body.Bytes(), &entries))
						app := entries[0].Runners[0].URL
						require.Contains(t, app, runnerID)
						var sid string
						if mode == "persistent" {
							var err error
							sid, app, _, _, err = registry.Reserve(runnerID)
							require.NoError(t, err)
							runners := registry.Discovery()[0].Runners
							require.Len(t, runners, 1)
							require.Equal(t, 1, runners[0].Capacity)
							require.Equal(t, 1, runners[0].CapacityUsed)
							require.Zero(t, runners[0].CapacityAvailable)
						}
						if proxy {
							if template == "" {
								require.Contains(t, app, "/run/")
							} else if strings.Contains(template, ".apps.example") {
								u, err := url.Parse(app)
								require.NoError(t, err)
								require.True(t, strings.HasSuffix(u.Host, ".apps.example"))
							} else {
								require.Contains(t, app, "/custom/")
							}
						}
						req := httptest.NewRequest("POST", app+"/test/price%2525.txt?x=1", strings.NewReader("request"))
						req.Header.Set("Livepeer-Session-Token", "forged")
						w := httptest.NewRecorder()
						s.ServeHTTP(w, req)
						require.Equal(t, 200, w.Code, w.Body.String())
						require.Equal(t, "ok", w.Body.String())
						traversal := httptest.NewRecorder()
						s.ServeHTTP(traversal, httptest.NewRequest("GET", app+"/%2e%2e/%2e%2e/metadata", nil))
						require.Equal(t, http.StatusBadRequest, traversal.Code)
						registry.ReleaseBySession(sid)
						_, sessions := registry.Counts()
						require.Zero(t, sessions)
					})
				}
			}
		}
	}
}

func TestDomainProxyRunnerIDs(t *testing.T) {
	for _, id := range []string{"Uppercase", "under_score", strings.Repeat("a", 64), "-leading", "trailing-"} {
		reg := NewRegistry("bootstrap", "https://orch.example", time.Second, time.Minute)
		reg.proxyTemplate = "https://{proxy}.example"
		req := heartbeatRequest{RunnerID: id, RunnerURL: "https://runner.example", App: "test", Mode: "single-shot", Proxy: true}
		_, code, err := reg.Heartbeat(req, "bootstrap")
		require.Error(t, err)
		require.Equal(t, http.StatusBadRequest, code)
		require.Error(t, reg.AddStatic(StaticRunner{ID: id, RunnerURL: req.RunnerURL, App: req.App, Mode: req.Mode, Proxy: true}))
	}
}
