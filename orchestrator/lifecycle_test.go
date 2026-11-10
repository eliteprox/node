package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRunnerStopDrainsProxy(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		delay  time.Duration
		status int
	}{
		{"response", 500 * time.Millisecond, http.StatusNoContent},
		{"deadline", time.Second, http.StatusBadGateway},
		{"client stop", 0, http.StatusBadGateway},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				registry, server := reviewServer(t, "http://127.0.0.1:1")
				defer server.Close()
				require.NoError(t, registry.AddStatic(StaticRunner{ID: "r", RunnerURL: "http://127.0.0.1:1", App: "app"}))
				sid, appURL, _, _, err := registry.Reserve("r")
				require.NoError(t, err)
				target, token, _, err := registry.sessionTarget("r", sid)
				require.NoError(t, err)
				started, finish := make(chan struct{}), make(chan struct{})
				done := make(chan time.Time, 1)
				transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
					close(started)
					select {
					case <-finish:
						return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				})
				w := httptest.NewRecorder()
				go func() {
					registry.metrics.HTTP(w, httptest.NewRequest(http.MethodPost, appURL, nil), func(w http.ResponseWriter, r *http.Request) {
						observability.App(r, registry.identity("r", sid))
						server.proxy(w, r, transport, target, "", "r", sid, token, "")
					})
					done <- time.Now()
				}()
				<-started
				prefix := "/runner/"
				if scenario.name == "client stop" {
					prefix = "/apps/"
				}
				stop := httptest.NewRequest(http.MethodPost, prefix+"r/session/"+sid+"/stop", nil)
				stop.Header.Set("Livepeer-Session-Token", token)
				stopped := httptest.NewRecorder()
				released := time.Now()
				server.ServeHTTP(stopped, stop)
				require.Equal(t, http.StatusOK, stopped.Code)
				_, sessions := registry.Counts()
				require.Zero(t, sessions, "capacity must be released before draining finishes")
				if scenario.name == "response" {
					time.AfterFunc(scenario.delay, func() { close(finish) })
				}
				require.Equal(t, scenario.delay, (<-done).Sub(released))
				require.Equal(t, scenario.status, w.Code)
				require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"outcome": "session_stopped"}))
			})
		})
	}
}

func TestReleaseCancelsActiveProxy(t *testing.T) {
	for _, mode := range []string{"persistent", "single-shot"} {
		for _, protocol := range []string{"http", "sse", "websocket"} {
			for _, issuer := range []string{"client", "shutdown", "expired", "health", "unregistered"} {
				t.Run(mode+"/"+protocol+"/"+issuer, func(t *testing.T) {
					started := make(chan http.Header, 1)
					ended := make(chan struct{})
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						defer close(ended)
						started <- r.Header.Clone()
						switch protocol {
						case "websocket":
							upgrader := websocket.Upgrader{}
							conn, err := upgrader.Upgrade(w, r, nil)
							if err != nil {
								return
							}
							defer conn.Close()
							_, _, _ = conn.ReadMessage()
						case "sse":
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, "data: started\n\n")
							w.(http.Flusher).Flush()
							<-r.Context().Done()
						default:
							<-r.Context().Done()
						}
					}))
					defer upstream.Close()
					policy, err := destination.New("runner", []string{strings.TrimPrefix(upstream.URL, "http://")})
					require.NoError(t, err)
					registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
					var credential string
					if issuer == "expired" || issuer == "unregistered" {
						registration := heartbeatRequest{RunnerID: "runner", RunnerURL: upstream.URL, App: "test", Mode: mode, Capacity: 1}
						registered, _, err := registry.Heartbeat(registration, "bootstrap")
						require.NoError(t, err)
						registration.RunnerID = registered.RunnerID
						_, _, err = registry.Heartbeat(registration, registered.HeartbeatSecret)
						require.NoError(t, err)
						credential = registered.HeartbeatSecret
					} else {
						require.NoError(t, registry.AddStatic(StaticRunner{ID: "runner", RunnerURL: upstream.URL, App: "test", Mode: mode, Capacity: 1}))
					}
					app := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
					server := httptest.NewServer(app)
					defer server.Close()
					defer app.Close()
					path := "/apps/runner/app"
					if mode == "persistent" {
						sid, _, _, _, err := registry.Reserve("runner")
						require.NoError(t, err)
						path = "/apps/runner/session/" + sid + "/app"
					}
					clientDone, headersReady := make(chan struct{}), make(chan struct{})
					if protocol == "websocket" {
						conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+path, nil)
						require.NoError(t, err)
						defer conn.Close()
						go func() { defer close(clientDone); _, _, _ = conn.ReadMessage() }()
					} else {
						go func() {
							defer close(clientDone)
							response, err := http.Get(server.URL + path)
							if err == nil {
								close(headersReady)
								defer response.Body.Close()
								_, _ = io.Copy(io.Discard, response.Body)
							}
						}()
					}
					var headers http.Header
					select {
					case headers = <-started:
					case <-time.After(3 * time.Second):
						t.Fatal("upstream did not start")
					}
					sid := headers.Get("Livepeer-Session-Id")
					require.NotEmpty(t, sid)
					require.NotEmpty(t, headers.Get("Livepeer-Session-Token"))
					if protocol == "sse" {
						select {
						case <-headersReady:
						case <-time.After(3 * time.Second):
							t.Fatal("SSE headers did not arrive")
						}
					}
					reason, outcome := "client_stop", "session_stopped"
					switch issuer {
					case "shutdown":
						app.Close()
						reason = "shutdown"
					case "expired":
						registry.mu.Lock()
						registry.runners["runner"].Last = time.Now().Add(-2 * time.Minute)
						registry.mu.Unlock()
						registry.Expire()
						reason, outcome = "runner_expired", "transport_error"
					case "health":
						registry.mu.Lock()
						registry.apps["runner"].HealthURL = upstream.URL + "/healthz"
						registry.mu.Unlock()
						health := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
							return &http.Response{StatusCode: 503, Body: http.NoBody}, nil
						})}
						for range 3 {
							registry.CheckStaticHealth(t.Context(), health)
						}
						reason, outcome = "runner_unhealthy", "transport_error"
					case "unregistered":
						_, err := registry.Unregister("runner", credential)
						require.NoError(t, err)
						reason = "runner_unregistered"
					default:
						response, err := http.Post(server.URL+"/apps/runner/session/"+sid+"/stop", "", nil)
						require.NoError(t, err)
						require.Equal(t, http.StatusOK, response.StatusCode)
						require.NoError(t, response.Body.Close())
					}

					select {
					case <-ended:
					case <-time.After(3 * time.Second):
						t.Fatal("release did not cancel upstream")
					}
					select {
					case <-clientDone:
					case <-time.After(3 * time.Second):
						t.Fatal("release did not close client stream")
					}
					_, sessions := registry.Counts()
					require.Zero(t, sessions)
					require.Eventually(t, func() bool { return test.MetricSum(t, registry.metrics.Registry, "app_calls_total", nil) == 1 }, time.Second, time.Millisecond)
					require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"outcome": outcome}))
					require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "session_ends_total", map[string]string{"reason": reason}))
					wantErrors := 0
					if outcome == "transport_error" {
						wantErrors = 1
					}
					require.EqualValues(t, wantErrors, test.MetricSum(t, registry.metrics.Registry, "stream_errors_total", nil))
					require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "app_calls_in_flight", nil))
				})
			}
		}
	}
}

func TestRegistryTeardownCancelsSessions(t *testing.T) {
	for _, reason := range []string{"release", "expired", "unregistered", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				registry := NewRegistry("bootstrap", "http://orch.example", time.Second, 3*time.Second)
				registration := heartbeatRequest{RunnerURL: "https://runner.example", App: "test", Capacity: 1}
				registered, _, err := registry.Heartbeat(registration, "bootstrap")
				require.NoError(t, err)
				registration.RunnerID = registered.RunnerID
				_, _, err = registry.Heartbeat(registration, registered.HeartbeatSecret)
				require.NoError(t, err)
				sid, _, _, _, err := registry.Reserve(registered.RunnerID)
				require.NoError(t, err)
				ctx, ok := registry.sessionContext(registered.RunnerID, sid)
				require.True(t, ok)
				require.Len(t, registry.Discovery()[0].Runners, 1)
				switch reason {
				case "release":
					registry.ReleaseBySession(sid)
				case "expired":
					synctest.Sleep(4 * time.Second)
					registry.Expire()
				case "unregistered":
					_, err = registry.Unregister(registered.RunnerID, registered.HeartbeatSecret)
					require.NoError(t, err)
				case "shutdown":
					registry.Close()
				}
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				if reason != "release" {
					require.Empty(t, registry.Discovery()[0].Runners)
				}
			})
		})
	}
}

func TestHealthFailureAndExhaustionCleanUpSession(t *testing.T) {
	for _, reason := range []string{"health", "exhausted"} {
		t.Run(reason, func(t *testing.T) {
			reg, app := reviewServer(t, "http://127.0.0.1:1")
			defer app.Close()
			reg.SetWeiPerUSD(big.NewRat(3600, 1))
			require.NoError(t, reg.AddStatic(StaticRunner{ID: "r", RunnerURL: "http://127.0.0.1:1", App: "test", PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "hour"}}))
			sid, _, _, _, err := reg.Reserve("r")
			require.NoError(t, err)
			ctx, ok := reg.sessionContext("r", sid)
			require.True(t, ok)
			app.newChannel("owned", "events", "application/octet-stream", "r", "r", sid)
			if reason == "health" {
				reg.apps["r"].HealthURL = "http://health.example/healthz"
				health := &http.Client{}
				for _, code := range []int{503, 503, 200, 503, 503} {
					health.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: code, Body: http.NoBody}, nil
					})
					reg.CheckStaticHealth(t.Context(), health)
					require.NoError(t, ctx.Err())
					require.Len(t, reg.Discovery()[0].Runners, 1)
				}
				reg.CheckStaticHealth(t.Context(), health)
				require.Empty(t, reg.Discovery()[0].Runners)
			} else {
				store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
				require.NoError(t, err)
				defer store.Close()
				key := paymentTestKey(t)
				engine, err := NewPaymentEngine(store, paymentTestChain{}, key.Address(), big.NewInt(10), big.NewInt(1))
				require.NoError(t, err)
				_, err = engine.MakeChallenge(t.Context(), "r", sid, key.Address(), 1, "seconds", "https://orch.example")
				require.NoError(t, err)
				require.NoError(t, engine.Charge(t.Context(), sid, time.Now().Add(-2*time.Second)))
				app.SetPayment(engine)
				app.ChargePaidSessions(t.Context())
			}
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.Empty(t, app.channels)
			_, sessions := reg.Counts()
			require.Zero(t, sessions)
		})
	}
}

func TestMultiAppRemovalKeepsStreams(t *testing.T) {
	for _, mode := range []string{"persistent", "single-shot"} {
		for _, protocol := range []string{"http", "sse", "websocket"} {
			t.Run(mode+"/"+protocol, func(t *testing.T) {
				started := make(chan string, 1)
				finish := make(chan struct{})
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					started <- r.Header.Get("Livepeer-Session-Id")
					if protocol == "websocket" {
						conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer conn.Close()
						kind, data, err := conn.ReadMessage()
						if err == nil {
							_ = conn.WriteMessage(kind, data)
						}
						return
					}
					if protocol == "sse" {
						w.Header().Set("Content-Type", "text/event-stream")
					}
					_, _ = io.WriteString(w, "started\n")
					w.(http.Flusher).Flush()
					select {
					case <-finish:
						_, _ = io.WriteString(w, "finished\n")
					case <-r.Context().Done():
					}
				}))
				defer upstream.Close()
				registry, appServer := reviewServer(t, upstream.URL)
				server := httptest.NewServer(appServer)
				defer server.Close()
				defer appServer.Close()
				a, b := registeredApp("a"), registeredApp("b")
				a.RunnerURL, a.Mode = upstream.URL, mode
				owner := appHeartbeat(t, registry, "owner", "bootstrap", a, b)
				appHeartbeat(t, registry, owner.RunnerID, owner.HeartbeatSecret, a, b)
				path := "/apps/a/app"
				if mode == "persistent" {
					sid, _, _, _, err := registry.Reserve("a")
					require.NoError(t, err)
					path = "/apps/a/session/" + sid + "/app"
				}
				var conn *websocket.Conn
				var response *http.Response
				var err error
				if protocol == "websocket" {
					conn, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+path, nil)
					require.NoError(t, err)
					defer conn.Close()
					require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
				} else {
					response, err = server.Client().Get(server.URL + path)
					require.NoError(t, err)
					defer response.Body.Close()
					require.Equal(t, 200, response.StatusCode)
				}
				var sid string
				select {
				case sid = <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("upstream did not start")
				}
				ctx, ok := registry.sessionContext("a", sid)
				require.True(t, ok)
				a.Status = "draining"
				appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, a, b)
				_, _, _, _, err = registry.Reserve("a")
				require.Error(t, err)
				appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, b)
				require.NoError(t, ctx.Err())
				_, _, _, _, err = registry.Reserve("b")
				require.NoError(t, err, "sibling admission continues")
				require.Len(t, registry.Discovery()[0].Runners, 1)
				if mode == "single-shot" {
					rejected, err := server.Client().Get(server.URL + "/apps/a/app")
					require.NoError(t, err)
					require.Equal(t, 404, rejected.StatusCode)
					require.NoError(t, rejected.Body.Close())
				}
				if protocol == "websocket" {
					require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte("still connected")))
					_, data, err := conn.ReadMessage()
					require.NoError(t, err)
					require.Equal(t, "still connected", string(data))
					require.NoError(t, conn.Close())
				} else {
					close(finish)
					body, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					require.Equal(t, "started\nfinished\n", string(body))
				}
				if mode == "persistent" {
					require.NoError(t, ctx.Err(), "request completion does not release a persistent reservation")
					registry.ReleaseBySession(sid)
				}
				require.Eventually(t, func() bool {
					registry.mu.Lock()
					defer registry.mu.Unlock()
					return registry.apps["a"] == nil
				}, 3*time.Second, time.Millisecond)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				registry.mu.Lock()
				require.Contains(t, registry.apps, "b")
				registry.mu.Unlock()
			})
		}
	}
}
