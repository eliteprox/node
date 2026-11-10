package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestAppDrainDeadline(t *testing.T) {
	for _, timeout := range []time.Duration{12 * time.Hour, 10 * time.Second, 0} {
		t.Run(timeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				registry, server := reviewServer(t, "http://127.0.0.1:1")
				defer server.Close()
				if timeout != 12*time.Hour {
					registry.drainTimeout = timeout
				}
				registry.ttl = 2 * time.Hour
				a, b, idle := registeredApp("a"), registeredApp("b"), registeredApp("idle")
				a.Proxy, b.Proxy = true, true
				credential, seq := "bootstrap", uint64(0)
				heartbeat := func(apps ...appRequest) heartbeatResponse {
					t.Helper()
					seq++
					w := ownerHeartbeatHTTP(t, server, heartbeatRequest{RunnerID: "owner", Seq: ownerSeq(seq), Apps: append([]appRequest{}, apps...)}, credential)
					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					var response heartbeatResponse
					require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
					return response
				}
				owner := heartbeat(a, b, idle)
				credential = owner.HeartbeatSecret
				heartbeat(a, b, idle)
				stale := heartbeatRequest{RunnerID: "owner", Seq: ownerSeq(seq), Apps: []appRequest{a, b, idle}}
				sid, _, _, _, err := registry.Reserve("a")
				require.NoError(t, err)
				ctx, _ := registry.sessionContext("a", sid)
				quote, _, err := registry.quoteForApp("a")
				require.NoError(t, err)
				_, _, _, _, err = registry.reserveWithPrice("a", "pending", &quote, "persistent")
				require.NoError(t, err)
				pendingCtx, _ := registry.sessionContext("a", "pending")
				sibling, _, _, _, err := registry.Reserve("b")
				require.NoError(t, err)
				siblingCtx, _ := registry.sessionContext("b", sibling)
				server.newChannel("app-channel", "events", "application/json", "owner", "a", sid)
				server.newChannel("sibling-channel", "events", "application/json", "owner", "b", sibling)
				var proxyIDs []string
				for _, sess := range registry.apps["a"].Sessions {
					for proxyID := range sess.Proxies {
						proxyIDs = append(proxyIDs, proxyID)
					}
				}
				deadline := time.Now().Add(timeout)
				observed := heartbeat(b)
				require.NotContains(t, registry.apps, "idle", "empty withdrawals are removed immediately")
				require.Len(t, registry.Discovery()[0].Runners, 1)
				_, _, _, status, err := registry.Reserve("a")
				require.Error(t, err)
				require.Equal(t, http.StatusNotFound, status)
				if timeout > 0 {
					require.Equal(t, []appResponse{{ID: "b", Status: "ready", SessionIDs: []string{sibling}}, {ID: "a", Status: "draining", SessionIDs: []string{"pending", sid}}}, observed.Apps)
					advance := func(duration time.Duration) {
						for duration > 0 {
							step := min(duration, time.Hour)
							time.Sleep(step)
							heartbeat(b)
							registry.Expire()
							duration -= step
						}
					}
					advance(timeout / 2)
					w := ownerHeartbeatHTTP(t, server, stale, credential)
					require.Equal(t, http.StatusConflict, w.Code)
					require.Equal(t, deadline, registry.apps["a"].drainDeadline)
					advance(timeout/2 - time.Nanosecond)
					require.NoError(t, ctx.Err())
					require.NoError(t, pendingCtx.Err())
					require.Contains(t, server.channels, "app-channel")
					time.Sleep(time.Nanosecond)
					registry.Expire() // No further heartbeat or cancellation command.
				}
				require.ErrorIs(t, context.Cause(ctx), errRunnerUnavailable)
				require.ErrorIs(t, context.Cause(pendingCtx), errRunnerUnavailable)
				require.NotContains(t, registry.apps, "a")
				require.NotContains(t, registry.sessions, sid)
				require.NotContains(t, registry.sessions, "pending")
				require.False(t, registry.activateSession("a", "pending"))
				for _, proxyID := range proxyIDs {
					require.NotContains(t, registry.proxies, proxyID)
				}
				require.NotContains(t, server.channels, "app-channel")
				require.Contains(t, server.channels, "sibling-channel")
				require.Contains(t, server.channels, owner.O2R.ChannelName)
				require.NoError(t, siblingCtx.Err())
				require.EqualValues(t, 2, test.MetricSum(t, registry.metrics.Registry, "session_ends_total", map[string]string{"reason": "app_drain_expired"}))
				observed = heartbeat(b)
				require.Equal(t, []appResponse{{ID: "b", Status: "ready", SessionIDs: []string{sibling}}}, observed.Apps)
				require.Equal(t, owner.O2R, observed.O2R)
				registry.ReleaseBySession(sibling)
				require.Equal(t, []appResponse{}, heartbeat().Apps)
			})
		})
	}
}

func TestAppDrainReadmissionAndCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry, server := reviewServer(t, "http://127.0.0.1:1")
		defer server.Close()
		registry.drainTimeout = 10 * time.Second
		a, b := registeredApp("a"), registeredApp("b")
		owner := appHeartbeat(t, registry, "owner", "bootstrap", a, b)
		appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, a, b)
		sid, _, _, _, err := registry.Reserve("a")
		require.NoError(t, err)
		ctx, _ := registry.sessionContext("a", sid)
		appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, b)
		firstDeadline := registry.apps["a"].drainDeadline
		time.Sleep(5 * time.Second)
		appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, a, b)
		require.True(t, registry.apps["a"].drainDeadline.IsZero())
		time.Sleep(6 * time.Second)
		registry.Expire()
		require.NoError(t, ctx.Err(), "readmitted work survives the original deadline")
		appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, b)
		require.True(t, registry.apps["a"].drainDeadline.After(firstDeadline))
		registry.ReleaseBySession(sid, "completed")
		require.NotContains(t, registry.apps, "a", "natural completion removes the registration before expiry")
		require.Equal(t, []appResponse{{ID: "b", Status: "ready", SessionIDs: []string{}}}, appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, b).Apps)
	})
}

func TestAppDrainReplacementAtCapacity(t *testing.T) {
	for _, global := range []bool{false, true} {
		for _, timeout := range []time.Duration{0, time.Second} {
			t.Run(fmt.Sprintf("global=%t/timeout=%s", global, timeout), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					registry := NewRegistry("bootstrap", "http://orch.example", time.Second, time.Minute)
					defer registry.Close()
					registry.drainTimeout = timeout
					count := maxAppsPerRunner
					if global {
						// Leave room in this owner's quota to isolate the global limit.
						count--
						for remaining := maxApps - count; remaining > 0; {
							group := make([]appRequest, min(remaining, maxAppsPerRunner))
							for i := range group {
								group[i] = registeredApp(fmt.Sprintf("fill-%d-%d", remaining, i))
							}
							appHeartbeat(t, registry, fmt.Sprintf("owner-%d", remaining), "bootstrap", group...)
							remaining -= len(group)
						}
					}
					apps := make([]appRequest, count)
					for i := range apps {
						apps[i] = registeredApp(fmt.Sprintf("app-%d", i))
					}
					owner := appHeartbeat(t, registry, "owner", "bootstrap", apps...)
					appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, apps...)
					sid, _, _, _, err := registry.Reserve(apps[0].ID)
					require.NoError(t, err)
					ctx, _ := registry.sessionContext(apps[0].ID, sid)
					if timeout > 0 {
						appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, apps[1:]...)
						time.Sleep(timeout) // Deadline elapsed; no expiry sweep yet.
					}
					replacement := append([]appRequest{registeredApp("replacement")}, apps[1:]...)
					invalid := append([]appRequest{}, replacement...)
					invalid[0].Proxy, invalid[0].Mode = true, "single-shot"
					registry.proxyTemplate = "https://{proxy}.example"
					invalid[0].ID = "invalid_proxy_id"
					last := registry.runners["owner"].Last
					_, status, err := registry.Heartbeat(heartbeatRequest{RunnerID: "owner", Apps: invalid}, owner.HeartbeatSecret)
					require.Error(t, err)
					require.Equal(t, http.StatusBadRequest, status)
					require.NoError(t, ctx.Err(), "failed validation cannot tear down omitted work")
					require.Equal(t, last, registry.runners["owner"].Last)
					appHeartbeat(t, registry, "owner", owner.HeartbeatSecret, replacement...)
					require.ErrorIs(t, context.Cause(ctx), errRunnerUnavailable)
					require.NotContains(t, registry.apps, apps[0].ID)
					require.Contains(t, registry.apps, "replacement")
					require.Len(t, registry.runners["owner"].Apps, count)
					if global {
						require.Len(t, registry.apps, maxApps)
					}
				})
			})
		}
	}
}
