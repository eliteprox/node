package orchestrator

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDynamicRunnerConfirmation(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	a, b := registeredApp("a"), registeredApp("b")
	w := heartbeatHTTP(t, server, map[string]any{"runner_id": "owner", "apps": []appRequest{a, b}}, "bootstrap")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var initial heartbeatResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &initial))
	require.Equal(t, "owner", initial.RunnerID)
	require.NotEmpty(t, initial.HeartbeatSecret)
	require.Equal(t, time.Second.String(), initial.HeartbeatInterval)
	require.Equal(t, time.Minute.String(), initial.HeartbeatTTL)
	require.NotNil(t, initial.O2R)
	require.Equal(t, []appResponse{
		{ID: "a", Status: "ready", SessionIDs: []string{}},
		{ID: "b", Status: "ready", SessionIDs: []string{}},
	}, initial.Apps)
	require.Empty(t, registry.Discovery()[0].Runners)

	other := appHeartbeat(t, registry, "other", "bootstrap", registeredApp("foreign"))
	invalid := a
	invalid.PriceInfo = priceInfo{Price: "-1", Currency: "usd", Unit: "fixed"}
	before := registry.runners["owner"].Last
	for _, tc := range []struct {
		name, credential string
		apps             []appRequest
		status           int
	}{
		{"bootstrap", "bootstrap", []appRequest{a, b}, http.StatusUnauthorized},
		{"invalid credential", "wrong", []appRequest{a, b}, http.StatusUnauthorized},
		{"foreign credential", other.HeartbeatSecret, []appRequest{a, b}, http.StatusUnauthorized},
		{"duplicate app", initial.HeartbeatSecret, []appRequest{a, a}, http.StatusBadRequest},
		{"invalid price", initial.HeartbeatSecret, []appRequest{b, invalid}, http.StatusBadRequest},
		{"foreign app", initial.HeartbeatSecret, []appRequest{b, registeredApp("foreign")}, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := heartbeatHTTP(t, server, map[string]any{"runner_id": "owner", "apps": tc.apps}, tc.credential)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.False(t, registry.runners["owner"].Confirmed)
			require.Equal(t, before, registry.runners["owner"].Last)
			require.Len(t, registry.runners["owner"].Apps, 2)
			require.Empty(t, registry.Discovery()[0].Runners)
		})
	}

	// Readiness follows the current app snapshot.
	a.Status = "draining"
	confirmed := appHeartbeat(t, registry, "owner", initial.HeartbeatSecret, a, b)
	require.Empty(t, confirmed.HeartbeatSecret)
	require.True(t, registry.runners["owner"].Confirmed)
	inventory := registry.Discovery()[0].Runners
	require.Len(t, inventory, 1)
	require.Contains(t, inventory[0].URL, "/apps/b/")
	_, _, _, status, err := registry.Reserve("a")
	require.Error(t, err)
	require.Equal(t, http.StatusNotFound, status)
	sid, _, _, _, err := registry.Reserve("b")
	require.NoError(t, err)
	response := appHeartbeat(t, registry, "owner", initial.HeartbeatSecret, b)
	require.Equal(t, []string{sid}, response.Apps[0].SessionIDs)
}

func TestEmptySnapshotConfirmsRunner(t *testing.T) {
	for _, initiallyEmpty := range []bool{false, true} {
		t.Run(map[bool]string{false: "registered apps", true: "empty registration"}[initiallyEmpty], func(t *testing.T) {
			registry, server := reviewServer(t, "http://127.0.0.1:1")
			defer server.Close()
			var apps []appRequest
			if !initiallyEmpty {
				apps = []appRequest{registeredApp("a")}
			}
			initial := appHeartbeat(t, registry, "owner", "bootstrap", apps...)
			require.False(t, registry.runners["owner"].Confirmed)
			confirmed := appHeartbeat(t, registry, "owner", initial.HeartbeatSecret)
			require.True(t, registry.runners["owner"].Confirmed)
			require.Equal(t, []appResponse{}, confirmed.Apps)
			require.Empty(t, registry.Discovery()[0].Runners)
			appHeartbeat(t, registry, "owner", initial.HeartbeatSecret, registeredApp("a"))
			require.Len(t, registry.Discovery()[0].Runners, 1)
		})
	}
}

func TestLostInitialRegistrationResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry, server := reviewServer(t, "http://127.0.0.1:1")
		defer server.Close()
		// Both attempts omit IDs and point to the same app endpoint. The consumer
		// receives only the second response; the first is inspected for assertions.
		lost := appHeartbeat(t, registry, "", "bootstrap", registeredApp(""))
		fresh := appHeartbeat(t, registry, "", "bootstrap", registeredApp(""))
		require.NotEqual(t, lost.RunnerID, fresh.RunnerID)
		require.NotEqual(t, lost.Apps[0].ID, fresh.Apps[0].ID)
		require.Empty(t, registry.Discovery()[0].Runners)
		app := registeredApp(fresh.Apps[0].ID)
		appHeartbeat(t, registry, fresh.RunnerID, fresh.HeartbeatSecret, app)
		inventory := registry.Discovery()[0].Runners
		require.Len(t, inventory, 1)
		require.Equal(t, app.Capacity, inventory[0].CapacityAvailable)
		require.Contains(t, inventory[0].URL, fresh.Apps[0].ID)
		_, _, _, status, err := registry.Reserve(lost.Apps[0].ID)
		require.Error(t, err)
		require.Equal(t, http.StatusNotFound, status)
		sid, _, _, _, err := registry.Reserve(fresh.Apps[0].ID)
		require.NoError(t, err)
		time.Sleep(registry.ttl / 2)
		appHeartbeat(t, registry, fresh.RunnerID, fresh.HeartbeatSecret, app)
		time.Sleep(registry.ttl/2 + time.Second)
		registry.Expire()
		require.NotContains(t, registry.runners, lost.RunnerID)
		require.NotContains(t, registry.apps, lost.Apps[0].ID)
		require.Contains(t, registry.sessions, sid)
		require.Len(t, registry.Discovery()[0].Runners, 1)
	})
}
