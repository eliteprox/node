package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ownerSeq(n uint64) *uint64 { return &n }

func ownerHeartbeatHTTP(t *testing.T, server *Server, req heartbeatRequest, credential string) *httptest.ResponseRecorder {
	t.Helper()
	body := map[string]any{"runner_id": req.RunnerID, "version": req.Version, "apps": req.Apps}
	if req.Seq != nil {
		body["seq"] = *req.Seq
	}
	return heartbeatHTTP(t, server, body, credential)
}

func TestOwnerSeqOrdering(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	a := registeredApp("a")
	request := heartbeatRequest{RunnerID: "owner", Seq: ownerSeq(100), Apps: []appRequest{a}}
	initial := ownerHeartbeatHTTP(t, server, request, "bootstrap")
	require.Equal(t, 200, initial.Code, initial.Body.String())
	var owner heartbeatResponse
	require.NoError(t, json.Unmarshal(initial.Body.Bytes(), &owner))
	require.Nil(t, owner.Seq, "bootstrap cannot opt an issued credential into ordering")
	request.Seq = nil
	for range 2 {
		w := ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
		require.Equal(t, 200, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), `"seq"`)
	}
	lastLegacy := registry.runners["owner"].Last
	for _, seq := range []string{`-1`, `1.5`, `"1"`, `18446744073709551616`, `true`, `[]`} {
		req := httptest.NewRequest(http.MethodPost, "/runners/heartbeat", strings.NewReader(`{"runner_id":"owner","seq":`+seq+`,"apps":[]}`))
		req.Header.Set("Authorization", owner.HeartbeatSecret)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		require.Equal(t, 400, w.Code, w.Body.String())
		require.Nil(t, registry.runners["owner"].Seq)
		require.Equal(t, lastLegacy, registry.runners["owner"].Last)
		require.Contains(t, registry.apps, "a")
	}
	request.Seq = ownerSeq(0)
	w := ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"seq":0`, "zero enables ordering")
	request.Seq = nil
	w = ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
	require.Equal(t, 409, w.Code, w.Body.String())
	request.Seq = ownerSeq(5)
	w = ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"seq":5`)
	last := registry.runners["owner"].Last
	for _, seq := range []*uint64{nil, ownerSeq(0), ownerSeq(4), ownerSeq(5)} {
		request.Seq, request.Version = seq, "rejected"
		request.Apps[0].Status = "draining"
		w := ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
		require.Equal(t, 409, w.Code, w.Body.String())
		require.JSONEq(t, `{"error":"seq must increase after owner opt-in","seq":5}`, w.Body.String())
		require.Equal(t, last, registry.runners["owner"].Last)
		require.Empty(t, registry.runners["owner"].Version)
		require.Equal(t, "ready", registry.apps["a"].Status)
	}
	other := appHeartbeat(t, registry, "other", "bootstrap", registeredApp("foreign"))
	request.Seq = ownerSeq(50)
	for _, credential := range []string{"", "invalid", "bootstrap", other.HeartbeatSecret} {
		w = ownerHeartbeatHTTP(t, server, request, credential)
		require.Equal(t, 401, w.Code, w.Body.String())
		require.EqualValues(t, 5, *registry.runners["owner"].Seq)
		require.Equal(t, last, registry.runners["owner"].Last)
	}
	request.Apps[0] = registeredApp("foreign")
	w = ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
	require.Equal(t, 409, w.Code)
	require.JSONEq(t, `{"error":"app id belongs to another runner"}`, w.Body.String())
	require.EqualValues(t, 5, *registry.runners["owner"].Seq)
	require.Equal(t, last, registry.runners["owner"].Last)
	require.Equal(t, "ready", registry.apps["a"].Status)
	require.Same(t, registry.runners["owner"], registry.apps["a"].owner)
	require.Same(t, registry.runners["other"], registry.apps["foreign"].owner)
	request.Apps[0], request.Seq = a, ownerSeq(9)
	w = ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.EqualValues(t, 9, *registry.runners["owner"].Seq, "sequence gaps are allowed")
}

func TestHeartbeatSeqWithdrawalAfterRegistrarRestart(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	a, b := registeredApp("a"), registeredApp("b")
	w := ownerHeartbeatHTTP(t, server, heartbeatRequest{RunnerID: "owner", Apps: []appRequest{a, b}}, "bootstrap")
	require.Equal(t, 200, w.Code, w.Body.String())
	var saved heartbeatResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &saved))
	ready := heartbeatRequest{RunnerID: saved.RunnerID, Apps: []appRequest{a, b}, Seq: ownerSeq(1)}
	w = ownerHeartbeatHTTP(t, server, ready, saved.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	sidA, _, _, _, err := registry.Reserve("a")
	require.NoError(t, err)
	sidB, _, _, _, err := registry.Reserve("b")
	require.NoError(t, err)
	ctxA, _ := registry.sessionContext("a", sidA)
	ctxB, _ := registry.sessionContext("b", sidB)
	// A restarted registrar withdraws using its saved identity and a newer sequence.
	ready.Seq = ownerSeq(2)
	withdrawn := heartbeatRequest{RunnerID: saved.RunnerID, Apps: []appRequest{b}, Seq: ownerSeq(3)}
	w = ownerHeartbeatHTTP(t, server, withdrawn, saved.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	var observed heartbeatResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &observed))
	require.Equal(t, []appResponse{{ID: "b", Status: "ready", SessionIDs: []string{sidB}}, {ID: "a", Status: "draining", SessionIDs: []string{sidA}}}, observed.Apps)
	last := registry.runners[saved.RunnerID].Last
	w = ownerHeartbeatHTTP(t, server, ready, saved.HeartbeatSecret)
	require.Equal(t, 409, w.Code, w.Body.String())
	require.True(t, registry.apps["a"].Removing)
	require.NoError(t, ctxA.Err(), "withdrawal preserves admitted work")
	require.NoError(t, ctxB.Err())
	_, _, _, status, err := registry.Reserve("a")
	require.Error(t, err)
	require.Equal(t, 404, status)
	// Normal completion removes the withdrawn registration and its session resources.
	registry.ReleaseBySession(sidA, "completed")
	require.ErrorIs(t, ctxA.Err(), context.Canceled)
	require.NotContains(t, registry.apps, "a")
	w = ownerHeartbeatHTTP(t, server, ready, saved.HeartbeatSecret)
	require.Equal(t, 409, w.Code)
	require.NotContains(t, registry.apps, "a", "a stale ready request cannot revive a completed registration")
	require.Equal(t, last, registry.runners[saved.RunnerID].Last)
	withdrawn.Seq = ownerSeq(5)
	w = ownerHeartbeatHTTP(t, server, withdrawn, saved.HeartbeatSecret)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &observed))
	require.Equal(t, []appResponse{{ID: "b", Status: "ready", SessionIDs: []string{sidB}}}, observed.Apps)
	require.NoError(t, ctxB.Err())
}

func TestOwnerSeqScopeReset(t *testing.T) {
	for _, reset := range []string{"expired", "unregistered", "restart"} {
		t.Run(reset, func(t *testing.T) {
			registry, server := reviewServer(t, "http://127.0.0.1:1")
			defer func() { server.Close() }()
			request := heartbeatRequest{RunnerID: "owner", Apps: []appRequest{registeredApp("a")}}
			owner, _, err := registry.Heartbeat(request, "bootstrap")
			require.NoError(t, err)
			request.Seq = ownerSeq(8)
			_, _, err = registry.Heartbeat(request, owner.HeartbeatSecret)
			require.NoError(t, err)
			sid, _, _, _, err := registry.Reserve("a")
			require.NoError(t, err)
			ctx, _ := registry.sessionContext("a", sid)
			switch reset {
			case "expired":
				registry.runners["owner"].Last = time.Now().Add(-2 * time.Minute)
				registry.Expire()
			case "unregistered":
				_, err = registry.Unregister("owner", owner.HeartbeatSecret)
				require.NoError(t, err)
			case "restart":
				server.Close()
				registry, server = reviewServer(t, "http://127.0.0.1:1")
			}
			require.Error(t, ctx.Err())
			w := ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
			require.Equal(t, 401, w.Code)
			replacement, _, err := registry.Heartbeat(request, "bootstrap")
			require.NoError(t, err)
			require.NotEqual(t, owner.HeartbeatSecret, replacement.HeartbeatSecret)
			require.Nil(t, replacement.Seq)
			w = ownerHeartbeatHTTP(t, server, request, owner.HeartbeatSecret)
			require.Equal(t, 401, w.Code)
			request.Seq = ownerSeq(1)
			w = ownerHeartbeatHTTP(t, server, request, replacement.HeartbeatSecret)
			require.Equal(t, 200, w.Code, w.Body.String())
			require.Empty(t, registry.sessions)
			require.EqualValues(t, 1, *registry.runners["owner"].Seq)
		})
	}
}
