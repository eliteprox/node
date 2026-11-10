package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/stretchr/testify/require"
)

// Ported from e2e-scope's Python multi-track media round-trip test. It tests
// the protocol through the pinned Python runner SDK and the public trickle URL.
func TestPinnedPythonMultiTrackTrickle(t *testing.T) {
	if testing.Short() {
		t.Skip("Python media integration test")
	}
	python, export, root := pinnedPythonSDK(t)
	available := exec.Command(python, "-c", "import av, numpy")
	if err := available.Run(); err != nil {
		if os.Getenv("PYTHON_RUNNER_SDK_DIR") != "" {
			t.Fatalf("configured Python media dependencies unavailable: %v", err)
		}
		t.Skip("Python media dependencies unavailable")
	}
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer runner.Close()
	registry := NewRegistry("bootstrap", "http://127.0.0.1:0", time.Second, 30*time.Second)
	registration := heartbeatRequest{RunnerID: "media-runner", RunnerURL: runner.URL, App: "media", Mode: "persistent", Capacity: 1}
	registered, status, err := registry.Heartbeat(registration, "bootstrap")
	require.NoError(t, err)
	_, _, err = registry.Heartbeat(registration, registered.HeartbeatSecret)
	require.NoError(t, err)
	require.Equal(t, 200, status)
	policy, err := destination.New("runner", []string{strings.TrimPrefix(runner.URL, "http://")})
	require.NoError(t, err)
	server := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	orch := httptest.NewUnstartedServer(server)
	defer orch.Close()
	registry.service = "http://" + orch.Listener.Addr().String()
	orch.Start()
	manifest, _, _, status, err := registry.Reserve("media-runner")
	require.NoError(t, err)
	require.Equal(t, 200, status)
	_, token, status, err := registry.sessionTarget("media-runner", manifest)
	require.NoError(t, err)
	require.Equal(t, 200, status)
	body := bytes.NewBufferString(`{"channels":[{"name":"media","mime_type":"video/MP2T"}]}`)
	request, err := http.NewRequest(http.MethodPost, orch.URL+"/runner/media-runner/session/"+manifest+"/channels", body)
	require.NoError(t, err)
	request.Header.Set("Livepeer-Session-Token", token)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	var created struct {
		Channels []struct {
			URL string `json:"url"`
		} `json:"channels"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&created))
	require.Len(t, created.Channels, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	fixture := filepath.Join(root, "cmd", "livepeer", "testdata", "python_multi_track_media_round_trip.py")
	command := exec.CommandContext(ctx, python, fixture, created.Channels[0].URL)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(export, "src"))
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	var result struct {
		VerificationOK      bool     `json:"verification_ok"`
		Errors              []string `json:"errors"`
		ObservedVideoTracks int      `json:"observed_video_tracks"`
		ObservedAudioTracks int      `json:"observed_audio_tracks"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(output), &result), string(output))
	require.True(t, result.VerificationOK, "media errors: %v", result.Errors)
	require.Equal(t, 2, result.ObservedVideoTracks)
	require.Equal(t, 2, result.ObservedAudioTracks)
}
