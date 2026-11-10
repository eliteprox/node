package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func TestRealBinaryOffchainFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("real binary integration test")
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	install := t.TempDir()
	for _, name := range []string{"livepeer", "livepeer-orchestrator", "livepeer-signer", "livepeer-chain", "livepeer-discovery"} {
		build := exec.Command("go", "build", "-o", filepath.Join(install, name), "./cmd/"+name)
		build.Dir = root
		output, err := build.CombinedOutput()
		require.NoError(t, err, string(output))
	}
	dispatcher := filepath.Join(install, "livepeer")
	version, err := exec.Command(dispatcher, "version").CombinedOutput()
	require.NoError(t, err, string(version))
	require.Contains(t, string(version), "livepeer dev")
	help, err := exec.Command(dispatcher, "orchestrator", "--help").CombinedOutput()
	require.NoError(t, err, string(help))
	require.Contains(t, string(help), "bootstrap-secret-file")
	completion, err := exec.Command(dispatcher, "completion", "orchestrator", "bash").CombinedOutput()
	require.NoError(t, err, string(completion))
	require.Contains(t, string(completion), "bash completion")
	for _, component := range []string{"orchestrator", "signer", "chain", "discovery"} {
		direct := filepath.Join(install, "livepeer-"+component)
		for _, arguments := range [][]string{{"--help"}, {"completion", "bash"}, {"completion", "zsh"}, {"completion", "fish"}, {"completion", "powershell"}} {
			actual, err := exec.Command(direct, arguments...).CombinedOutput()
			require.NoError(t, err, string(actual))
			dispatched := append([]string{component}, arguments...)
			if arguments[0] == "completion" {
				dispatched = append([]string{"completion", component}, arguments[1:]...)
			}
			forwarded, err := exec.Command(dispatcher, dispatched...).CombinedOutput()
			require.NoError(t, err, string(forwarded))
			require.Equal(t, string(actual), string(forwarded), "component=%s args=%v", component, arguments)
			golden := filepath.Join(root, "cmd", "livepeer", "testdata", "cli", component+"-"+strings.TrimPrefix(strings.Join(arguments, "-"), "--")+".golden")
			home, err := os.UserHomeDir()
			require.NoError(t, err)
			value := bytes.ReplaceAll(actual, []byte(home), []byte("$HOME"))
			if arguments[0] == "completion" {
				// The generated shell boilerplate is large; pin its complete digest.
				value = fmt.Appendf(nil, "%x\n", sha256.Sum256(value))
			}
			if os.Getenv("UPDATE_CLI_GOLDENS") == "1" {
				require.NoError(t, os.MkdirAll(filepath.Dir(golden), 0755))
				require.NoError(t, os.WriteFile(golden, value, 0644))
			}
			expected, err := os.ReadFile(golden)
			require.NoError(t, err)
			if !bytes.Equal(expected, value) {
				artifact := filepath.Join(t.ArtifactDir(), filepath.Base(golden)+".actual")
				require.NoError(t, os.WriteFile(artifact, actual, 0644))
				t.Errorf("CLI golden differs: %s; full output: %s", golden, artifact)
			}

		}
	}

	testBinaryMigrations(t, install)
	t.Run("discovery dump", func(t *testing.T) { testDiscoveryDumpBinary(t, install) })

	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Livepeer-Session-Token"))
		if r.URL.Path == "/credentials" {
			write := map[string]string{"token": r.Header.Get("Livepeer-Session-Token")}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(write))
			return
		}
		require.Equal(t, "/hello", r.URL.Path)
		_, _ = io.WriteString(w, "from-runner")
	}))
	defer runner.Close()
	mainPort, metricsPort := freePort(t), freePort(t)
	serviceURL := fmt.Sprintf("http://127.0.0.1:%d", mainPort)
	metricsURL := fmt.Sprintf("http://127.0.0.1:%d", metricsPort)
	secretPath := filepath.Join(t.TempDir(), "bootstrap")
	require.NoError(t, os.WriteFile(secretPath, []byte("exact-bootstrap"), 0600))
	cmd := exec.Command(dispatcher, "orchestrator", "--listen", fmt.Sprintf("127.0.0.1:%d", mainPort), "--metrics-listen", fmt.Sprintf("127.0.0.1:%d", metricsPort), "--bootstrap-secret-file", secretPath, "--runner-grants", "127.0.0.0/8", "--session-proxy-grants", strings.TrimPrefix(runner.URL, "http://"), "--drain-timeout", "0")
	var log bytes.Buffer
	cmd.Stdout, cmd.Stderr = &log, &log
	require.NoError(t, cmd.Start())
	processDone := make(chan struct{})
	var processErr error
	go func() { processErr = cmd.Wait(); close(processDone) }()
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-processDone:
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Kill()
				<-processDone
			}
		}
		_ = os.WriteFile(filepath.Join(t.ArtifactDir(), "orchestrator.log"), log.Bytes(), 0644)
	})
	require.Eventually(t, func() bool {
		response, err := http.Get(metricsURL + "/readyz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond)
	request, err := http.NewRequest(http.MethodPost, serviceURL+"/runners/heartbeat", bytes.NewBufferString(`{"runner_url":"`+runner.URL+`","app":"e2e","mode":"persistent","capacity":1,"price_info":{"price":0}}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", "exact-bootstrap")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var registered struct {
		RunnerID        string `json:"runner_id"`
		HeartbeatSecret string `json:"heartbeat_secret"`
		Orchestrator    string `json:"orchestrator"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&registered))
	require.NoError(t, response.Body.Close())
	require.NotEmpty(t, registered.HeartbeatSecret)
	require.Equal(t, serviceURL, registered.Orchestrator)
	request, err = http.NewRequest(http.MethodPost, serviceURL+"/runners/heartbeat", bytes.NewBufferString(`{"runner_id":"`+registered.RunnerID+`","runner_url":"`+runner.URL+`","app":"e2e","mode":"persistent","capacity":1}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", registered.HeartbeatSecret)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response, err = http.Post(serviceURL+"/apps/"+registered.RunnerID+"/session", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var reserved struct {
		SessionID string `json:"session_id"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&reserved))
	require.NoError(t, response.Body.Close())
	response, err = http.Get(serviceURL + "/apps/" + registered.RunnerID + "/session/" + reserved.SessionID + "/app/hello")
	require.NoError(t, err)
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "from-runner", string(data))
	require.NoError(t, response.Body.Close())
	testDiscoveryBinary(t, install, serviceURL)
	request, err = http.NewRequest(http.MethodPost, serviceURL+"/runners/heartbeat", bytes.NewBufferString(`{"runner_id":"`+registered.RunnerID+`","seq":1,"apps":[]}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", registered.HeartbeatSecret)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var withdrawn struct {
		Apps []json.RawMessage `json:"apps"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&withdrawn))
	require.NoError(t, response.Body.Close())
	require.Equal(t, []json.RawMessage{}, withdrawn.Apps, "configured zero timeout removes the active registration immediately")
	response, err = http.Get(serviceURL + "/apps/" + registered.RunnerID + "/session/" + reserved.SessionID + "/app/hello")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	require.NoError(t, response.Body.Close())
	checkGoSDK(t, root, serviceURL, runner.URL)
	checkPythonSDK(t, root, serviceURL, runner.URL)
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	select {
	case <-processDone:
		require.NoError(t, processErr, log.String())
	case <-time.After(5 * time.Second):
		t.Fatal("orchestrator did not shut down")
	}
	cmd.Process = nil
}

func testDiscoveryBinary(t *testing.T, install, orchestratorURL string) {
	t.Helper()
	listen := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	metrics := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	baseURL := "http://" + listen
	logFile, err := os.Create(filepath.Join(t.TempDir(), "discovery.log"))
	require.NoError(t, err)
	defer logFile.Close()
	cmd := exec.Command(filepath.Join(install, "livepeer"), "discovery", "--config=", "--listen", listen,
		"--metrics-listen", strings.TrimPrefix(metrics, "http://"),
		"--nodes", orchestratorURL, "--grants", strings.TrimPrefix(orchestratorURL, "http://"))
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("discovery did not shut down")
		}
	}()
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		response, err := client.Get(metrics + "/readyz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == 200
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		response, err := client.Get(baseURL + "/discovery")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var entries []struct {
			Address string `json:"address"`
			Runners []struct {
				URL string `json:"url"`
			} `json:"runners"`
		}
		if json.NewDecoder(response.Body).Decode(&entries) != nil || len(entries) != 2 {
			return false
		}
		return entries[0].Address == "" && entries[1].Address == orchestratorURL && len(entries[1].Runners) > 0 &&
			strings.HasPrefix(entries[1].Runners[0].URL, orchestratorURL+"/apps/")
	}, 5*time.Second, 20*time.Millisecond)
}

func testDiscoveryDumpBinary(t *testing.T, install string) {
	t.Helper()
	listen, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listen.Close()
	metrics, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer metrics.Close()
	started := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/inventory/discovery":
			_, _ = io.WriteString(w, `[{"address":"https://orch.example","runners":[{"url":"https://orch.example/apps/runner/app","app":"test","observed_at":"2000-01-01T00:00:00Z"}]}]`)
		case "/blocked/a/discovery", "/blocked/b/discovery":
			_, _ = fmt.Fprintf(w, `[{"address":"https://orch.example","runners":[{"url":%q,"app":"test","observed_at":"2000-01-01T00:00:00Z","extension":%q}]}]`, "https://orch.example"+r.URL.Path, strings.Repeat("x", 900<<10))
		case "/failed/discovery":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/cancel/discovery":
			started <- nil
			<-r.Context().Done()
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	for _, entry := range [][]string{{"livepeer-discovery"}, {"livepeer", "discovery"}} {
		t.Run(entry[0], func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				sources    []string
				flags      []string
				entries    int
				exit       int
				diagnostic string
				noOutput   bool
			}{
				{name: "inventory", sources: []string{"inventory"}, entries: 2},
				{name: "partial failure", sources: []string{"inventory", "failed"}, entries: 2, diagnostic: "discovery refresh failed"},
				{name: "total failure", sources: []string{"failed"}, exit: 2, diagnostic: "all discovery sources failed"},
				{name: "conflicting flags", flags: []string{"--print-config"}, exit: 2, diagnostic: "mutually exclusive", noOutput: true},
				{name: "broken stdout", exit: 2, diagnostic: "cannot write discovery inventory", noOutput: true},
				{name: "blocked stdout", sources: []string{"blocked/a", "blocked/b"}, exit: 2, diagnostic: "cannot write discovery inventory", noOutput: true},
				{name: "reader closes after cancellation", sources: []string{"blocked/a"}, exit: 2, diagnostic: "cannot write discovery inventory", noOutput: true},
				{name: "cancellation", sources: []string{"cancel"}, exit: 2, diagnostic: "context canceled", noOutput: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					args := append([]string{}, entry[1:]...)
					args = append(args, "--config=", "--dump", "--listen", listen.Addr().String(), "--metrics-listen", metrics.Addr().String(),
						"--grants", strings.TrimPrefix(upstream.URL, "http://"))
					for _, source := range tc.sources {
						args = append(args, "--nodes", upstream.URL+"/"+source)
					}
					args = append(args, tc.flags...)
					cmd := exec.CommandContext(ctx, filepath.Join(install, entry[0]), args...)
					var out, logs bytes.Buffer
					cmd.Stdout, cmd.Stderr = &out, &logs
					var ready chan error
					if tc.name == "cancellation" {
						ready = started
					}
					var stdoutReader, stderrReader, stderrWriter *os.File
					if tc.name == "broken stdout" || tc.name == "blocked stdout" || tc.name == "reader closes after cancellation" {
						reader, writer, err := os.Pipe()
						require.NoError(t, err)
						defer reader.Close()
						defer writer.Close()
						cmd.Stdout = writer
						stdoutReader = reader
						if tc.name == "broken stdout" {
							require.NoError(t, reader.Close())
						} else {
							ready = make(chan error, 1)
							go func() {
								var first [1]byte
								_, err := reader.Read(first[:])
								ready <- err
							}()
						}
					}
					if tc.name == "reader closes after cancellation" {
						stderrReader, stderrWriter, err = os.Pipe()
						require.NoError(t, err)
						defer stderrReader.Close()
						defer stderrWriter.Close()
						require.NoError(t, stderrWriter.SetWriteDeadline(time.Now().Add(100*time.Millisecond)))
						_, err := stderrWriter.Write(make([]byte, 1<<20))
						require.ErrorIs(t, err, os.ErrDeadlineExceeded)
						require.NoError(t, stderrWriter.SetWriteDeadline(time.Time{}))
						cmd.Stderr = stderrWriter
					}
					require.NoError(t, cmd.Start())
					if stderrWriter != nil {
						require.NoError(t, stderrWriter.Close())
					}
					done := make(chan error, 1)
					go func() { done <- cmd.Wait() }()
					if ready != nil {
						select {
						case err := <-ready:
							require.NoError(t, err)
							require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
						case <-ctx.Done():
							t.Fatal("dump did not become ready for cancellation")
						}
					}
					if stderrReader != nil {
						// Delay the final error report until cancellation has returned
						// from Root and the stdout reader has closed.
						for i := range 2 {
							select {
							case err := <-done:
								t.Fatalf("dump exited before stderr was drained: %v", err)
							case <-time.After(100 * time.Millisecond):
							}
							if i == 0 {
								require.NoError(t, stdoutReader.Close())
							}
						}
						_, err := io.Copy(&logs, stderrReader)
						require.NoError(t, err)
					}
					err := <-done
					require.NoError(t, ctx.Err(), "dump must finish before its process deadline")
					if tc.exit == 0 {
						require.NoError(t, err, logs.String())
					} else {
						var exit *exec.ExitError
						require.ErrorAs(t, err, &exit, logs.String())
						require.Equal(t, tc.exit, exit.ExitCode(), logs.String())
					}
					if tc.diagnostic == "" {
						require.Empty(t, logs.String())
					} else {
						require.Contains(t, logs.String(), tc.diagnostic)
					}
					if tc.noOutput {
						require.Empty(t, out.String())
						return
					}
					var entries []json.RawMessage
					require.NoError(t, json.Unmarshal(out.Bytes(), &entries), out.String())
					require.NotNil(t, entries)
					require.Len(t, entries, tc.entries)
					require.True(t, strings.HasSuffix(out.String(), "\n"))
					require.Equal(t, 1, strings.Count(out.String(), "\n"))
				})
			}
		})
	}
}

func checkPythonSDK(t *testing.T, root, serviceURL, runnerURL string) {
	t.Helper()
	python, export := test.PythonSDK(t, root)
	if python == "" {
		return
	}
	fixture := filepath.Join(root, "cmd", "livepeer", "testdata", "python_sdk_compat.py")
	run := exec.Command(python, fixture, serviceURL, runnerURL, "exact-bootstrap")
	run.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(export, "src"), "PYTHONDONTWRITEBYTECODE=1")
	output, err := run.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Python SDK registration")
}

func checkGoSDK(t *testing.T, root, serviceURL, runnerURL string) {
	t.Helper()
	sdkDir := test.GoSDK(t, root)
	if sdkDir == "" {
		return
	}
	module := exec.Command("go", "list", "-m", "-f", "{{.GoVersion}}")
	module.Dir = root
	goVersion, err := module.Output()
	require.NoError(t, err)
	dir := t.TempDir()
	goMod := fmt.Sprintf("module sdkcompat\n\ngo %s\n\nrequire github.com/livepeer/golang-runner v0.0.0\n\nreplace github.com/livepeer/golang-runner => %s\n", strings.TrimSpace(string(goVersion)), sdkDir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0600))
	source, err := os.ReadFile(filepath.Join(root, "cmd", "livepeer", "testdata", "go_sdk_compat.go"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.go"), source, 0600))
	run := exec.Command("go", "run", "main.go", serviceURL, runnerURL, "exact-bootstrap")
	run.Dir = dir
	output, err := run.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Go SDK registration")
}
