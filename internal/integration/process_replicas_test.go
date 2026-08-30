//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thiagomontozo/agentmesh/internal/domain"
)

func TestSeparateProcessesExecutePreserveAndRecoverRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	binary := buildAgentMeshBinary(t, ctx)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	port := reserveTCPPort(t)
	apiURL := "http://127.0.0.1:" + port
	common := map[string]string{
		"AGENTMESH_MODE": "distributed", "AGENTMESH_DATABASE_URL": env("AGENTMESH_DATABASE_URL", "postgres://agentmesh:agentmesh@localhost:5432/agentmesh?sslmode=disable"),
		"AGENTMESH_NATS_URL": env("AGENTMESH_NATS_URL", "nats://localhost:4222"), "AGENTMESH_REDIS_URL": env("AGENTMESH_REDIS_URL", "redis://localhost:6379/0"),
		"AGENTMESH_LEASE_TTL": "600ms", "AGENTMESH_NATS_ACK_WAIT": "2s", "AGENTMESH_ATTEMPT_TIMEOUT": "10s",
		"AGENTMESH_MAX_ATTEMPTS": "1", "AGENTMESH_SHUTDOWN_TIMEOUT": "2s", "AGENTMESH_AGENT_HEALTH_INTERVAL": "1h",
	}
	apiEnvironment := cloneEnvironment(common, map[string]string{
		"AGENTMESH_ROLE": "api", "AGENTMESH_ADDR": "127.0.0.1:" + port, "AGENTMESH_INSTANCE_ID": "process-api-" + suffix,
	})
	api := startAgentMeshProcess(t, ctx, binary, apiEnvironment, "agentmesh API started")
	defer func() { api.stop(false) }()
	waitHTTPReady(t, ctx, apiURL+"/readyz")

	fastWorker := startAgentMeshProcess(t, ctx, binary, cloneEnvironment(common, map[string]string{
		"AGENTMESH_ROLE": "worker", "AGENTMESH_INSTANCE_ID": "process-worker-fast-" + suffix, "AGENTMESH_EXECUTION_DELAY": "200ms",
	}), "agentmesh worker started")
	defer fastWorker.stop(false)
	agent := createProcessAgent(t, apiURL, suffix)
	normal := submitReplicaRun(t, apiURL, agent.ID, "process-normal", "process-normal-"+suffix)
	sseResult := make(chan string, 1)
	go func() {
		response, err := http.Get(apiURL + "/api/v1/runs/" + normal.ID + "/events")
		if err != nil {
			sseResult <- "error: " + err.Error()
			return
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		sseResult <- string(body)
	}()
	completed := waitProcessRun(t, ctx, apiURL, normal.ID, domain.RunSucceeded)
	if !strings.Contains(completed.Output, "process-normal") || !fastWorker.contains(normal.ID) {
		t.Fatalf("worker process did not execute API process Run: run=%+v logs=%s", completed, fastWorker.outputString())
	}
	replayed := submitReplicaRun(t, apiURL, agent.ID, "process-normal", "process-normal-"+suffix)
	if replayed.ID != normal.ID {
		t.Fatalf("cross-process idempotency returned %s instead of %s", replayed.ID, normal.ID)
	}
	select {
	case body := <-sseResult:
		if !strings.Contains(body, "event: run.succeeded") {
			t.Fatalf("API process SSE missed worker event: %s", body)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for cross-process SSE")
	}
	fastWorker.stop(true)

	owner := startAgentMeshProcess(t, ctx, binary, cloneEnvironment(common, map[string]string{
		"AGENTMESH_ROLE": "worker", "AGENTMESH_INSTANCE_ID": "process-worker-owner-" + suffix, "AGENTMESH_EXECUTION_DELAY": "5s",
	}), "agentmesh worker started")
	defer owner.stop(false)
	abandoned := submitReplicaRun(t, apiURL, agent.ID, "process-recovery", "process-recovery-"+suffix)
	waitProcessRun(t, ctx, apiURL, abandoned.ID, domain.RunRunning)
	if !owner.contains(abandoned.ID) {
		t.Fatalf("owner process did not claim Run %s: %s", abandoned.ID, owner.outputString())
	}

	api.stop(true)
	api = startAgentMeshProcess(t, ctx, binary, apiEnvironment, "agentmesh API started")
	waitHTTPReady(t, ctx, apiURL+"/readyz")
	stillRunning := getReplicaRun(t, apiURL, abandoned.ID)
	if stillRunning.Status != domain.RunRunning {
		t.Fatalf("API restart interfered with valid worker ownership: %+v", stillRunning)
	}

	owner.stop(false)
	time.Sleep(800 * time.Millisecond)
	recovery := startAgentMeshProcess(t, ctx, binary, cloneEnvironment(common, map[string]string{
		"AGENTMESH_ROLE": "worker", "AGENTMESH_INSTANCE_ID": "process-worker-recovery-" + suffix, "AGENTMESH_EXECUTION_DELAY": "20ms",
	}), "agentmesh worker started")
	defer recovery.stop(true)
	recovered := waitProcessRun(t, ctx, apiURL, abandoned.ID, domain.RunSucceeded)
	if !strings.Contains(recovered.Output, "process-recovery") || !recovery.contains(abandoned.ID) {
		t.Fatalf("replacement worker did not recover abandoned Run: run=%+v logs=%s", recovered, recovery.outputString())
	}
}

type processOutput struct {
	mu     sync.RWMutex
	buffer bytes.Buffer
}

func (b *processOutput) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(payload)
}

func (b *processOutput) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.buffer.String()
}

type agentMeshProcess struct {
	command *exec.Cmd
	output  *processOutput
	done    chan error
	once    sync.Once
}

func startAgentMeshProcess(t *testing.T, ctx context.Context, binary string, environment map[string]string, readyLog string) *agentMeshProcess {
	t.Helper()
	output := &processOutput{}
	command := exec.CommandContext(ctx, binary)
	command.Env = mergedProcessEnvironment(environment)
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &agentMeshProcess{command: command, output: output, done: make(chan error, 1)}
	go func() { process.done <- command.Wait() }()
	deadline := time.Now().Add(8 * time.Second)
	for !strings.Contains(output.String(), readyLog) {
		select {
		case err := <-process.done:
			t.Fatalf("AgentMesh process exited before readiness: %v\n%s", err, output.String())
		default:
		}
		if time.Now().After(deadline) {
			process.stop(false)
			t.Fatalf("AgentMesh process did not become ready\n%s", output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	return process
}

func (p *agentMeshProcess) stop(graceful bool) {
	if p == nil || p.command == nil || p.command.Process == nil {
		return
	}
	p.once.Do(func() {
		if graceful {
			_ = p.command.Process.Signal(os.Interrupt)
			select {
			case <-p.done:
				return
			case <-time.After(4 * time.Second):
			}
		}
		_ = p.command.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
		}
	})
}

func (p *agentMeshProcess) contains(value string) bool {
	return strings.Contains(p.output.String(), value)
}
func (p *agentMeshProcess) outputString() string { return p.output.String() }

func buildAgentMeshBinary(t *testing.T, ctx context.Context) string {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "agentmesh")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/agentmesh")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build AgentMesh process binary: %v\n%s", err, output)
	}
	return binary
}

func reserveTCPPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := fmt.Sprintf("%d", listener.Addr().(*net.TCPAddr).Port)
	_ = listener.Close()
	return port
}

func waitHTTPReady(t *testing.T, ctx context.Context, url string) {
	t.Helper()
	for {
		response, err := http.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", url)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func createProcessAgent(t *testing.T, baseURL, suffix string) domain.Agent {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"name": "process-agent-" + suffix})
	response, err := http.Post(baseURL+"/api/v1/agents", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("create process Agent: status=%d body=%s", response.StatusCode, body)
	}
	var agent domain.Agent
	if err := json.NewDecoder(response.Body).Decode(&agent); err != nil {
		t.Fatal(err)
	}
	return agent
}

func waitProcessRun(t *testing.T, ctx context.Context, baseURL, runID string, status domain.RunStatus) domain.Run {
	t.Helper()
	for {
		run := getReplicaRun(t, baseURL, runID)
		if run.Status == status {
			return run
		}
		if (run.Status == domain.RunSucceeded || run.Status == domain.RunFailed || run.Status == domain.RunCanceled) && run.Status != status {
			t.Fatalf("Run %s ended in %s instead of %s: %+v", runID, run.Status, status, run)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Run %s to reach %s", runID, status)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func cloneEnvironment(base, overrides map[string]string) map[string]string {
	result := make(map[string]string, len(base)+len(overrides))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range overrides {
		result[key] = value
	}
	return result
}

func mergedProcessEnvironment(overrides map[string]string) []string {
	result := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, overridden := overrides[key]; !overridden {
			result = append(result, entry)
		}
	}
	for key, value := range overrides {
		result = append(result, key+"="+value)
	}
	return result
}
