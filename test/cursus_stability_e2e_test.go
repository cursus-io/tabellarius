//go:build e2e

package test

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
)

const (
	composeFile = "docker-compose.yaml"
	topicName   = "tabellarius.cdc"
)

func TestCursusRestartRecoveryE2E(t *testing.T) {
	if os.Getenv("RUN_CURSUS_E2E") != "1" {
		t.Skip("set RUN_CURSUS_E2E=1 to run Docker Compose stability E2E")
	}

	runCompose(t, "down")
	t.Cleanup(func() { runCompose(t, "down") })

	runCompose(t, "up", "-d", "--build", "broker", "mysql")
	waitForContainer(t, "cdc-mysql", "healthy")
	waitForContainer(t, "tabellarius-cursus", "healthy")
	initializeCDC(t)
	runCompose(t, "up", "-d", "cdc-server")

	restartAndAssertPublish(t, "cdc-server", "CDC server restart recovery")
	restartAndAssertPublish(t, "broker", "Cursus broker restart recovery")
	assertTopicHasRecords(t)
}

func restartAndAssertPublish(t *testing.T, service, scenario string) {
	t.Helper()
	since := time.Now().UTC()

	runCompose(t, "restart", service)
	if service == "cdc-server" {
		waitForServiceLog(t, service, since, "[binlog] stream started")
	}
	if service == "broker" {
		waitForContainer(t, "tabellarius-cursus", "healthy")
	}

	id := time.Now().UnixNano()
	runCompose(t, "exec", "-T", "mysql", "mysql", "-uroot", "-proot", "mydb", "-e",
		fmt.Sprintf("INSERT INTO users (email, name) VALUES ('stability-%d@example.com', 'stability-%d')", id, id))

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		logs := composeOutput(t, "logs", "--since", since.Format(time.RFC3339Nano), "cdc-server")
		if strings.Contains(logs, "[publish]") && strings.Contains(logs, "table=mydb.users") && strings.Contains(logs, "op=INSERT") {
			return
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("%s: new MySQL transaction was not published after restart", scenario)
}

func waitForServiceLog(t *testing.T, service string, since time.Time, expected string) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		logs := composeOutput(t, "logs", "--since", since.Format(time.RFC3339Nano), service)
		if strings.Contains(logs, expected) {
			return
		}
		if strings.Contains(logs, "[binlog] stream failed:") {
			t.Fatalf("%s failed to start its binlog stream: %s", service, safeCDCStartupDiagnostics(logs))
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("%s did not log %q after restart", service, expected)
}

func safeCDCStartupDiagnostics(logs string) string {
	lines := strings.Split(logs, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "password") || strings.Contains(lower, "dsn") || strings.Contains(line, "@") {
			continue
		}
		if strings.Contains(line, "[binlog]") || strings.Contains(line, "[FATAL]") {
			filtered = append(filtered, line)
		}
	}
	return strings.Join(filtered, "\n")
}

func initializeCDC(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "compose", "-f", composeFile, "run", "--rm", "cdc-cli", "--mode", "init", "--apply=true")
		output, err := cmd.CombinedOutput()
		if err == nil {
			return
		}
		lastErr = fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("CDC initialization failed: %v", lastErr)
}

func waitForContainer(t *testing.T, name, expectedStatus string) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}", name)
		output, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(output)) == expectedStatus {
			return
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("%s did not reach status %s", name, expectedStatus)
}

func assertTopicHasRecords(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		cfg := sdk.NewDefaultConsumerConfig()
		cfg.BrokerAddrs = []string{"127.0.0.1:9000"}
		client, err := sdk.NewConsumerClient(cfg)
		if err == nil {
			offsets, err := client.ListOffsets(topicName, 0)
			if err == nil && len(offsets) == 1 && offsets[0].LEO > 0 {
				return
			}
			if err == nil {
				lastErr = fmt.Errorf("topic has no persisted records: %+v", offsets)
			} else {
				lastErr = err
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("topic records did not become queryable after restart: %v", lastErr)
}

func runCompose(t *testing.T, args ...string) {
	t.Helper()

	cmd := exec.Command("docker", append([]string{"compose", "-f", composeFile}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s failed: %v\n%s", strings.Join(args, " "), err, output)
	}
}
