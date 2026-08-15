//go:build e2e

package test

import (
	"os/exec"
	"strings"
	"testing"
)

func composeOutput(t *testing.T, args ...string) string {
	t.Helper()

	cmd := exec.Command("docker", append([]string{"compose", "-f", composeFile}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s failed: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
