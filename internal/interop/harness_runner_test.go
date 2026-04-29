package interop_test

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
	"time"
)

const scenarioTimeout = 5 * time.Second

type scenarioProcess struct {
	cmd    *exec.Cmd
	output bytes.Buffer
}

func mustBuildScenario(t *testing.T, cfg interopConfig, name string) {
	t.Helper()

	path, output := runBuildHarness(t, cfg, name)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat scenario binary: %v\n%s", err, output)
	}
}

func startScenario(t *testing.T, name string, args ...string) *scenarioProcess {
	t.Helper()

	cmd := exec.Command(harnessBinaryPath(name), args...)
	var proc scenarioProcess
	cmd.Stdout = &proc.output
	cmd.Stderr = &proc.output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	proc.cmd = cmd

	return &proc
}

func mustWaitProcessSuccess(t *testing.T, proc *scenarioProcess) string {
	t.Helper()

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- proc.cmd.Wait()
	}()

	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("scenario exited with error: %v\n%s", err, proc.output.String())
		}
	case <-time.After(scenarioTimeout):
		_ = proc.cmd.Process.Kill()
		<-waitErr
		t.Fatalf("timed out waiting for scenario\n%s", proc.output.String())
	}

	return proc.output.String()
}
