package interop_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadInteropConfigRequiresENETSourceDir(t *testing.T) {
	t.Setenv("ENET_SOURCE_DIR", "")
	t.Setenv("GOENET_INTEROP_ENVFILE", filepath.Join(t.TempDir(), "missing.env"))

	_, err := loadInteropConfig()
	if err == nil {
		t.Fatal("expected missing ENET_SOURCE_DIR error")
	}
}

func TestLoadInteropConfigUsesEnvFileOverride(t *testing.T) {
	t.Setenv("ENET_SOURCE_DIR", "")
	t.Setenv("GOENET_INTEROP_ENVFILE", "testdata/interop/env/basic.env")

	cfg, err := loadInteropConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ENETSourceDir != "/tmp/from-env-file" {
		t.Fatalf("ENETSourceDir = %q", cfg.ENETSourceDir)
	}
}

func TestResolveInteropEnvPrefersProcessEnv(t *testing.T) {
	t.Setenv("ENET_SOURCE_DIR", "/tmp/enet")

	cfg, err := loadInteropConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ENETSourceDir != "/tmp/enet" {
		t.Fatalf("ENETSourceDir = %q, want /tmp/enet", cfg.ENETSourceDir)
	}
}

func TestBuildHarnessCommandIncludesScenarioName(t *testing.T) {
	path := harnessBinaryPath("go_server_reliable_exchange")
	want := filepath.Join(interopDirFromRuntime(), "bin", "go_server_reliable_exchange")
	if path != want {
		t.Fatalf("harnessBinaryPath() = %q, want %q", path, want)
	}
}

func TestResolveInteropEnvScriptPrefersProcessEnv(t *testing.T) {
	envFile := filepath.Join(t.TempDir(), "interop.env")
	if err := os.WriteFile(envFile, []byte("ENET_SOURCE_DIR=/tmp/from-env-file\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(interopScriptPath("resolve_env.sh"))
	cmd.Env = append(baseScriptEnv(t),
		"ENET_SOURCE_DIR=/tmp/from-process",
		"GOENET_INTEROP_ENVFILE="+envFile,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("resolve_env.sh failed: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "/tmp/from-process" {
		t.Fatalf("resolve_env.sh output = %q, want /tmp/from-process", got)
	}
}

func TestBuildHarnessScriptReportsMissingENETSourceDir(t *testing.T) {
	cmd := exec.Command(interopScriptPath("build_harness.sh"))
	cmd.Env = append(baseScriptEnv(t),
		"ENET_SOURCE_DIR=",
		"GOENET_INTEROP_ENVFILE="+filepath.Join(t.TempDir(), "missing.env"),
	)

	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected build_harness.sh to fail without ENET_SOURCE_DIR")
	}
	if !strings.Contains(string(output), "ENET_SOURCE_DIR must be set") {
		t.Fatalf("build_harness.sh output = %q", output)
	}
}

func TestBuildHarnessScriptUsesResolvedDefaultOutputPath(t *testing.T) {
	fixture := newBuildHarnessScriptFixture(t)

	explicitEnvFile := filepath.Join(t.TempDir(), "interop.env")
	if err := os.WriteFile(explicitEnvFile, []byte("ENET_SOURCE_DIR=/tmp/from-env-file\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	fixture.run(t, explicitEnvFile)

	got := fixture.readDelegateLog(t)
	wantOutput := filepath.Join(fixture.interopDir, "bin", "enet-harness")
	if got.outputPath != wantOutput {
		t.Fatalf("delegate output path = %q, want %q", got.outputPath, wantOutput)
	}
	if got.enetSourceDir != "/tmp/from-env-file" {
		t.Fatalf("delegate ENET_SOURCE_DIR = %q, want /tmp/from-env-file", got.enetSourceDir)
	}
}

func TestBuildHarnessScriptForwardsExplicitOutputPath(t *testing.T) {
	fixture := newBuildHarnessScriptFixture(t)

	explicitEnvFile := filepath.Join(t.TempDir(), "interop.env")
	if err := os.WriteFile(explicitEnvFile, []byte("ENET_SOURCE_DIR=/tmp/from-env-file\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	explicitOutput := filepath.Join(t.TempDir(), "custom-output")
	fixture.run(t, explicitEnvFile, explicitOutput)

	got := fixture.readDelegateLog(t)
	if got.outputPath != explicitOutput {
		t.Fatalf("delegate output path = %q, want %q", got.outputPath, explicitOutput)
	}
	if got.enetSourceDir != "/tmp/from-env-file" {
		t.Fatalf("delegate ENET_SOURCE_DIR = %q, want /tmp/from-env-file", got.enetSourceDir)
	}
}

func baseScriptEnv(t *testing.T) []string {
	t.Helper()

	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	if tmpDir := os.Getenv("TMPDIR"); tmpDir != "" {
		env = append(env, "TMPDIR="+tmpDir)
	}

	return env
}

type buildHarnessScriptFixture struct {
	interopDir string
	logPath    string
}

type delegateLog struct {
	enetSourceDir string
	outputPath    string
}

func newBuildHarnessScriptFixture(t *testing.T) *buildHarnessScriptFixture {
	t.Helper()

	rootDir := t.TempDir()
	interopDir := filepath.Join(rootDir, "interop")
	scriptsDir := filepath.Join(interopDir, "scripts")
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	copyScriptForTest(t, interopScriptPath("resolve_env.sh"), filepath.Join(scriptsDir, "resolve_env.sh"))
	copyScriptForTest(t, interopScriptPath("build_harness.sh"), filepath.Join(scriptsDir, "build_harness.sh"))

	logPath := filepath.Join(rootDir, "delegate.log")
	delegate := fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
printf 'ENET_SOURCE_DIR=%%s\n' "${ENET_SOURCE_DIR:-}" > %q
printf 'OUTPUT=%%s\n' "$1" >> %q
mkdir -p "$(dirname "$1")"
: > "$1"
`, logPath, logPath)
	if err := os.WriteFile(filepath.Join(interopDir, "build_c_harness.sh"), []byte(delegate), 0o755); err != nil {
		t.Fatal(err)
	}

	return &buildHarnessScriptFixture{
		interopDir: interopDir,
		logPath:    logPath,
	}
}

func copyScriptForTest(t *testing.T, srcPath, dstPath string) {
	t.Helper()

	content, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dstPath, content, 0o755); err != nil {
		t.Fatal(err)
	}
}

func (f *buildHarnessScriptFixture) run(t *testing.T, envFile string, args ...string) {
	t.Helper()

	cmd := exec.Command(filepath.Join(f.interopDir, "scripts", "build_harness.sh"), args...)
	cmd.Env = append(baseScriptEnv(t), "GOENET_INTEROP_ENVFILE="+envFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build_harness.sh failed: %v\n%s", err, output)
	}
}

func (f *buildHarnessScriptFixture) readDelegateLog(t *testing.T) delegateLog {
	t.Helper()

	content, err := os.ReadFile(f.logPath)
	if err != nil {
		t.Fatal(err)
	}

	var got delegateLog
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch key {
		case "ENET_SOURCE_DIR":
			got.enetSourceDir = value
		case "OUTPUT":
			got.outputPath = value
		}
	}

	return got
}
