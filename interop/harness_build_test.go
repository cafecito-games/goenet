package interop_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestBuildHarnessProducesScenarioBinary(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	path, output := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat binary: %v\n%s", err, output)
	}
}

func TestBuildHarnessSkipsUnchangedScenario(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	path, _ := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	before := modTime(t, path)
	_, output := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	after := modTime(t, path)
	if !strings.Contains(output, "SKIP go_server_reliable_exchange") {
		t.Fatalf("build output = %q", output)
	}
	if !before.Equal(after) {
		t.Fatalf("mod time changed: before=%v after=%v", before, after)
	}
}

func TestBuildHarnessRebuildsWhenScenarioChanges(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	path, _ := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	before := modTime(t, path)
	source := scenarioSourcePath("go_server_reliable_exchange")
	original := mustReadFile(t, source)
	originalModTime := modTime(t, source)
	t.Cleanup(func() {
		mustWriteFile(t, source, original)
		mustSetModTime(t, source, originalModTime)
	})
	mustWriteFile(t, source, original+"\n")
	mustSetModTime(t, source, before.Add(time.Second))
	_, output := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	after := modTime(t, path)
	if !strings.Contains(output, "BUILD go_server_reliable_exchange") {
		t.Fatalf("build output = %q", output)
	}
	if !after.After(before) {
		t.Fatalf("mod time did not advance: before=%v after=%v", before, after)
	}
}

func TestBuildHarnessRebuildsWhenENETSourceDirChanges(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	firstRoot := mustCreateENETSourceDir(t, cfg.ENETSourceDir)
	secondRoot := mustCreateENETSourceDir(t, cfg.ENETSourceDir)

	path, _ := runBuildHarnessWithENETSourceDir(t, firstRoot, "go_server_reliable_exchange")
	before := modTime(t, path)
	mustSetModTime(t, filepath.Join(secondRoot, "include", "enet.h"), before.Add(-time.Second))
	_, output := runBuildHarnessWithENETSourceDir(t, secondRoot, "go_server_reliable_exchange")
	after := modTime(t, path)
	if !strings.Contains(output, "BUILD go_server_reliable_exchange") {
		t.Fatalf("build output = %q", output)
	}
	if !after.After(before) {
		t.Fatalf("mod time did not advance: before=%v after=%v", before, after)
	}
}

func TestBuildHarnessRejectsUnknownScenario(t *testing.T) {
	cmd := exec.Command(filepath.Join(interopDirFromRuntime(), "scripts", "build_harness.sh"), "does_not_exist")
	cmd.Env = append(baseScriptEnv(t),
		"ENET_SOURCE_DIR=",
		"GOENET_INTEROP_ENVFILE="+filepath.Join(t.TempDir(), "missing.env"),
	)

	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected unknown scenario build to fail")
	}
	if !strings.Contains(string(output), "interop: unknown scenario: does_not_exist") {
		t.Fatalf("build_harness.sh output = %q", output)
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

func mustLoadInteropConfigForTest(t *testing.T) interopConfig {
	t.Helper()

	cfg, err := loadInteropConfig()
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("ENET_SOURCE_DIR", cfg.ENETSourceDir)
	return cfg
}

func runBuildHarness(t *testing.T, cfg interopConfig, scenario string) (string, string) {
	t.Helper()

	return runBuildHarnessWithENETSourceDir(t, cfg.ENETSourceDir, scenario)
}

func runBuildHarnessWithENETSourceDir(t *testing.T, enetSourceDir, scenario string) (string, string) {
	t.Helper()

	cmd := exec.Command(filepath.Join(interopDirFromRuntime(), "scripts", "build_harness.sh"), scenario)
	cmd.Env = append(baseScriptEnv(t), "ENET_SOURCE_DIR="+enetSourceDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build_harness.sh failed: %v\n%s", err, output)
	}

	return harnessBinaryPath(scenario), string(output)
}
