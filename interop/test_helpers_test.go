package interop_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type interopConfig struct {
	ENETSourceDir string
}

func loadInteropConfig() (interopConfig, error) {
	envFile := strings.TrimSpace(os.Getenv("GOENET_INTEROP_ENVFILE"))
	if envFile == "" {
		envFile = filepath.Join(interopDirFromRuntime(), ".env")
	}

	fileValue := ""
	file, err := os.Open(envFile)
	if err == nil {
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			key, value, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			if strings.TrimSpace(key) != "ENET_SOURCE_DIR" {
				continue
			}

			fileValue = strings.TrimSpace(value)
			break
		}
	}

	dir := strings.TrimSpace(os.Getenv("ENET_SOURCE_DIR"))
	if dir == "" {
		dir = fileValue
	}
	if dir == "" {
		return interopConfig{}, fmt.Errorf("interop: ENET_SOURCE_DIR must be set in the environment or interop/.env")
	}

	return interopConfig{ENETSourceDir: dir}, nil
}

func interopDirFromRuntime() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func harnessBinaryPath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "bin", name)
}

func scenarioSourcePath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "cases", name+".c")
}

func interopScriptPath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "scripts", name)
}

func modTime(t *testing.T, path string) time.Time {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	return info.ModTime()
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustSetModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()

	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}
}

func mustCreateENETSourceDir(t *testing.T, sourceRoot string) string {
	t.Helper()

	root := t.TempDir()
	includeDir := filepath.Join(root, "include")
	if err := os.MkdirAll(includeDir, 0o755); err != nil {
		t.Fatal(err)
	}

	mustCopyFile(t, filepath.Join(sourceRoot, "include", "enet.h"), filepath.Join(includeDir, "enet.h"))
	return root
}

func mustCopyFile(t *testing.T, src, dst string) {
	t.Helper()

	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
