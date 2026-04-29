package interop_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func interopScriptPath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "scripts", name)
}
