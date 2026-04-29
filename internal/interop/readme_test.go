package interop_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestREADMECoversInteropWorkflow(t *testing.T) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(interopDirFromRuntime(), "README.md"))
	if err != nil {
		t.Fatal(err)
	}

	readme := string(data)

	required := []string{
		"cp internal/interop/.env.example internal/interop/.env",
		"ENET_SOURCE_DIR",
		"task interop:test",
		"build step still runs on every invocation, but only rebuilds scenarios whose inputs changed",
		"internal/interop/cases/",
		"one scenario entrypoint per `.c` file",
	}
	for _, needle := range required {
		if !strings.Contains(readme, needle) {
			t.Fatalf("interop/README.md does not document %q", needle)
		}
	}
}
