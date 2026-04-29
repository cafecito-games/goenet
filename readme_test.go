package goenet_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestREADMETracksCurrentPublicAPI(t *testing.T) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(".", "README.md"))
	if err != nil {
		t.Fatal(err)
	}

	readme := string(data)

	required := []string{
		"Listen",
		"NewHost",
		"Connect",
		"Service",
		"Flush",
		"Broadcast",
		"Close",
		"Peer.Send",
		"DisconnectLater",
		"Reset",
	}
	for _, needle := range required {
		if !strings.Contains(readme, needle) {
			t.Fatalf("README.md does not document %q", needle)
		}
	}

	rejected := []string{
		"Public host construction, listen/connect, and service-loop APIs are not exported yet.",
		"currently drive `internal/engine` directly until the public host API exists",
	}
	for _, needle := range rejected {
		if strings.Contains(readme, needle) {
			t.Fatalf("README.md still contains stale guidance %q", needle)
		}
	}
}
