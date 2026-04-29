package interop_test

import (
	"path/filepath"
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
