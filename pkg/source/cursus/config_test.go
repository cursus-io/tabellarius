package cursus

import (
	"testing"

	"github.com/cursus-io/cursus/sdk"
)

func TestLoadPublisherConfigAcceptsStringLogLevel(t *testing.T) {
	cfg, err := loadPublisherConfig("../../../test/cursus-config.yaml")
	if err != nil {
		t.Fatalf("loadPublisherConfig() error = %v", err)
	}
	if cfg.LogLevel != sdk.LogLevelInfo {
		t.Fatalf("LogLevel = %v, want %v", cfg.LogLevel, sdk.LogLevelInfo)
	}
}
