package cursus

import (
	"testing"
)

func TestLoadPublisherConfigAcceptsStringLogLevel(t *testing.T) {
	cfg, err := loadPublisherConfig("../../../test/cursus-config.yaml")
	if err != nil {
		t.Fatalf("loadPublisherConfig() error = %v", err)
	}
	if cfg == nil {
		t.Fatal("expected publisher config")
	}
}
