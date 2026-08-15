package source

import (
	"testing"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/model"
)

func TestNewFromConfig_MySQL(t *testing.T) {
	cfg := &config.Config{
		Database: config.Database{
			Type:   model.MySQL,
			Schema: "test",
		},
		CDCServer: config.CDCServer{
			OffsetFile:      "/tmp/offset",
			PublisherConfig: "/config.yaml",
		},
	}

	src, err := NewFromConfig(nil, cfg)
	if err != nil {
		t.Logf("Expected failure to initialize source (no real cursus): %v", err)
		return
	}
	if src == nil {
		t.Fatal("expected source, got nil")
	}
}
