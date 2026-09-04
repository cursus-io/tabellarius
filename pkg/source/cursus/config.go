package cursus

import (
	"fmt"
	"os"
	"strings"

	"github.com/cursus-io/cursus/sdk"
	"gopkg.in/yaml.v3"
)

func loadPublisherConfig(configPath string) (*sdk.PublisherConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read publisher config %s: %w", configPath, err)
	}

	var metadata struct {
		LogLevel string `yaml:"log_level"`
	}
	if err := yaml.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("parse publisher config %s: %w", configPath, err)
	}

	var fields map[string]any
	if err := yaml.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("parse publisher config %s: %w", configPath, err)
	}
	delete(fields, "log_level")
	normalized, err := yaml.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("normalize publisher config %s: %w", configPath, err)
	}

	cfg := sdk.NewDefaultPublisherConfig()
	if err := yaml.Unmarshal(normalized, cfg); err != nil {
		return nil, fmt.Errorf("parse publisher config %s: %w", configPath, err)
	}

	switch strings.ToLower(strings.TrimSpace(metadata.LogLevel)) {
	case "", "info", "debug", "warn", "warning", "error":
		// Wire v2 no longer exposes a per-producer log level. Retain
		// validation so existing configuration remains accepted.
	default:
		return nil, fmt.Errorf("invalid publisher log_level %q", metadata.LogLevel)
	}
	return cfg, nil
}
