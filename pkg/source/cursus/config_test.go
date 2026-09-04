package cursus

import (
	"os"
	"path/filepath"
	"strings"
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

func TestPublisherPolicyRejectsUnsafeCDCSettings(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name: "auto create",
			config: "broker_addrs: [localhost:9000]\ntopic: cdc\npartitions: 1\n" +
				"auto_create_topics: true\nacks: all\nenable_idempotence: true\n",
			wantErr: "auto_create_topics",
		},
		{
			name: "weak acknowledgement",
			config: "broker_addrs: [localhost:9000]\ntopic: cdc\npartitions: 1\n" +
				"auto_create_topics: false\nacks: '1'\nenable_idempotence: true\n",
			wantErr: "acks=all",
		},
		{
			name: "idempotence disabled",
			config: "broker_addrs: [localhost:9000]\ntopic: cdc\npartitions: 1\n" +
				"auto_create_topics: false\nacks: all\nenable_idempotence: false\n",
			wantErr: "enable_idempotence",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "publisher.yaml")
			if err := os.WriteFile(path, []byte(tt.config), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := NewCursusPublisher(path)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("loadPublisherConfig() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
