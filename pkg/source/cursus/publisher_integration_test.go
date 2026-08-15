//go:build integration

package cursus

import (
	"os"
	"testing"

	"github.com/cursus-io/cursus/sdk"
)

func TestCursusTopicContainsPublishedCDCRecords(t *testing.T) {
	addr := os.Getenv("CURSUS_ADDR")
	if addr == "" {
		t.Fatal("CURSUS_ADDR must name a running Cursus broker")
	}

	cfg := sdk.NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{addr}
	client, err := sdk.NewConsumerClient(cfg)
	if err != nil {
		t.Fatalf("NewConsumerClient() error = %v", err)
	}

	offsets, err := client.ListOffsets("tabellarius.cdc", 0)
	if err != nil {
		t.Fatalf("ListOffsets() error = %v", err)
	}
	if len(offsets) != 1 {
		t.Fatalf("ListOffsets() returned %d partitions, want 1", len(offsets))
	}
	if offsets[0].LEO == 0 {
		t.Fatalf("topic has no persisted records: %+v", offsets[0])
	}
}
