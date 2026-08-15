package cursus

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cursus-io/tabellarius/pkg/model"
)

type fakePublisher struct {
	message string
	err     error
	closed  bool
}

func (p *fakePublisher) PublishMessage(message string) (uint64, error) {
	p.message = message
	return 1, p.err
}

func (p *fakePublisher) Close() error {
	p.closed = true
	return nil
}

func TestPublisherPublishesSerializableTransaction(t *testing.T) {
	fake := &fakePublisher{}
	publisher := &Publisher{pub: fake}
	event := model.NewTransactionEvent(
		model.SourceMySQLBinlog,
		model.MySQLOffset{File: "mysql-bin.000001", Pos: 42},
		time.Date(2026, 8, 16, 1, 2, 3, 0, time.FixedZone("KST", 9*60*60)),
		"tx-1",
		[]model.RowChange{{Schema: "mydb", Table: "orders", Op: model.OpInsert, Rows: []model.RowData{{After: map[string]any{"id": 1}}}}},
	)

	if err := publisher.Publish(event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	var payload eventPayload
	if err := json.Unmarshal([]byte(fake.message), &payload); err != nil {
		t.Fatalf("invalid published JSON: %v", err)
	}
	if payload.Type != "transaction" || payload.TxID != "tx-1" || payload.Offset != "mysql-bin.000001:42" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if len(payload.Changes) != 1 || payload.Changes[0].Table != "orders" {
		t.Fatalf("changes were not preserved: %+v", payload.Changes)
	}

	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !fake.closed {
		t.Fatal("publisher was not closed")
	}
}

func TestPublisherReturnsClientError(t *testing.T) {
	want := errors.New("broker unavailable")
	publisher := &Publisher{pub: &fakePublisher{err: want}}
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, time.Now(), "tx-1", model.TxCommit)

	if err := publisher.Publish(event); !errors.Is(err, want) {
		t.Fatalf("Publish() error = %v, want wrapped %v", err, want)
	}
}
