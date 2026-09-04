package cursus

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/tabellarius/pkg/model"
)

type fakePublisher struct {
	message string
	err     error
	closed  bool
	flushed bool
	acked   uint64
	noAck   bool
}

func TestPublisherLogDoesNotRenderRowValues(t *testing.T) {
	var output bytes.Buffer
	writer, flags := log.Writer(), log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(writer)
		log.SetFlags(flags)
	}()

	publisher := &Publisher{}
	publisher.logEvent(model.NewTransactionEvent(
		model.SourceMySQLBinlog,
		model.MySQLOffset{File: "mysql-bin.000001", Pos: 42},
		time.Now(),
		"tx-1",
		[]model.RowChange{{Schema: "commerce", Table: "members", Op: model.OpUpdate, Rows: []model.RowData{{Before: map[string]any{"password_hash": "secret-before"}, After: map[string]any{"password_hash": "secret-after"}}}}},
	))

	got := output.String()
	if strings.Contains(got, "secret-before") || strings.Contains(got, "secret-after") || strings.Contains(got, "password_hash") {
		t.Fatalf("row values leaked to log: %s", got)
	}
}

func TestPublisherLogDoesNotRenderDDLQuery(t *testing.T) {
	var output bytes.Buffer
	writer, flags := log.Writer(), log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(writer)
		log.SetFlags(flags)
	}()

	publisher := &Publisher{}
	publisher.logEvent(model.NewBinlogDDLEvent(
		model.SourceMySQLBinlog,
		model.MySQLOffset{File: "mysql-bin.000001", Pos: 42},
		time.Now(),
		"tx-1",
		"CREATE USER sensitive IDENTIFIED BY 'do-not-log'",
	))
	if got := output.String(); strings.Contains(got, "do-not-log") || strings.Contains(got, "CREATE USER") {
		t.Fatalf("DDL query leaked to log: %s", got)
	}
}

func (p *fakePublisher) Send(message string) (uint64, error) {
	p.message = message
	return 1, p.err
}

func (p *fakePublisher) Flush() {
	p.flushed = true
	if p.err == nil && !p.noAck {
		p.acked++
	}
}

func (p *fakePublisher) GetUniqueAckCount() uint64 {
	return p.acked
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
	if !fake.flushed {
		t.Fatal("publisher was not flushed after send")
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
	fake := &fakePublisher{err: want}
	publisher := &Publisher{pub: fake}
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, time.Now(), "tx-1", model.TxCommit)

	if err := publisher.Publish(event); !errors.Is(err, want) {
		t.Fatalf("Publish() error = %v, want wrapped %v", err, want)
	}
	if fake.flushed {
		t.Fatal("publisher was flushed after Send returned an error")
	}
}

func TestPublisherRejectsMissingBrokerAcknowledgement(t *testing.T) {
	publisher := &Publisher{pub: &fakePublisher{noAck: true}}
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, time.Now(), "tx-1", model.TxCommit)

	if err := publisher.Publish(event); err == nil || !strings.Contains(err.Error(), "acknowledgement") {
		t.Fatalf("Publish() error = %v, want missing acknowledgement", err)
	}
}
