package cursus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/tabellarius/pkg/model"
)

type fakePublisher struct {
	message     string
	err         error
	closed      bool
	flushed     bool
	sendCalls   int
	flushCalls  int
	flushErrors []error
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
	p.sendCalls++
	return 1, p.err
}

func (p *fakePublisher) Flush() error {
	p.flushed = true
	p.flushCalls++
	if len(p.flushErrors) > 0 {
		err := p.flushErrors[0]
		p.flushErrors = p.flushErrors[1:]
		return err
	}
	return nil
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

	if err := publisher.PublishContext(context.Background(), event); !errors.Is(err, want) {
		t.Fatalf("Publish() error = %v, want wrapped %v", err, want)
	}
	if fake.flushed {
		t.Fatal("publisher was flushed after Send returned an error")
	}
}

func TestPublisherWaitsForSameDeliveryAfterAmbiguousTimeout(t *testing.T) {
	fake := &fakePublisher{flushErrors: []error{
		errors.New("producer flush timeout after 10ms"),
		errors.New("producer flush timeout after 10ms"),
	}}
	var waits []time.Duration
	publisher := &Publisher{
		pub:          fake,
		retryInitial: time.Millisecond,
		retryMax:     2 * time.Millisecond,
		wait: func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
	}
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, time.Now(), "tx-1", model.TxCommit)

	if err := publisher.PublishContext(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if fake.sendCalls != 1 {
		t.Fatalf("Send() calls = %d, want 1 for the same producer sequence", fake.sendCalls)
	}
	if fake.flushCalls != 3 || len(waits) != 2 {
		t.Fatalf("Flush() calls = %d waits = %d, want 3 and 2", fake.flushCalls, len(waits))
	}
}

func TestPublisherStopsRetryingOnContextCancellation(t *testing.T) {
	fake := &fakePublisher{flushErrors: []error{errors.New("producer flush timeout after 10ms")}}
	ctx, cancel := context.WithCancel(context.Background())
	publisher := &Publisher{
		pub: fake,
		wait: func(ctx context.Context, _ time.Duration) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		},
	}
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, time.Now(), "tx-1", model.TxCommit)

	if err := publisher.PublishContext(ctx, event); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context cancellation", err)
	}
	if fake.sendCalls != 1 {
		t.Fatalf("Send() calls = %d, want 1", fake.sendCalls)
	}
}

func TestPublisherReturnsNonRetryableBrokerError(t *testing.T) {
	want := &sdk.BrokerError{Code: "PARTITION_LEADER_FENCED", Class: sdk.ErrorClassFencing, Retryable: false}
	fake := &fakePublisher{flushErrors: []error{want}}
	publisher := &Publisher{pub: fake}
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, time.Now(), "tx-1", model.TxCommit)

	if err := publisher.PublishContext(context.Background(), event); !errors.Is(err, want) {
		t.Fatalf("Publish() error = %v, want wrapped %v", err, want)
	}
	if fake.flushCalls != 1 {
		t.Fatalf("Flush() calls = %d, want 1", fake.flushCalls)
	}
}

func TestPublisherDoesNotRetryUnknownDeliveryOutcome(t *testing.T) {
	want := &sdk.ProducerOutcomeUnknownError{
		Partition: 0,
		Stage:     "acknowledgement read",
		Cause:     context.DeadlineExceeded,
	}
	fake := &fakePublisher{flushErrors: []error{want}}
	publisher := &Publisher{pub: fake}
	event := model.NewTransactionBoundaryEvent(model.SourceMySQLBinlog, model.MySQLOffset{}, time.Now(), "tx-1", model.TxCommit)

	err := publisher.PublishContext(context.Background(), event)
	if !errors.Is(err, sdk.ErrProducerOutcomeUnknown) {
		t.Fatalf("Publish() error = %v, want unknown delivery outcome", err)
	}
	if fake.flushCalls != 1 {
		t.Fatalf("Flush() calls = %d, want 1", fake.flushCalls)
	}
}
