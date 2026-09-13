package source

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/util"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type fakeInspector struct {
	events []model.Event
	err    error
}

func (f *fakeInspector) Start(ctx context.Context, out chan<- model.Event) error {
	for _, event := range f.events {
		select {
		case out <- event:
		case <-ctx.Done():
			return nil
		}
	}
	return f.err
}

type blockingInspector struct{}

func (*blockingInspector) Start(ctx context.Context, _ chan<- model.Event) error {
	<-ctx.Done()
	return nil
}

type fakeEventPublisher struct {
	events   []model.Event
	err      error
	failures int
	closed   bool
}

type blockingEventPublisher struct {
	started chan struct{}
	ack     chan struct{}
}

func (p *blockingEventPublisher) PublishContext(ctx context.Context, _ model.Event) error {
	close(p.started)
	select {
	case <-p.ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*blockingEventPublisher) Close() error { return nil }

func (f *fakeEventPublisher) PublishContext(_ context.Context, event model.Event) error {
	if f.err != nil && (f.failures < 0 || f.failures > 0) {
		if f.failures > 0 {
			f.failures--
		}
		return f.err
	}
	f.events = append(f.events, event)
	return nil
}

func (f *fakeEventPublisher) Close() error {
	f.closed = true
	return nil
}

func transactionEvents(offset model.MySQLOffset) []model.Event {
	timestamp := time.Now().UTC()
	return []model.Event{
		model.NewBinlogRowEvent(
			model.SourceMySQLBinlog,
			offset,
			timestamp,
			"gtid:"+offset.GTID,
			[]model.RowChange{{
				Schema: "commerce",
				Table:  "markets",
				Op:     model.OpUpdate,
				Rows:   []model.RowData{{PK: map[string]any{"id": 8}}},
			}},
		),
		model.NewTransactionBoundaryEvent(
			model.SourceMySQLBinlog,
			offset,
			timestamp,
			"gtid:"+offset.GTID,
			model.TxCommit,
		),
	}
}

func TestRunSavesCheckpointOnlyAfterPublishSucceeds(t *testing.T) {
	offset := model.MySQLOffset{
		File:    "mysql-bin.000011",
		Pos:     797583887,
		GTID:    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:42",
		GTIDSet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42",
	}
	path := filepath.Join(t.TempDir(), "commerce.binlog")
	publisher := &fakeEventPublisher{}
	source := &TabellariusSource{
		ins:            &fakeInspector{events: transactionEvents(offset)},
		pub:            publisher,
		checkpointPath: path,
	}

	if err := source.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(publisher.events))
	}
	checkpoint, found, err := util.LoadJSONStrict[model.MySQLOffset](path)
	if err != nil || !found {
		t.Fatalf("checkpoint found=%v err=%v", found, err)
	}
	if checkpoint != offset {
		t.Fatalf("checkpoint = %+v, want %+v", checkpoint, offset)
	}
}

func TestRunSavesTaggedGTIDCheckpoint(t *testing.T) {
	offset := model.MySQLOffset{
		File:    "mysql-bin.000011",
		Pos:     797583889,
		GTID:    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:mysqlsh:7",
		GTIDSet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42:mysqlsh:1-7",
	}
	path := filepath.Join(t.TempDir(), "commerce.binlog")
	source := &TabellariusSource{
		ins:            &fakeInspector{events: transactionEvents(offset)},
		pub:            &fakeEventPublisher{},
		checkpointPath: path,
	}

	if err := source.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	checkpoint, found, err := util.LoadJSONStrict[model.MySQLOffset](path)
	if err != nil || !found {
		t.Fatalf("checkpoint found=%v err=%v", found, err)
	}
	if checkpoint != offset {
		t.Fatalf("checkpoint = %+v, want %+v", checkpoint, offset)
	}
	set, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, checkpoint.GTIDSet)
	if err != nil {
		t.Fatalf("parse persisted tagged GTID set: %v", err)
	}
	if got := set.String(); got != offset.GTIDSet {
		t.Fatalf("persisted tagged GTID set = %q, want %q", got, offset.GTIDSet)
	}
}

func TestRunDoesNotStoreCheckpointBeforeAcknowledgement(t *testing.T) {
	offset := model.MySQLOffset{
		File:    "mysql-bin.000011",
		Pos:     797583888,
		GTID:    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:43",
		GTIDSet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-43",
	}
	path := filepath.Join(t.TempDir(), "commerce.binlog")
	publisher := &blockingEventPublisher{started: make(chan struct{}), ack: make(chan struct{})}
	source := &TabellariusSource{
		ins:            &fakeInspector{events: transactionEvents(offset)},
		pub:            publisher,
		checkpointPath: path,
	}
	done := make(chan error, 1)
	go func() { done <- source.Run(context.Background()) }()

	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("publish did not start")
	}
	if _, found, err := util.LoadJSONStrict[model.MySQLOffset](path); err != nil || found {
		t.Fatalf("checkpoint stored before acknowledgement: found=%v err=%v", found, err)
	}
	close(publisher.ack)
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	checkpoint, found, err := util.LoadJSONStrict[model.MySQLOffset](path)
	if err != nil || !found || checkpoint != offset {
		t.Fatalf("checkpoint after acknowledgement = %+v found=%v err=%v, want %+v", checkpoint, found, err, offset)
	}
}

func TestRunDoesNotAdvanceCheckpointAfterPublishFailure(t *testing.T) {
	want := errors.New("broker unavailable")
	offset := model.MySQLOffset{File: "mysql-bin.000011", Pos: 99}
	path := filepath.Join(t.TempDir(), "commerce.binlog")
	source := &TabellariusSource{
		ins:            &fakeInspector{events: transactionEvents(offset)},
		pub:            &fakeEventPublisher{err: want, failures: -1},
		checkpointPath: path,
	}

	err := source.Run(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Run() error = %v, want wrapped %v", err, want)
	}
	if _, found, loadErr := util.LoadJSONStrict[model.MySQLOffset](path); loadErr != nil || found {
		t.Fatalf("checkpoint advanced after publish failure: found=%v err=%v", found, loadErr)
	}
}

func TestRestartReprocessesSameGTIDAfterUnacknowledgedPublish(t *testing.T) {
	want := errors.New("broker unavailable")
	offset := model.MySQLOffset{
		File:    "mysql-bin.000011",
		Pos:     101,
		GTID:    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:43",
		GTIDSet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-43",
	}
	path := filepath.Join(t.TempDir(), "commerce.binlog")
	first := &TabellariusSource{
		ins:            &fakeInspector{events: transactionEvents(offset)},
		pub:            &fakeEventPublisher{err: want, failures: 1},
		checkpointPath: path,
	}

	if err := first.Run(context.Background()); !errors.Is(err, want) {
		t.Fatalf("first Run() error = %v, want wrapped %v", err, want)
	}
	if _, found, err := util.LoadJSONStrict[model.MySQLOffset](path); err != nil || found {
		t.Fatalf("checkpoint advanced before acknowledgement: found=%v err=%v", found, err)
	}

	replayedPublisher := &fakeEventPublisher{}
	restarted := &TabellariusSource{
		ins:            &fakeInspector{events: transactionEvents(offset)},
		pub:            replayedPublisher,
		checkpointPath: path,
	}
	if err := restarted.Run(context.Background()); err != nil {
		t.Fatalf("restarted Run() error = %v", err)
	}
	if len(replayedPublisher.events) != 1 || replayedPublisher.events[0].Offset().Compare(offset) != 0 {
		t.Fatalf("replayed events = %#v, want the same GTID offset %s", replayedPublisher.events, offset.String())
	}
	checkpoint, found, err := util.LoadJSONStrict[model.MySQLOffset](path)
	if err != nil || !found || checkpoint != offset {
		t.Fatalf("checkpoint after replay = %+v found=%v err=%v, want %+v", checkpoint, found, err, offset)
	}
}

func TestRunPropagatesInspectorFailure(t *testing.T) {
	want := errors.New("terminal stream error")
	source := &TabellariusSource{
		ins:            &fakeInspector{err: want},
		pub:            &fakeEventPublisher{},
		checkpointPath: filepath.Join(t.TempDir(), "offset"),
	}

	err := source.Run(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Run() error = %v, want wrapped %v", err, want)
	}
}

func TestRunTreatsCancellationAsCleanShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := &TabellariusSource{
		ins:            &blockingInspector{},
		pub:            &fakeEventPublisher{},
		checkpointPath: filepath.Join(t.TempDir(), "offset"),
	}

	if err := source.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}
