package source

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/util"
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
	events []model.Event
	err    error
	closed bool
}

func (f *fakeEventPublisher) Publish(event model.Event) error {
	if f.err != nil {
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

func TestRunDoesNotAdvanceCheckpointAfterPublishFailure(t *testing.T) {
	want := errors.New("broker unavailable")
	offset := model.MySQLOffset{File: "mysql-bin.000011", Pos: 99}
	path := filepath.Join(t.TempDir(), "commerce.binlog")
	source := &TabellariusSource{
		ins:            &fakeInspector{events: transactionEvents(offset)},
		pub:            &fakeEventPublisher{err: want},
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
