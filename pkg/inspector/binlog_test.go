package inspector

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

func TestParseDSN(t *testing.T) {
	b := &BinlogInspector{
		dsn: "user:pass@tcp(localhost:3307)/mydb",
	}

	if err := b.parseDSN(); err != nil {
		t.Fatalf("parseDSN failed: %v", err)
	}
	if b.user != "user" || b.password != "pass" {
		t.Fatalf("auth parse failed")
	}
	if b.host != "localhost" || b.port != 3307 {
		t.Fatalf("host parse failed: %s:%d", b.host, b.port)
	}
}

func TestParseDSNPreservesEscapedCredentials(t *testing.T) {
	b := &BinlogInspector{dsn: "user:p:a%40ss@tcp([::1]:3307)/mydb?parseTime=true"}
	if err := b.parseDSN(); err != nil {
		t.Fatalf("parseDSN failed: %v", err)
	}
	if b.user != "user" || b.password != "p:a%40ss" || b.host != "::1" || b.port != 3307 {
		t.Fatalf("unexpected parsed connection: user=%q password=%q host=%q port=%d", b.user, b.password, b.host, b.port)
	}
}

func TestRequiredTLSConfig(t *testing.T) {
	cfg := requiredTLSConfig()
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minimum TLS version = %d, want TLS 1.2", cfg.MinVersion)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("required-mode TLS must not require a managed CA bundle")
	}
}

func TestEmitRowEvents_Write(t *testing.T) {
	out := make(chan model.Event, 1)

	b := &BinlogInspector{
		currentFile: "binlog.000001",
		currentTxID: "tx-1",
		tableMeta: map[string]*tableMeta{
			"test.users": {
				pkName: "id", pkIndex: 0, columns: []string{"id", "name"},
			},
		},
	}

	ev := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("test"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "alice"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.WRITE_ROWS_EVENTv2,
		LogPos:    123,
	}

	b.emitRowEvents(out, header, ev)
	close(out)

	got, ok := <-out
	if !ok {
		t.Fatalf("no event emitted")
	}

	rowEvt, ok := got.(*model.BinlogRowEvent)
	if !ok {
		t.Fatalf("unexpected event type: %T", got)
	}

	if rowEvt.TxID() != "tx-1" {
		t.Fatalf("unexpected txID: %s", rowEvt.TxID())
	}
	if len(rowEvt.Changes()) != 1 {
		t.Fatalf("expected 1 change, got %d", len(rowEvt.Changes()))
	}

	change := rowEvt.Changes()[0]
	if change.Op != model.OpInsert {
		t.Fatalf("expected insert op, got %s", change.Op)
	}
}

func TestEmitRowEvents_UpdateInvalid(t *testing.T) {
	out := make(chan model.Event, 1)

	b := &BinlogInspector{
		currentTxID: "tx-1",
		tableMeta: map[string]*tableMeta{
			"test.users": {
				pkName: "id", pkIndex: 0, columns: []string{"id", "name"},
			},
		},
	}

	ev := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("test"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "before"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.UPDATE_ROWS_EVENTv2,
		LogPos:    10,
	}

	b.emitRowEvents(out, header, ev)
	close(out)

	if _, ok := <-out; ok {
		t.Fatalf("expected no events for invalid update")
	}
}

func TestGTIDStartsTransactionAndXIDCommitsIt(t *testing.T) {
	out := make(chan model.Event, 1)
	b := &BinlogInspector{dbType: model.MySQL, currentFile: "mysql-bin.000011"}
	id := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	err := b.handleEvent(context.Background(), out, &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.GTID_EVENT, LogPos: 100},
		Event:  &replication.GTIDEvent{SID: id[:], GNO: 42},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-out:
		t.Fatalf("GTID event committed the transaction early: %T", event)
	default:
	}

	set, err := mysql.ParseGTIDSet("mysql", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42")
	if err != nil {
		t.Fatal(err)
	}
	err = b.handleEvent(context.Background(), out, &replication.BinlogEvent{
		Header: &replication.EventHeader{LogPos: 200},
		Event:  &replication.XIDEvent{XID: 7, GSet: set},
	})
	if err != nil {
		t.Fatal(err)
	}

	boundary, ok := (<-out).(*model.TransactionBoundaryEvent)
	if !ok {
		t.Fatal("expected transaction boundary")
	}
	if boundary.TxID() != "gtid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:42" {
		t.Fatalf("TxID() = %q", boundary.TxID())
	}
	offset := boundary.Offset().(model.MySQLOffset)
	if offset.GTIDSet != set.String() || offset.Pos != 200 {
		t.Fatalf("offset = %+v", offset)
	}
}

func TestSavepointDoesNotCommitTransaction(t *testing.T) {
	out := make(chan model.Event, 1)
	b := &BinlogInspector{dbType: model.MySQL, currentTxID: "gtid:tx-1"}
	err := b.handleEvent(context.Background(), out, &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT, LogPos: 150},
		Event:  &replication.QueryEvent{Schema: []byte("commerce"), Query: []byte("SAVEPOINT before_update")},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-out:
		t.Fatalf("SAVEPOINT committed the transaction early: %T", event)
	default:
	}
	if b.currentTxID != "gtid:tx-1" {
		t.Fatalf("transaction identity changed to %q", b.currentTxID)
	}
}

func TestAnonymousGTIDUsesFilePositionIdentityWhenOptional(t *testing.T) {
	b := &BinlogInspector{dbType: model.MySQL}
	err := b.handleEvent(context.Background(), make(chan model.Event, 1), &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ANONYMOUS_GTID_EVENT, LogPos: 123},
		Event:  &replication.GTIDEvent{SID: make([]byte, 16)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if b.currentGTID != "" || b.currentTxID != "anonymous:123" {
		t.Fatalf("anonymous event state = gtid %q tx %q", b.currentGTID, b.currentTxID)
	}
}

func TestAnonymousGTIDFailsClosedWhenRequired(t *testing.T) {
	b := &BinlogInspector{
		dbType:  model.MySQL,
		options: BinlogInspectorOptions{RequireGTID: true},
	}
	err := b.handleEvent(context.Background(), make(chan model.Event, 1), &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ANONYMOUS_GTID_EVENT, LogPos: 123},
		Event:  &replication.GTIDEvent{SID: make([]byte, 16)},
	})
	if err == nil {
		t.Fatal("expected anonymous GTID to fail closed")
	}
}

func TestZeroGTIDFailsClosedWhenRequired(t *testing.T) {
	b := &BinlogInspector{
		dbType:  model.MySQL,
		options: BinlogInspectorOptions{RequireGTID: true},
	}
	err := b.handleEvent(context.Background(), make(chan model.Event, 1), &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.GTID_EVENT, LogPos: 123},
		Event:  &replication.GTIDEvent{SID: make([]byte, 16), GNO: 0},
	})
	if err == nil {
		t.Fatal("expected zero GTID to fail closed")
	}
}

func TestOnTableMapRejectsMissingConfiguredPrimaryKey(t *testing.T) {
	b := &BinlogInspector{tableMeta: map[string]*tableMeta{
		"commerce.markets": {pkName: "id", pkIndex: -1},
	}}
	err := b.onTableMap(&replication.TableMapEvent{
		Schema:     []byte("commerce"),
		Table:      []byte("markets"),
		ColumnName: [][]byte{[]byte("name"), []byte("status")},
	})
	if err == nil {
		t.Fatal("expected missing configured primary key to fail closed")
	}
}

func TestNewBinlogInspectorRejectsMalformedCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.binlog")
	if err := os.WriteFile(path, []byte("not-json"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := NewBinlogInspector(nil, model.MySQL, "commerce", "user:pass@tcp(localhost:3306)/commerce", path, 1, nil)
	if err == nil {
		t.Fatal("expected malformed checkpoint error")
	}
}

func TestNewBinlogInspectorRequiresConfiguredCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.binlog")
	_, err := NewBinlogInspectorWithOptions(
		nil,
		model.MySQL,
		"commerce",
		"user:pass@tcp(localhost:3306)/commerce",
		path,
		1,
		nil,
		BinlogInspectorOptions{RequireExistingCheckpoint: true},
	)
	if err == nil {
		t.Fatal("expected missing required checkpoint error")
	}
}
func TestEmitRowEvents_AllowListControlsPublishing(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		eventType replication.EventType
		rows      [][]interface{}
		publish   bool
		wantOp    model.OpType
	}{
		{name: "allow-listed insert", table: "categories", eventType: replication.WRITE_ROWS_EVENTv2, rows: [][]interface{}{{1, "new"}}, publish: true, wantOp: model.OpInsert},
		{name: "allow-listed update", table: "categories", eventType: replication.UPDATE_ROWS_EVENTv2, rows: [][]interface{}{{1, "before"}, {1, "after"}}, publish: true, wantOp: model.OpUpdate},
		{name: "allow-listed delete", table: "categories", eventType: replication.DELETE_ROWS_EVENTv2, rows: [][]interface{}{{1, "old"}}, publish: true, wantOp: model.OpDelete},
		{name: "unconfigured insert", table: "products", eventType: replication.WRITE_ROWS_EVENTv2, rows: [][]interface{}{{1, "new"}}, wantOp: model.OpInsert},
		{name: "unconfigured update", table: "products", eventType: replication.UPDATE_ROWS_EVENTv2, rows: [][]interface{}{{1, "before"}, {1, "after"}}, wantOp: model.OpUpdate},
		{name: "unconfigured delete", table: "products", eventType: replication.DELETE_ROWS_EVENTv2, rows: [][]interface{}{{1, "old"}}, wantOp: model.OpDelete},
		{name: "revision history consumer insert", table: "revision_history", eventType: replication.WRITE_ROWS_EVENTv2, rows: [][]interface{}{{1, "consumer insert"}}, wantOp: model.OpInsert},
		{name: "revision history update", table: "revision_history", eventType: replication.UPDATE_ROWS_EVENTv2, rows: [][]interface{}{{1, "before"}, {1, "after"}}, wantOp: model.OpUpdate},
		{name: "revision history delete", table: "revision_history", eventType: replication.DELETE_ROWS_EVENTv2, rows: [][]interface{}{{1, "old"}}, wantOp: model.OpDelete},
		{name: "cdc log insert", table: "tabellarius_cdc_log", eventType: replication.WRITE_ROWS_EVENTv2, rows: [][]interface{}{{1, "consumer insert"}}, wantOp: model.OpInsert},
		{name: "cdc log update", table: "tabellarius_cdc_log", eventType: replication.UPDATE_ROWS_EVENTv2, rows: [][]interface{}{{1, "before"}, {1, "after"}}, wantOp: model.OpUpdate},
		{name: "cdc log delete", table: "tabellarius_cdc_log", eventType: replication.DELETE_ROWS_EVENTv2, rows: [][]interface{}{{1, "old"}}, wantOp: model.OpDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := make(chan model.Event, 1)
			b := &BinlogInspector{
				tableMeta: map[string]*tableMeta{
					"commerce.categories": {pkName: "id", pkIndex: 0, columns: []string{"id", "name"}},
				},
			}
			event := &replication.RowsEvent{
				Table: &replication.TableMapEvent{
					Schema: []byte("commerce"),
					Table:  []byte(tt.table),
				},
				Rows: tt.rows,
			}

			b.emitRowEvents(out, &replication.EventHeader{EventType: tt.eventType, LogPos: 123}, event)

			if b.currentTxID == "" {
				t.Fatal("row event did not establish a transaction ID")
			}
			if len(b.tableMeta) != 1 {
				t.Fatalf("unconfigured table added metadata: %#v", b.tableMeta)
			}

			select {
			case got := <-out:
				if !tt.publish {
					t.Fatalf("unexpected event for unconfigured table %s: %#v", tt.table, got)
				}
				rowEvent, ok := got.(*model.BinlogRowEvent)
				if !ok {
					t.Fatalf("event type = %T, want *model.BinlogRowEvent", got)
				}
				changes := rowEvent.Changes()
				if len(changes) != 1 || changes[0].Op != tt.wantOp {
					t.Fatalf("changes = %#v, want one %s change", changes, tt.wantOp)
				}
			default:
				if tt.publish {
					t.Fatalf("no event emitted for allow-listed table %s", tt.table)
				}
			}
		})
	}
}
func TestNewBinlogInspector_RegistersOnlyConfiguredTables(t *testing.T) {
	b, err := NewBinlogInspector(
		nil,
		model.MySQL,
		"commerce",
		"user:pass@tcp(localhost:3306)/commerce",
		"",
		1,
		[]config.Table{{Name: "categories", PK: "id", IncludeColumns: []string{"id", "name"}}},
	)
	if err != nil {
		t.Fatalf("NewBinlogInspector() error = %v", err)
	}

	if len(b.tableMeta) != 1 {
		t.Fatalf("configured table metadata = %#v, want only categories", b.tableMeta)
	}
	if _, ok := b.tableMeta["commerce.categories"]; !ok {
		t.Fatal("categories is not registered for capture")
	}
	if _, ok := b.tableMeta["commerce.revision_history"]; ok {
		t.Fatal("revision_history must not be registered for capture")
	}
	if _, ok := b.tableMeta["commerce.tabellarius_cdc_log"]; ok {
		t.Fatal("tabellarius_cdc_log must not be registered for capture")
	}
}

func TestNewBinlogInspectorRequiresColumnAllowList(t *testing.T) {
	_, err := NewBinlogInspector(
		nil,
		model.MySQL,
		"commerce",
		"user:pass@tcp(localhost:3306)/commerce",
		"",
		1,
		[]config.Table{{Name: "categories", PK: "id"}},
	)
	if err == nil {
		t.Fatal("expected missing include_columns to fail closed")
	}
}

func TestEmitRowEventsRejectsMissingPrimaryKeyInRowImage(t *testing.T) {
	b := &BinlogInspector{
		currentTxID: "tx-1",
		tableMeta: map[string]*tableMeta{
			"commerce.categories": {pkName: "id", pkIndex: 1, columns: []string{"name", "id"}},
		},
	}
	event := &replication.RowsEvent{
		Table: &replication.TableMapEvent{Schema: []byte("commerce"), Table: []byte("categories")},
		Rows:  [][]interface{}{{"missing-id"}},
	}
	err := b.emitRowEventsContext(
		context.Background(),
		make(chan model.Event, 1),
		&replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 123},
		event,
	)
	if err == nil {
		t.Fatal("expected missing primary key in row image to fail closed")
	}
}

func TestRowToMapExcludesConfiguredColumns(t *testing.T) {
	got := rowToMap(
		[]string{"id", "email", "password_hash", "future_secret"},
		[]interface{}{1, "seller@example.com", "secret", "unknown-secret"},
		map[string]struct{}{"id": {}, "email": {}, "password_hash": {}},
		map[string]struct{}{"password_hash": {}},
	)

	if _, ok := got["password_hash"]; ok {
		t.Fatal("password_hash must be excluded before publishing")
	}
	if _, ok := got["future_secret"]; ok {
		t.Fatal("unknown columns must fail closed when an include policy exists")
	}
	if got["id"] != 1 || got["email"] != "seller@example.com" {
		t.Fatalf("unexpected sanitized row: %#v", got)
	}
}
