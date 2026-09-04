package model

import "testing"

func TestBinlogRowEvent_Interface(t *testing.T) {
	offset := MySQLOffset{
		File: "binlog.000001",
		Pos:  123,
	}

	event := &BinlogRowEvent{
		source: SourceMySQLBinlog,
		offset: offset,
		txID:   "tx-1",
		changes: []RowChange{
			{
				Schema: "test",
				Table:  "users",
				Op:     OpInsert,
				Rows: []RowData{
					{
						After: map[string]any{
							"id":   1,
							"name": "alice",
						},
					},
				},
			},
		},
	}

	var _ Event = event
	var _ RowChangeEvent = event

	if event.Source() != SourceMySQLBinlog {
		t.Fatal("Source mismatch")
	}
	if event.Offset().String() != "binlog.000001:123" {
		t.Fatalf("Offset mismatch: %s", event.Offset().String())
	}
	if event.TxID() != "tx-1" {
		t.Fatal("TxID mismatch")
	}

	changes := event.Changes()
	if len(changes) != 1 {
		t.Fatal("unexpected number of row changes")
	}

	rc := changes[0]
	if rc.Schema != "test" {
		t.Fatal("schema mismatch")
	}
	if rc.Table != "users" {
		t.Fatal("table mismatch")
	}
	if rc.Op != OpInsert {
		t.Fatal("operation mismatch")
	}
	if len(rc.Rows) != 1 {
		t.Fatal("row data missing")
	}

	row := rc.Rows[0]
	if row.Before != nil {
		t.Fatal("unexpected before data on insert")
	}
	if row.After == nil {
		t.Fatal("after data missing on insert")
	}
	if row.After["name"] != "alice" {
		t.Fatal("row content mismatch")
	}
}

func TestMySQLOffsetUsesStableGTIDIdentity(t *testing.T) {
	offset := MySQLOffset{
		File:    "mysql-bin.000123",
		Pos:     456,
		GTID:    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:42",
		GTIDSet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42",
	}

	if got, want := offset.String(), "mysql-gtid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:42"; got != want {
		t.Fatalf("offset.String() = %q, want %q", got, want)
	}
}
