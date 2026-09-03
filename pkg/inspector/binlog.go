package inspector

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/util"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type BinlogInspector struct {
	db       *sql.DB
	dbType   model.DatabaseType
	dsn      string
	serverID uint32

	host     string
	port     uint16
	user     string
	password string

	offsetPath  string
	currentFile string

	tableMeta   map[string]*tableMeta
	currentTxID string
}

var _ Inspector[model.Event] = (*BinlogInspector)(nil)

func NewBinlogInspector(db *sql.DB, dbType model.DatabaseType, schema, dsn, offsetPath string, serverID uint32, tables []config.Table) (*BinlogInspector, error) {
	if !dbType.IsBinlogBased() {
		return nil, fmt.Errorf("db %s is not binlog based", dbType)
	}

	b := &BinlogInspector{
		db:         db,
		dbType:     dbType,
		dsn:        dsn,
		serverID:   serverID,
		offsetPath: offsetPath,
		tableMeta:  make(map[string]*tableMeta),
	}

	for _, t := range tables {
		key := fmt.Sprintf("%s.%s", schema, t.Name)
		includeColumns := make(map[string]struct{}, len(t.IncludeColumns))
		for _, column := range t.IncludeColumns {
			includeColumns[column] = struct{}{}
		}
		excludeColumns := make(map[string]struct{}, len(t.ExcludeColumns))
		for _, column := range t.ExcludeColumns {
			excludeColumns[column] = struct{}{}
		}
		b.tableMeta[key] = &tableMeta{
			pkName:         t.PK,
			pkIndex:        -1,
			includeColumns: includeColumns,
			excludeColumns: excludeColumns,
		}
	}

	if err := b.parseDSN(); err != nil {
		return nil, err
	}

	if off, ok := util.LoadJSON[model.MySQLOffset](offsetPath); ok {
		b.currentFile = off.File
	}

	return b, nil
}

func (b *BinlogInspector) Start(ctx context.Context, out chan<- model.Event) error {
	log.Printf("[binlog] connect %s@%s:%d", b.user, b.host, b.port)

	cfg := replication.BinlogSyncerConfig{
		ServerID:   b.serverID,
		Flavor:     b.dbType.BinlogFlavor(),
		Host:       b.host,
		Port:       b.port,
		User:       b.user,
		Password:   b.password,
		UseDecimal: true,
		ParseTime:  true,
		TLSConfig:  requiredTLSConfig(),
	}

	syncer := replication.NewBinlogSyncer(cfg)

	var startPos mysql.Position
	if off, ok := util.LoadJSON[model.MySQLOffset](b.offsetPath); ok {
		startPos = mysql.Position{Name: off.File, Pos: off.Pos}
		b.currentFile = off.File
	}

	streamer, err := syncer.StartSync(startPos)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				log.Printf("[binlog] error: %v", err)
				time.Sleep(300 * time.Millisecond)
				continue
			}

			var txID string
			switch e := ev.Event.(type) {
			case *replication.XIDEvent:
				txID = fmt.Sprintf("xid:%d", e.XID)
			case *replication.GTIDEvent:
				txID = fmt.Sprintf("gtid:%s:%d", string(e.SID), e.GNO)
			case *replication.QueryEvent:
				query := string(e.Query)
				src := model.SourceType(b.dbType)

				if isSystemSchema(e.Schema) {
					continue
				}

				if query == "BEGIN" || query == "COMMIT" || query == "ROLLBACK" {
					continue
				}

				if strings.Contains(query, "ALTER TABLE") || strings.Contains(query, "CREATE TABLE") {
					log.Printf("[schema] DDL detected: %s. Refreshing metadata...", query)

					for key := range b.tableMeta {
						schema, table := splitKey(key)
						cols := b.fetchColumns(schema, table)

						if len(cols) > 0 {
							b.tableMeta[key].columns = cols
							b.updatePKIndex(key)
						}
					}
				}

				if !isDML(e) {
					eventTime := time.Unix(int64(ev.Header.Timestamp), 0)
					offset := model.MySQLOffset{
						File: b.currentFile,
						Pos:  ev.Header.LogPos,
					}
					out <- model.NewBinlogDDLEvent(src, offset, eventTime, b.currentTxID, query)
				} else {
					if b.currentTxID == "" {
						b.currentTxID = fmt.Sprintf("query:%d", ev.Header.LogPos)
					}
				}
			}

			if txID != "" && b.currentTxID == "" {
				b.currentTxID = txID
			}

			switch e := ev.Event.(type) {
			case *replication.TableMapEvent:
				b.onTableMap(e)
			case *replication.RowsEvent:
				b.emitRowEvents(out, ev.Header, e)
			case *replication.RotateEvent:
				b.currentFile = string(e.NextLogName)
			case *replication.XIDEvent, *replication.GTIDEvent, *replication.QueryEvent:
				if b.currentTxID != "" {
					eventTime := time.Unix(int64(ev.Header.Timestamp), 0)

					offset := model.MySQLOffset{
						File: b.currentFile,
						Pos:  ev.Header.LogPos,
					}
					out <- model.NewTransactionBoundaryEvent(model.SourceType(b.dbType), offset, eventTime, b.currentTxID, model.TxCommit)
					if err := util.SaveJSON(b.offsetPath, offset); err != nil {
						log.Printf("[binlog] failed to save offset: %v", err)
					}
					b.currentTxID = ""
				}
			default:
				log.Printf("[binlog] unhandled event type: %T", ev.Event)
			}
		}
	}
}

func requiredTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // MySQL REQUIRED mode has no managed CA bundle.
	}
}

func (b *BinlogInspector) onTableMap(e *replication.TableMapEvent) {
	if isSystemSchema(e.Schema) {
		return
	}

	key := fmt.Sprintf("%s.%s", e.Schema, e.Table)
	meta, ok := b.tableMeta[key]
	if !ok {
		return
	}

	if len(e.ColumnName) == 0 {
		cols := b.fetchColumns(string(e.Schema), string(e.Table))
		if len(cols) == 0 {
			log.Printf("[binlog] column metadata missing for table %s, skip pk index detection", key)
			return
		}
		meta.columns = cols
	} else {
		meta.columns = bytesToStrings(e.ColumnName)
	}

	meta.pkIndex = -1
	for i, col := range meta.columns {
		if col == meta.pkName {
			meta.pkIndex = i
			break
		}
	}

	if meta.pkIndex == -1 {
		log.Printf("[binlog] pk %s not found in table %s, fallback to first column", meta.pkName, key)
		meta.pkIndex = 0
	}
}

func (b *BinlogInspector) emitRowEvents(out chan<- model.Event, h *replication.EventHeader, e *replication.RowsEvent) {
	if isSystemSchema(e.Table.Schema) {
		return
	}

	table := fmt.Sprintf("%s.%s", e.Table.Schema, e.Table.Table)
	if b.currentTxID == "" && !isSystemSchema(e.Table.Schema) {
		b.currentTxID = fmt.Sprintf("tx:%d", h.LogPos)
	}

	eventTime := time.Unix(int64(h.Timestamp), 0)
	meta, ok := b.tableMeta[table]
	if !ok {
		log.Printf("[binlog] debug: skip row event for unconfigured table %s", table)
		return
	}

	offset := model.MySQLOffset{
		File: b.currentFile,
		Pos:  h.LogPos,
	}

	src := model.SourceType(b.dbType)
	schema := string(e.Table.Schema)
	tableName := string(e.Table.Table)

	var op model.OpType
	switch h.EventType {
	case replication.WRITE_ROWS_EVENTv2:
		op = model.OpInsert
	case replication.DELETE_ROWS_EVENTv2:
		op = model.OpDelete
	case replication.UPDATE_ROWS_EVENTv2:
		op = model.OpUpdate
	default:
		return
	}

	var rowsData []model.RowData
	if op == model.OpUpdate {
		if len(e.Rows)%2 != 0 {
			log.Printf("[binlog] invalid UPDATE_ROWS_EVENT rows=%d table=%s", len(e.Rows), table)
			return
		}
		for i := 0; i < len(e.Rows); i += 2 {
			before := e.Rows[i]
			after := e.Rows[i+1]
			rowsData = append(rowsData, model.RowData{
				PK:     extractPK(meta, before),
				Before: rowToMap(meta.columns, before, meta.includeColumns, meta.excludeColumns),
				After:  rowToMap(meta.columns, after, meta.includeColumns, meta.excludeColumns),
			})
		}
	} else {
		for _, row := range e.Rows {
			data := model.RowData{PK: extractPK(meta, row)}
			if op == model.OpInsert {
				data.After = rowToMap(meta.columns, row, meta.includeColumns, meta.excludeColumns)
			} else {
				data.Before = rowToMap(meta.columns, row, meta.includeColumns, meta.excludeColumns)
			}
			rowsData = append(rowsData, data)
		}
	}

	if len(rowsData) > 0 {
		out <- model.NewBinlogRowEvent(
			src,
			offset,
			eventTime,
			b.currentTxID,
			[]model.RowChange{
				{
					Schema: schema,
					Table:  tableName,
					Op:     op, Rows: rowsData,
				},
			})
	}
}
