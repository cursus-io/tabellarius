package inspector

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/health"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/util"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

type BinlogInspectorOptions struct {
	RequireExistingCheckpoint bool
	RequireGTID               bool
	CaptureDDL                bool
	Status                    *health.Status
	TLSConfig                 *tls.Config
}

type BinlogInspector struct {
	db       *sql.DB
	dbType   model.DatabaseType
	dsn      string
	schema   string
	serverID uint32

	host     string
	port     uint16
	user     string
	password string

	offsetPath  string
	currentFile string

	tableMeta   map[string]*tableMeta
	currentTxID string
	currentGTID string
	options     BinlogInspectorOptions
}

var _ Inspector[model.Event] = (*BinlogInspector)(nil)

func NewBinlogInspector(db *sql.DB, dbType model.DatabaseType, schema, dsn, offsetPath string, serverID uint32, tables []config.Table) (*BinlogInspector, error) {
	return NewBinlogInspectorWithOptions(db, dbType, schema, dsn, offsetPath, serverID, tables, BinlogInspectorOptions{})
}

func NewBinlogInspectorWithOptions(db *sql.DB, dbType model.DatabaseType, schema, dsn, offsetPath string, serverID uint32, tables []config.Table, options BinlogInspectorOptions) (*BinlogInspector, error) {
	if !dbType.IsBinlogBased() {
		return nil, fmt.Errorf("db %s is not binlog based", dbType)
	}

	b := &BinlogInspector{
		db:         db,
		dbType:     dbType,
		dsn:        dsn,
		schema:     schema,
		serverID:   serverID,
		offsetPath: offsetPath,
		tableMeta:  make(map[string]*tableMeta),
		options:    options,
	}

	for _, t := range tables {
		if strings.TrimSpace(t.Name) == "" || strings.TrimSpace(t.PK) == "" {
			return nil, errors.New("configured tables require non-empty name and pk fields")
		}
		if len(t.IncludeColumns) == 0 {
			return nil, fmt.Errorf("configured table %s requires an explicit include_columns allow-list", t.Name)
		}
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

	off, found, err := util.LoadJSONStrict[model.MySQLOffset](offsetPath)
	if err != nil {
		return nil, fmt.Errorf("load binlog checkpoint: %w", err)
	}
	if options.RequireExistingCheckpoint && !found {
		return nil, fmt.Errorf("required binlog checkpoint %s does not exist", offsetPath)
	}
	if found {
		b.currentFile = off.File
	}

	return b, nil
}

func (b *BinlogInspector) Start(ctx context.Context, out chan<- model.Event) error {
	if err := b.verifyGTIDMode(ctx); err != nil {
		return err
	}
	log.Printf("[binlog] connect host=%s port=%d server_id=%d", b.host, b.port, b.serverID)

	tlsConfig := b.options.TLSConfig
	if tlsConfig == nil {
		tlsConfig = requiredTLSConfig()
	} else {
		tlsConfig = tlsConfig.Clone()
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID:   b.serverID,
		Flavor:     b.dbType.BinlogFlavor(),
		Host:       b.host,
		Port:       b.port,
		User:       b.user,
		Password:   b.password,
		UseDecimal: true,
		ParseTime:  true,
		TLSConfig:  tlsConfig,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	off, found, err := util.LoadJSONStrict[model.MySQLOffset](b.offsetPath)
	if err != nil {
		return fmt.Errorf("load binlog checkpoint: %w", err)
	}
	if b.options.RequireExistingCheckpoint && !found {
		return fmt.Errorf("required binlog checkpoint %s does not exist", b.offsetPath)
	}
	if b.options.RequireGTID && found && off.GTIDSet == "" {
		return errors.New("GTID mode requires a GTID checkpoint; convert the legacy file/position checkpoint before starting")
	}

	var streamer *replication.BinlogStreamer
	if found && off.GTIDSet != "" {
		set, err := mysql.ParseGTIDSet(b.dbType.BinlogFlavor(), off.GTIDSet)
		if err != nil {
			return fmt.Errorf("parse checkpoint GTID set: %w", err)
		}
		streamer, err = syncer.StartSyncGTID(set)
		if err == nil {
			log.Printf("[binlog] stream started from GTID checkpoint")
		}
	} else if b.options.RequireGTID {
		set, parseErr := mysql.ParseGTIDSet(b.dbType.BinlogFlavor(), "")
		if parseErr != nil {
			return fmt.Errorf("create empty GTID set: %w", parseErr)
		}
		streamer, err = syncer.StartSyncGTID(set)
		if err == nil {
			log.Printf("[binlog] stream started from empty GTID checkpoint")
		}
	} else {
		startPos := mysql.Position{}
		if found {
			startPos = mysql.Position{Name: off.File, Pos: off.Pos}
			b.currentFile = off.File
		}
		streamer, err = syncer.StartSync(startPos)
		if err == nil {
			log.Printf("[binlog] stream started file=%s pos=%d", startPos.Name, startPos.Pos)
		}
	}
	if err != nil {
		return fmt.Errorf("start binlog stream: %w", err)
	}
	b.options.Status.StreamStarted()

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read binlog stream: %w", err)
		}
		if err := b.handleEvent(ctx, out, ev); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (b *BinlogInspector) handleEvent(ctx context.Context, out chan<- model.Event, ev *replication.BinlogEvent) error {
	b.options.Status.BinlogEventReceived(uint64(ev.Header.EventSize))
	eventTime := time.Unix(int64(ev.Header.Timestamp), 0)
	src := model.SourceType(b.dbType)

	switch e := ev.Event.(type) {
	case *replication.GTIDEvent:
		if ev.Header.EventType == replication.ANONYMOUS_GTID_EVENT {
			if b.options.RequireGTID {
				return errors.New("anonymous GTID event received while require_gtid is enabled")
			}
			b.currentGTID = ""
			b.currentTxID = fmt.Sprintf("anonymous:%d", ev.Header.LogPos)
			return nil
		}
		id, err := uuid.FromBytes(e.SID)
		if err != nil {
			return fmt.Errorf("decode GTID SID: %w", err)
		}
		if id == uuid.Nil || e.GNO <= 0 {
			if b.options.RequireGTID {
				return errors.New("invalid zero GTID received while require_gtid is enabled")
			}
			b.currentGTID = ""
			b.currentTxID = fmt.Sprintf("anonymous:%d", ev.Header.LogPos)
			return nil
		}
		b.currentGTID = fmt.Sprintf("%s:%d", id.String(), e.GNO)
		b.currentTxID = "gtid:" + b.currentGTID
		return nil

	case *replication.TableMapEvent:
		return b.onTableMap(e)

	case *replication.RowsEvent:
		return b.emitRowEventsContext(ctx, out, ev.Header, e)

	case *replication.RotateEvent:
		b.currentFile = string(e.NextLogName)
		return nil

	case *replication.XIDEvent:
		if b.currentTxID == "" {
			b.currentTxID = fmt.Sprintf("xid:%d", e.XID)
		}
		return b.emitBoundary(ctx, out, ev.Header.LogPos, eventTime, model.TxCommit, e.GSet)

	case *replication.QueryEvent:
		query := strings.TrimSpace(string(e.Query))
		upper := strings.ToUpper(query)
		switch upper {
		case "BEGIN":
			return nil
		case "COMMIT":
			return b.emitBoundary(ctx, out, ev.Header.LogPos, eventTime, model.TxCommit, e.GSet)
		case "ROLLBACK":
			return b.emitBoundary(ctx, out, ev.Header.LogPos, eventTime, model.TxRollback, e.GSet)
		}

		isDDL := strings.HasPrefix(upper, "ALTER TABLE") || strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "DROP TABLE") || strings.HasPrefix(upper, "TRUNCATE TABLE") || strings.HasPrefix(upper, "RENAME TABLE")
		if !isDDL {
			return nil
		}
		if b.currentTxID == "" {
			b.currentTxID = fmt.Sprintf("query:%d", ev.Header.LogPos)
		}

		if string(e.Schema) == b.schema {
			for key := range b.tableMeta {
				schema, table := splitKey(key)
				cols := b.fetchColumns(schema, table)
				if len(cols) > 0 {
					b.tableMeta[key].columns = cols
					if err := b.updatePKIndex(key); err != nil {
						return err
					}
				}
			}
			if b.options.CaptureDDL {
				offset := b.offset(ev.Header.LogPos, e.GSet)
				if err := sendEvent(ctx, out, model.NewBinlogDDLEvent(src, offset, eventTime, b.currentTxID, query)); err != nil {
					return err
				}
			}
		}

		return b.emitBoundary(ctx, out, ev.Header.LogPos, eventTime, model.TxCommit, e.GSet)
	}

	return nil
}

func (b *BinlogInspector) verifyGTIDMode(ctx context.Context) error {
	if !b.options.RequireGTID {
		return nil
	}
	if b.dbType != model.MySQL {
		return fmt.Errorf("require_gtid is supported only for mysql, got %s", b.dbType)
	}
	if b.db == nil {
		return errors.New("require_gtid needs a database connection for runtime verification")
	}
	var mode string
	if err := b.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
		return fmt.Errorf("verify MySQL GTID mode: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(mode), "ON") {
		return fmt.Errorf("MySQL gtid_mode must be ON when require_gtid is enabled, got %q", mode)
	}
	return nil
}

func (b *BinlogInspector) emitBoundary(ctx context.Context, out chan<- model.Event, pos uint32, eventTime time.Time, kind model.TxBoundaryKind, set mysql.GTIDSet) error {
	if b.currentTxID == "" {
		return nil
	}
	offset := b.offset(pos, set)
	if err := sendEvent(ctx, out, model.NewTransactionBoundaryEvent(model.SourceType(b.dbType), offset, eventTime, b.currentTxID, kind)); err != nil {
		return err
	}
	b.currentTxID = ""
	b.currentGTID = ""
	return nil
}

func (b *BinlogInspector) offset(pos uint32, set mysql.GTIDSet) model.MySQLOffset {
	offset := model.MySQLOffset{File: b.currentFile, Pos: pos, GTID: b.currentGTID}
	if set != nil {
		offset.GTIDSet = set.String()
	}
	return offset
}

func sendEvent(ctx context.Context, out chan<- model.Event, event model.Event) error {
	select {
	case out <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func requiredTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // MySQL REQUIRED mode has no managed CA bundle.
	}
}

func (b *BinlogInspector) onTableMap(e *replication.TableMapEvent) error {
	if isSystemSchema(e.Schema) {
		return nil
	}

	key := fmt.Sprintf("%s.%s", e.Schema, e.Table)
	meta, ok := b.tableMeta[key]
	if !ok {
		return nil
	}

	if len(e.ColumnName) == 0 {
		cols := b.fetchColumns(string(e.Schema), string(e.Table))
		if len(cols) == 0 {
			return fmt.Errorf("column metadata missing for configured table %s", key)
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
		return fmt.Errorf("configured primary key %s not found in table %s", meta.pkName, key)
	}
	return nil
}

func (b *BinlogInspector) emitRowEvents(out chan<- model.Event, h *replication.EventHeader, e *replication.RowsEvent) {
	_ = b.emitRowEventsContext(context.Background(), out, h, e)
}

func (b *BinlogInspector) emitRowEventsContext(ctx context.Context, out chan<- model.Event, h *replication.EventHeader, e *replication.RowsEvent) error {
	rowImages := uint64(len(e.Rows))
	b.options.Status.RowImagesReceived(rowImages)
	if isSystemSchema(e.Table.Schema) {
		b.options.Status.RowImagesFiltered(rowImages)
		return nil
	}

	table := fmt.Sprintf("%s.%s", e.Table.Schema, e.Table.Table)
	if b.currentTxID == "" {
		b.currentTxID = fmt.Sprintf("tx:%d", h.LogPos)
	}

	eventTime := time.Unix(int64(h.Timestamp), 0)
	meta, ok := b.tableMeta[table]
	if !ok {
		b.options.Status.RowImagesFiltered(rowImages)
		return nil
	}
	if meta.pkIndex < 0 || meta.pkIndex >= len(meta.columns) {
		return fmt.Errorf("configured primary key %s has no valid column index for table %s", meta.pkName, table)
	}

	offset := b.offset(h.LogPos, nil)
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
		b.options.Status.RowImagesFiltered(rowImages)
		return nil
	}

	var rowsData []model.RowData
	if op == model.OpUpdate {
		if len(e.Rows)%2 != 0 {
			return fmt.Errorf("invalid UPDATE_ROWS_EVENT rows=%d table=%s", len(e.Rows), table)
		}
		for i := 0; i < len(e.Rows); i += 2 {
			before := e.Rows[i]
			after := e.Rows[i+1]
			pk, err := extractPK(meta, before)
			if err != nil {
				return fmt.Errorf("extract primary key for table %s: %w", table, err)
			}
			rowsData = append(rowsData, model.RowData{
				PK:     pk,
				Before: rowToMap(meta.columns, before, meta.includeColumns, meta.excludeColumns),
				After:  rowToMap(meta.columns, after, meta.includeColumns, meta.excludeColumns),
			})
		}
	} else {
		for _, row := range e.Rows {
			pk, err := extractPK(meta, row)
			if err != nil {
				return fmt.Errorf("extract primary key for table %s: %w", table, err)
			}
			data := model.RowData{PK: pk}
			if op == model.OpInsert {
				data.After = rowToMap(meta.columns, row, meta.includeColumns, meta.excludeColumns)
			} else {
				data.Before = rowToMap(meta.columns, row, meta.includeColumns, meta.excludeColumns)
			}
			rowsData = append(rowsData, data)
		}
	}

	if len(rowsData) == 0 {
		b.options.Status.RowImagesFiltered(rowImages)
		return nil
	}
	b.options.Status.RowImagesCaptured(rowImages)
	return sendEvent(ctx, out, model.NewBinlogRowEvent(
		src,
		offset,
		eventTime,
		b.currentTxID,
		[]model.RowChange{{
			Schema: schema,
			Table:  tableName,
			Op:     op,
			Rows:   rowsData,
		}},
	))
}
