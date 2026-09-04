package inspector

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func (b *BinlogInspector) parseDSN() error {
	parsed, err := mysqldriver.ParseDSN(b.dsn)
	if err != nil {
		return fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if parsed.Net != "tcp" {
		return fmt.Errorf("MySQL replication requires tcp DSN, got %q", parsed.Net)
	}
	host, portValue, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		return fmt.Errorf("parse MySQL address: %w", err)
	}
	port, err := strconv.ParseUint(portValue, 10, 16)
	if err != nil {
		return fmt.Errorf("parse MySQL port: %w", err)
	}
	b.user = parsed.User
	b.password = parsed.Passwd
	b.host = host
	b.port = uint16(port)

	return nil
}

func (b *BinlogInspector) fetchColumns(schema, table string) []string {
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := b.db.Query(query, schema, table)
	if err != nil {
		log.Printf("[binlog] failed to query columns for table %s.%s: %v", schema, table, err)
		return nil
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("[binlog] failed to close column metadata rows: %v", err)
		}
	}()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			log.Printf("[binlog] failed to scan column: %v", err)
			continue
		}
		cols = append(cols, col)
	}

	return cols
}

func extractPK(meta *tableMeta, row []interface{}) (map[string]any, error) {
	if meta.pkIndex < 0 || meta.pkIndex >= len(row) {
		return nil, fmt.Errorf("primary key column %s is absent from row image", meta.pkName)
	}
	return map[string]any{meta.pkName: row[meta.pkIndex]}, nil
}

func rowToMap(cols []string, row []interface{}, includeColumns, excludeColumns map[string]struct{}) map[string]any {
	m := make(map[string]any, len(row))

	if len(cols) == 0 {
		return m
	}

	for i, c := range cols {
		if len(includeColumns) > 0 {
			if _, included := includeColumns[c]; !included {
				continue
			}
		}
		if _, excluded := excludeColumns[c]; excluded {
			continue
		}
		if i < len(row) {
			m[c] = row[i]
		} else {
			m[c] = nil
		}
	}

	return m
}

func bytesToStrings(b [][]byte) []string {
	out := make([]string, len(b))
	for i := range b {
		out[i] = string(b[i])
	}
	return out
}

func isSystemSchema(schema []byte) bool {
	s := string(schema)
	switch s {
	case "mysql", "performance_schema", "information_schema", "sys":
		return true
	default:
		return false
	}
}

func splitKey(key string) (string, string) {
	parts := strings.Split(key, ".")
	if len(parts) != 2 {
		return "", parts[0]
	}
	return parts[0], parts[1]
}

func (b *BinlogInspector) updatePKIndex(key string) error {
	meta, ok := b.tableMeta[key]
	if !ok {
		return nil
	}

	meta.pkIndex = -1
	for i, col := range meta.columns {
		if col == meta.pkName {
			meta.pkIndex = i
			break
		}
	}

	if meta.pkIndex == -1 {
		return fmt.Errorf("configured primary key %s not found in table %s after DDL", meta.pkName, key)
	}
	return nil
}
