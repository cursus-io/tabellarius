package inspector

import (
	"fmt"
	"log"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
)

func (b *BinlogInspector) parseDSN() error {
	parts := strings.Split(b.dsn, "@tcp(")
	if len(parts) != 2 {
		return fmt.Errorf("invalid dsn")
	}

	auth := strings.Split(parts[0], ":")
	addr := strings.Split(strings.TrimSuffix(parts[1], ")"), ":")

	b.user = auth[0]
	b.password = auth[1]
	b.host = addr[0]
	b.port = 3306

	if len(addr) == 2 {
		if _, err := fmt.Sscanf(addr[1], "%d", &b.port); err != nil {
			return fmt.Errorf("invalid port in DSN: %w", err)
		}
	}

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
	defer rows.Close()

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

func isDML(e *replication.QueryEvent) bool {
	q := string(e.Query)
	return strings.HasPrefix(q, "INSERT") || strings.HasPrefix(q, "UPDATE") || strings.HasPrefix(q, "DELETE")
}

func extractPK(meta *tableMeta, row []interface{}) map[string]any {
	if meta.pkIndex >= 0 && meta.pkIndex < len(row) {
		return map[string]any{meta.pkName: row[meta.pkIndex]}
	}

	if len(row) > 0 {
		return map[string]any{meta.pkName: row[0]}
	}

	return map[string]any{}
}

func rowToMap(cols []string, row []interface{}, includeColumns, excludeColumns map[string]struct{}) map[string]any {
	m := make(map[string]any, len(row))

	if len(cols) == 0 {
		for i, v := range row {
			m[fmt.Sprintf("col_%d", i)] = v
		}
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

func (b *BinlogInspector) updatePKIndex(key string) {
	meta, ok := b.tableMeta[key]
	if !ok {
		return
	}

	meta.pkIndex = -1
	for i, col := range meta.columns {
		if col == meta.pkName {
			meta.pkIndex = i
			break
		}
	}

	if meta.pkIndex == -1 {
		log.Printf("[binlog] pk %s not found in table %s after DDL, fallback to 0", meta.pkName, key)
		meta.pkIndex = 0
	}
}
