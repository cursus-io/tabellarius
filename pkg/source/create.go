package source

import (
	"database/sql"
	"fmt"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/inspector"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/source/cursus"
	"github.com/cursus-io/tabellarius/pkg/util"
)

func NewFromConfig(db *sql.DB, cfg *config.Config) (*TabellariusSource, error) {
	switch cfg.Database.Type {
	case model.MySQL, model.MariaDB:
		return NewMySQLSource(db, cfg.Database.Type, cfg.Database.Schema, cfg.DSN(), cfg.CDCServer.OffsetFile, cfg.CDCServer.PublisherConfig, cfg.Tables)
	case model.Postgres:
		return nil, fmt.Errorf("postgres source not implemented")
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
	}
}

func NewMySQLSource(db *sql.DB, dbType model.DatabaseType, dbSchema, dbDSN string, offsetPath string, pubConfigPath string, tables []config.Table) (*TabellariusSource, error) {
	binlogOffset := offsetPath + ".binlog"
	ins, err := inspector.NewBinlogInspector(db, dbType, dbSchema, dbDSN, binlogOffset, util.GenerateID(), tables)
	if err != nil {
		return nil, fmt.Errorf("failed to create binlog inspector: %w", err)
	}

	var inspector inspector.Inspector[model.Event] = ins

	pub, err := cursus.NewCursusPublisher(pubConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	return &TabellariusSource{
		ins: inspector,
		pub: pub,
	}, nil
}
