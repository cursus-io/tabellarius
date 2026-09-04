package source

import (
	"database/sql"
	"fmt"

	"github.com/cursus-io/tabellarius/pkg/config"
	"github.com/cursus-io/tabellarius/pkg/health"
	"github.com/cursus-io/tabellarius/pkg/inspector"
	"github.com/cursus-io/tabellarius/pkg/model"
	"github.com/cursus-io/tabellarius/pkg/source/cursus"
)

func NewFromConfig(db *sql.DB, cfg *config.Config) (*TabellariusSource, error) {
	return NewFromConfigWithStatus(db, cfg, nil)
}

func NewFromConfigWithStatus(db *sql.DB, cfg *config.Config, status *health.Status) (*TabellariusSource, error) {
	connection, err := cfg.DatabaseConnection()
	if err != nil {
		return nil, fmt.Errorf("prepare database connection: %w", err)
	}
	switch cfg.Database.Type {
	case model.MySQL, model.MariaDB:
		if cfg.CDCServer.ServerID == 0 {
			return nil, fmt.Errorf("server_id must be non-zero")
		}
		if len(cfg.Tables) == 0 {
			return nil, fmt.Errorf("at least one capture table must be configured")
		}
		return NewMySQLSource(
			db,
			cfg.Database.Type,
			cfg.Database.Schema,
			connection.DSN,
			cfg.CDCServer.OffsetFile,
			cfg.CDCServer.PublisherConfig,
			cfg.CDCServer.ServerID,
			cfg.Tables,
			inspector.BinlogInspectorOptions{
				RequireExistingCheckpoint: cfg.CDCServer.RequireExistingCheckpoint,
				RequireGTID:               cfg.CDCServer.RequireGTID,
				CaptureDDL:                cfg.CDCServer.CaptureDDL,
				Status:                    status,
				TLSConfig:                 connection.TLSConfig,
			},
			cfg.CDCServer.AllowSingleReplicaPublisher,
			status,
		)
	case model.Postgres:
		return nil, fmt.Errorf("postgres source not implemented")
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
	}
}

func NewMySQLSource(db *sql.DB, dbType model.DatabaseType, dbSchema, dbDSN string, offsetPath string, pubConfigPath string, serverID uint32, tables []config.Table, options inspector.BinlogInspectorOptions, allowSingleReplicaPublisher bool, status *health.Status) (*TabellariusSource, error) {
	binlogOffset := offsetPath + ".binlog"
	ins, err := inspector.NewBinlogInspectorWithOptions(
		db,
		dbType,
		dbSchema,
		dbDSN,
		binlogOffset,
		serverID,
		tables,
		options,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create binlog inspector: %w", err)
	}

	var inspector inspector.Inspector[model.Event] = ins

	pub, err := cursus.NewCursusPublisherWithOptions(pubConfigPath, cursus.PublisherOptions{
		AllowSingleReplica: allowSingleReplicaPublisher,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	return &TabellariusSource{
		ins:            inspector,
		pub:            pub,
		checkpointPath: binlogOffset,
		status:         status,
	}, nil
}
