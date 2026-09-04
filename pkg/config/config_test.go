package config

import (
	"os"
	"strings"
	"testing"

	"github.com/cursus-io/tabellarius/pkg/model"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestLoadConfig(t *testing.T) {
	t.Setenv("TABELLARIUS_TEST_PASSWORD", "expanded-password")
	yaml := `
database:
  type: mysql
  schema: mydb
  user: root
  password: ${TABELLARIUS_TEST_PASSWORD}
  host: localhost
  port: 3306

cdc_log:
  table: cdc_log

tables:
  - name: users
    pk: id
  - name: orders
    pk: id

cdc_server:
  server_id: 9106001
  offset_file: offset.txt
  publisher_config: /config.yaml
  require_gtid: true
`

	tmp, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(tmp.Name()) })

	if _, err := tmp.WriteString(yaml); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(tmp.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Database.Type != model.MySQL {
		t.Fatalf("expected db type mysql, got %s", cfg.Database.Type)
	}

	if cfg.Database.Schema != "mydb" {
		t.Fatalf("unexpected schema: %s", cfg.Database.Schema)
	}
	if cfg.Database.Password != "expanded-password" {
		t.Fatalf("expected password environment expansion")
	}

	if len(cfg.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(cfg.Tables))
	}

	if cfg.CDCServer.OffsetFile != "offset.txt" {
		t.Fatalf("unexpected offset file: %s", cfg.CDCServer.OffsetFile)
	}
	if !cfg.CDCServer.RequireGTID {
		t.Fatal("expected require_gtid to be loaded")
	}
}

func TestResolveEnvironmentReferencePreservesLiteralDollarSigns(t *testing.T) {
	got, err := resolveEnvironmentReference("pa$$word")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "pa$$word" {
		t.Fatalf("unexpected literal password: %s", got)
	}
}

func TestResolveEnvironmentReferenceRejectsMissingVariable(t *testing.T) {
	if _, err := resolveEnvironmentReference("${TABELLARIUS_MISSING_TEST_PASSWORD}"); err == nil {
		t.Fatal("expected missing environment variable error")
	}
}

func TestDSN_MySQL(t *testing.T) {
	cfg := &Config{
		Database: Database{
			Type:     model.MySQL,
			Schema:   "mydb",
			User:     "user",
			Password: "pass",
			Host:     "localhost",
			Port:     3306,
		},
	}

	connection, err := cfg.DatabaseConnection()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := mysqldriver.ParseDSN(connection.DSN)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.User != "user" || parsed.Passwd != "pass" || parsed.Addr != "localhost:3306" || parsed.DBName != "mydb" || !parsed.ParseTime {
		t.Fatalf("unexpected parsed DSN: %+v", parsed)
	}
	if !strings.HasPrefix(parsed.TLSConfig, "tabellarius-") {
		t.Fatalf("unexpected TLS config name %q", parsed.TLSConfig)
	}
	if connection.TLSConfig == nil || !connection.TLSConfig.InsecureSkipVerify {
		t.Fatal("default required mode must encrypt without requiring a managed CA")
	}
}

func TestDSN_Postgres(t *testing.T) {
	cfg := &Config{
		Database: Database{
			Type:     model.Postgres,
			Schema:   "mydb",
			User:     "user",
			Password: "pass",
			Host:     "localhost",
			Port:     5432,
		},
	}

	connection, err := cfg.DatabaseConnection()
	if err != nil {
		t.Fatal(err)
	}
	dsn := connection.DSN
	expected := "postgres://user:pass@localhost:5432/mydb"

	if dsn != expected {
		t.Fatalf("unexpected dsn:\nexpected=%s\ngot=%s", expected, dsn)
	}
}

func TestDatabaseConnectionRejectsUnsupportedType(t *testing.T) {
	cfg := &Config{
		Database: Database{
			Type: "oracle",
		},
	}

	if _, err := cfg.DatabaseConnection(); err == nil {
		t.Fatal("expected unsupported database error")
	}
}

func TestDatabaseConnectionRequiresCAForVerifiedTLS(t *testing.T) {
	cfg := &Config{Database: Database{
		Type:    model.MySQL,
		Host:    "mysql.example",
		Port:    3306,
		TLSMode: "verify_identity",
	}}
	if _, err := cfg.DatabaseConnection(); err == nil || !strings.Contains(err.Error(), "tls_ca_file") {
		t.Fatalf("DatabaseConnection() error = %v", err)
	}

	cfg.Database.TLSMode = "invalid"
	if _, err := cfg.DatabaseConnection(); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("DatabaseConnection() error = %v", err)
	}
}
