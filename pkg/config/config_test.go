package config

import (
	"os"
	"testing"

	"github.com/cursus-io/tabellarius/pkg/model"
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
`

	tmp, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.WriteString(yaml); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmp.Close()

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

	dsn := cfg.DSN()
	expected := "user:pass@tcp(localhost:3306)/mydb?parseTime=true&tls=skip-verify"

	if dsn != expected {
		t.Fatalf("unexpected dsn:\nexpected=%s\ngot=%s", expected, dsn)
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

	dsn := cfg.DSN()
	expected := "postgres://user:pass@localhost:5432/mydb"

	if dsn != expected {
		t.Fatalf("unexpected dsn:\nexpected=%s\ngot=%s", expected, dsn)
	}
}

func TestDSN_UnsupportedType_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic, but did not panic")
		}
	}()

	cfg := &Config{
		Database: Database{
			Type: "oracle",
		},
	}

	_ = cfg.DSN()
}
