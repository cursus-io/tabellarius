package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/tabellarius/pkg/model"
)

func TestSaveLoadJSON(t *testing.T) {
	tmp, _ := os.CreateTemp("", "json-*")
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(tmp.Name()) })

	v := model.MySQLOffset{
		File: "binlog.1",
		Pos:  123,
	}

	if err := SaveJSON(tmp.Name(), v); err != nil {
		t.Fatalf("SaveJSON failed: %v", err)
	}

	loaded, ok := LoadJSON[model.MySQLOffset](tmp.Name())
	if !ok {
		t.Fatal("LoadJSON failed")
	}

	if loaded.File != v.File || loaded.Pos != v.Pos {
		t.Fatal("loaded value mismatch")
	}
}

func TestLoadJSONStrictRejectsMalformedCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.json")
	if err := os.WriteFile(path, []byte("not-json"), 0600); err != nil {
		t.Fatal(err)
	}

	_, found, err := LoadJSONStrict[model.MySQLOffset](path)
	if !found {
		t.Fatal("malformed checkpoint must still be reported as present")
	}
	if err == nil {
		t.Fatal("expected malformed checkpoint error")
	}
}

func TestSaveJSONReplacesCheckpointWithoutLeavingTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offset.json")
	first := model.MySQLOffset{File: "binlog.1", Pos: 1}
	second := model.MySQLOffset{File: "binlog.2", Pos: 2}

	if err := SaveJSON(path, first); err != nil {
		t.Fatal(err)
	}
	if err := SaveJSON(path, second); err != nil {
		t.Fatal(err)
	}

	loaded, found, err := LoadJSONStrict[model.MySQLOffset](path)
	if err != nil || !found {
		t.Fatalf("LoadJSONStrict() found=%v err=%v", found, err)
	}
	if loaded != second {
		t.Fatalf("loaded = %+v, want %+v", loaded, second)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "offset.json" {
		t.Fatalf("unexpected checkpoint files: %+v", entries)
	}
}
