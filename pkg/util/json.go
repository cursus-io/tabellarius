package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func SaveJSON[T any](path string, v T) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("create checkpoint temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secure checkpoint temp file: %w", err)
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write checkpoint temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync checkpoint temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint temp file: %w", err)
	}
	if err := replaceFile(tmpPath, path); err != nil {
		return fmt.Errorf("replace checkpoint: %w", err)
	}
	return nil
}

func LoadJSON[T any](path string) (T, bool) {
	v, found, err := LoadJSONStrict[T](path)
	return v, found && err == nil
}

func LoadJSONStrict[T any](path string) (T, bool, error) {
	var zero T

	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return zero, false, nil
		}
		return zero, false, fmt.Errorf("read checkpoint: %w", err)
	}

	if err := json.Unmarshal(b, &zero); err != nil {
		return zero, true, fmt.Errorf("decode checkpoint: %w", err)
	}

	return zero, true, nil
}
