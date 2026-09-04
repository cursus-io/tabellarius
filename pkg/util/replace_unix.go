//go:build !windows

package util

import "os"

func replaceFile(from, to string) error {
	return os.Rename(from, to)
}
