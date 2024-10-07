package io

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func FileReaders(path string) (io.Reader, error) {
	if path == "" {
		return os.Stdin, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("os.Open: %w", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("os.Stat: %w", err)
	}

	// regular file
	if fi.IsDir() {
		var readers []io.Reader
		dirEntry, err := os.ReadDir(path)
		if err != nil {
			return nil, fmt.Errorf("os.ReadDir: %w", err)
		}

		for _, entry := range dirEntry {
			// ignore subdirectories
			if !entry.IsDir() {
				f, err := os.Open(filepath.Join(path, entry.Name()))
				if err != nil {
					return nil, fmt.Errorf("os.Open: %w", err)
				}

				readers = append(readers, f)
			}
		}

		return io.MultiReader(readers...), nil
	}

	return f, nil
}

func FileWriter(path string) (io.Writer, error) {
	if path == "" {
		return os.Stdout, nil
	}

	return os.Create(path)
}
