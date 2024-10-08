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

	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("os.Stat: %w", err)
	}

	if !fi.IsDir() {
		return os.Open(path)
	}

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

func FileWriter(path string) (io.Writer, error) {
	if path == "" {
		return os.Stdout, nil
	}

	return os.Create(path)
}
