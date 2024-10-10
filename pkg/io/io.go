package io

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type (
	ReadCloser = io.ReadCloser
	Reader     = io.Reader
)

func MultiReader(readers ...io.Reader) io.Reader {
	return io.MultiReader(readers...)
}

func FileReaders(path string) ([]io.Reader, error) {
	if path == "" {
		return []io.Reader{os.Stdin}, nil
	}

	isDir, err := IsDir(path)
	if err != nil {
		return nil, fmt.Errorf("isDir: %w", err)
	}

	if !isDir {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("os.Open: %w", err)
		}

		return []io.Reader{f}, nil
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

	return readers, nil
}

func FileWriter(path string) (io.Writer, error) {
	if path == "" {
		return os.Stdout, nil
	}

	return os.Create(path)
}

func IsDir(path string) (bool, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("os.Stat: %w", err)
	}

	return fi.IsDir(), nil
}
