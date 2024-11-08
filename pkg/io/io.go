package io

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"gocloud.dev/blob/gcsblob"
)

type (
	ReadCloser  = io.ReadCloser
	Reader      = io.Reader
	WriteCloser = io.WriteCloser
)

var EOF = io.EOF

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

func IsGCSBucketURL(path string) bool {
	url, err := url.Parse(path)
	if err != nil {
		return false
	}

	return url.Scheme == gcsblob.Scheme
}

func IsInputFileAboveCount(path string, threshold int) (bool, error) {
	fs, err := FileReaders(path)
	if err != nil {
		return false, fmt.Errorf("FileReaders: %w", err)
	}

	in := MultiReader(fs...)

	return ReadAboveCount(in, threshold)
}

func ReadAboveCount(r io.Reader, threshold int) (bool, error) {
	csvReader := csv.NewReader(r)

	count := 0
	for {
		_, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return false, err
		}

		count++

		if count > threshold {
			return true, nil
		}
	}

	return false, nil
}
