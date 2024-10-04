package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Config struct {
	// Path to the configuration file.
	configPath string
	// Key configuration.
	keyConfig *KeyConfig
}

type KeyConfig struct {
	// Unique identifier for the key.
	ID string `json:"id"`
	// base64 encoded key data
	Key string `json:"key"`
	// Key is created using which PAIR mode
	Mode string `json:"mode"`
	// timestamp of when the key was created
	// RFC3339 format
	// e.g. 2021-09-01T12:00:00Z
	CreatedAt string `json:"created_at"`
}

func ensureKeyConfigPath(configPath string) error {
	dir := filepath.Dir(configPath)

	// 0700: rwx------, owner can read, write, execute, but not group or other users.
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("os.MkdirAll(%s): %w", dir, err)
	}

	return nil
}

func LoadKeyConfig(configPath string) (*Config, error) {
	if err := ensureKeyConfigPath(configPath); err != nil {
		return nil, err
	}

	// 0600: rw-------, only owner can read and write, but not execute.
	file, err := os.OpenFile(configPath, os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}
	defer file.Close()

	var config KeyConfig
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		} else {
			return nil, fmt.Errorf("json.Decode: %w", err)
		}
	}

	return &Config{
		configPath: configPath,
		keyConfig:  &config,
	}, nil
}

func (c *CliContext) SaveCconfig() error {
	file, err := os.OpenFile(c.config.configPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("os.OpenFile: %w", err)
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(c.config.keyConfig); err != nil {
		return fmt.Errorf("json.Encode: %w", err)
	}

	return nil
}
