package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"optable-pair-cli/pkg/keys"
	"os"
	"path/filepath"
	"runtime"
)

var defaultThreadCount = runtime.NumCPU()

type Config struct {
	// Path to the configuration file.
	configPath string
	// Key configuration.
	keyConfig *keys.KeyConfig
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

	var config keys.KeyConfig
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		if errors.Is(err, io.EOF) {
			return &Config{configPath: configPath}, nil
		} else {
			return nil, fmt.Errorf("json.Decode: %w", err)
		}
	}

	return &Config{
		configPath: configPath,
		keyConfig:  &config,
	}, nil
}

func (c *CliContext) SaveConfig() error {
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

func ReadKeyConfig(providedKeyPath string, defaultConfig *keys.KeyConfig) (string, error) {
	if providedKeyPath == "" {
		advertiserKey := defaultConfig.Key
		if advertiserKey == "" {
			return "", errors.New("advertiser key is required, please either provide one or generate one.")
		}
		return advertiserKey, nil
	}
	config, err := LoadKeyConfig(providedKeyPath)
	if err != nil {
		return "", err
	}
	if config.keyConfig.Key == "" {
		return "", errors.New("malformed key configuration file, please regenerate the key.")
	}

	return config.keyConfig.Key, nil
}
