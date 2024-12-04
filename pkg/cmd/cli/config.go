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

func loadAllKeyConfigs(configPath string) (map[string]keys.KeyConfig, error) {
	if err := ensureKeyConfigPath(configPath); err != nil {
		return nil, err
	}

	// 0600: rw-------, only owner can read and write, but not execute.
	file, err := os.OpenFile(configPath, os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}
	defer file.Close()

	var configs map[string]keys.KeyConfig
	if err := json.NewDecoder(file).Decode(&configs); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, err
		}
		return nil, fmt.Errorf("json.Decode: %w", err)
	}
	return configs, nil
}

func LoadKeyConfig(context, configPath string, strict bool) (*Config, error) {
	configs, err := loadAllKeyConfigs(configPath)
	if errors.Is(err, io.EOF) {
		return &Config{configPath: configPath}, nil
	} else if err != nil {
		return nil, err
	}
	if config, ok := configs[context]; ok {
		return &Config{
			configPath: configPath,
			keyConfig:  &config,
		}, nil
	}
	if !strict {
		return &Config{configPath: configPath}, nil
	}
	return nil, errors.New("no key configuration found for the specified context")
}

func (c *CmdContext) SaveConfig(context string) error {
	configs, err := loadAllKeyConfigs(c.config.configPath)
	if errors.Is(err, io.EOF) {
		configs = make(map[string]keys.KeyConfig)
	} else if err != nil {
		return err
	}
	file, err := os.OpenFile(c.config.configPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("os.OpenFile: %w", err)
	}
	configs[context] = *c.config.keyConfig
	if err := json.NewEncoder(file).Encode(configs); err != nil {
		return fmt.Errorf("json.Encode: %w", err)
	}

	return nil
}

func ReadKeyConfig(context string, config *Config) (string, error) {
	config, err := LoadKeyConfig(context, config.configPath, true)
	if err != nil {
		return "", err
	}
	if config.keyConfig.Key == "" {
		return "", errors.New("malformed key configuration file, please regenerate the key")
	}

	return config.keyConfig.Key, nil
}
