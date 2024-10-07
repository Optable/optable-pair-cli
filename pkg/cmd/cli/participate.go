package cli

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"optable-pair-cli/pkg/keys"
	"optable-pair-cli/pkg/pair"
	"os"
	"path/filepath"
)

type (
	ParticipateCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		Input              string `cmd:"" short:"i" help:"The input file containing the advertiser data to be hashed and encrypted. If given a directory, all files in the directory will be processed."`
		AdvertiserKey      string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		Output             string `cmd:"" short:"o" help:"The output file to write the advertiser data to, default to stdout."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
	}
)

func (c *ParticipateCmd) Run(cli *CliContext) error {
	if c.PairCleanroomToken == "" {
		return errors.New("pair clean room token is required")
	}

	if c.AdvertiserKey == "" {
		c.AdvertiserKey = cli.config.keyConfig.Key
		if c.AdvertiserKey == "" {
			return errors.New("advertiser key is required, please either provide one or generate one.")
		}
	}

	// TODO(Justin): read salt from token.
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("failed to generate salt: %w", err)
	}

	saltStr := base64.StdEncoding.EncodeToString(salt)

	// validate the private key
	if _, err := keys.NewPAIRPrivateKey(saltStr, c.AdvertiserKey); err != nil {
		return fmt.Errorf("failed to create PAIR private key: %w", err)
	}

	var (
		in  io.Reader
		out io.Writer
		err error
	)
	if c.Input == "" {
		in = os.Stdin
	} else {
		f, err := os.Open(c.Input)
		if err != nil {
			return fmt.Errorf("failed to open input file: %w", err)
		}

		fi, err := os.Stat(c.Input)
		if err != nil {
			return fmt.Errorf("failed to stat input file: %w", err)
		}

		// regular file
		if fi.IsDir() {
			var readers []io.Reader
			dirEntry, err := os.ReadDir(c.Input)
			if err != nil {
				return fmt.Errorf("failed to read directory: %w", err)
			}

			for _, entry := range dirEntry {
				// ignore subdirectories
				if !entry.IsDir() {
					f, err := os.Open(filepath.Join(c.Input, entry.Name()))
					if err != nil {
						return fmt.Errorf("failed to open file: %w", err)
					}

					readers = append(readers, f)
				}
			}

			in = io.MultiReader(readers...)
		} else {
			in = f
		}
	}

	// TODO(Justin): write to GCS bucket url from Cleanroom passed by token.
	if c.Output == "" {
		out = os.Stdout
	} else {
		out, err = os.Create(c.Output)
		if err != nil {
			return fmt.Errorf("failed to open output file: %w", err)
		}
	}

	if err := pair.HashEncrypt(cli.Context(), in, out, c.NumThreads, saltStr, c.AdvertiserKey); err != nil {
		return fmt.Errorf("HashEncrypt: %w", err)
	}

	return nil
}
