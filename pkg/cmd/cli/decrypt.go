package cli

import (
	"encoding/base64"
	"errors"
	"fmt"
	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/pair"
)

type (
	DecryptCmd struct {
		Input         string `cmd:"" short:"i" help:"The input file containing the matched triple encrypted PAIR IDs to be decrypted. If given a directory, all files in the directory will be processed."`
		AdvertiserKey string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		Output        string `cmd:"" short:"o" help:"The output file to write the decrypted PAIR IDs to, default to stdout."`
		NumThreads    int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
	}
)

func (c *DecryptCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	if c.AdvertiserKey == "" {
		c.AdvertiserKey = cli.config.keyConfig.Key
		if c.AdvertiserKey == "" {
			return errors.New("advertiser key is required, please either provide one or generate one.")
		}
	}

	fs, err := io.FileReaders(c.Input)
	if err != nil {
		return fmt.Errorf("io.FileReaders: %w", err)
	}
	in := io.MultiReader(fs...)

	out, err := io.FileWriter(c.Output)
	if err != nil {
		return fmt.Errorf("io.FileWriter: %w", err)
	}

	d, err := pair.NewPAIRIDReadWriter(in, out)
	if err != nil {
		return fmt.Errorf("pair.NewDecrypter: %w", err)
	}

	// no need for original salt
	salt := base64.StdEncoding.EncodeToString(make([]byte, 32))

	// Decrypt and write
	if err := d.Decrypt(ctx, c.NumThreads, salt, c.AdvertiserKey); err != nil {
		return fmt.Errorf("pair.Decrypt: %w", err)
	}

	return nil
}
