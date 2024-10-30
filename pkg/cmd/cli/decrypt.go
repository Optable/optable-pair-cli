package cli

import (
	"encoding/base64"
	"fmt"
	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/pair"
)

type (
	DecryptCmd struct {
		Input             string `cmd:"" short:"i" help:"The input file containing the already matched triple encrypted PAIR IDs to be decrypted. If given a directory, all files in the directory will be processed."`
		AdvertiserKeyPath string `cmd:"" short:"k" name:"keypath" help:"The path to the advertiser clean room's private key to use for the operation. If not provided, the key saved in the configuration file will be used."`
		Output            string `cmd:"" short:"o" help:"The output file to write the resulting publisher decrypted PAIR IDs to. Defaults to stdout."`
		NumThreads        int    `cmd:"" short:"n" help:"The number of threads to use for the operation. Defaults to the number of the available cores on the machine."`
	}
)

func (c *DecryptCmd) Help() string {
	return `
Using the advertiser clean room private key, decrypt a specified list of
triple encrypted PAIR IDs to obtain a list of publisher PAIR IDs. The
specified list is assumed to contain previously matched PAIR IDs.

This command is useful only if you are computing the intersection of triple
encrypted PAIR IDs yourself (for example, by transferring the triple-encrypted
PAIR ID datasets to a database and running the intersection query there, then
downloading the result), and not when using the ` + "`opair run`" + ` command with the -o
flag.
	`
}

func (c *DecryptCmd) Run(cli *CliContext) error {
	ctx := cli.Context()
	if c.NumThreads <= 0 {
		c.NumThreads = defaultThreadCount
	}
	advertiserKey, err := ReadKeyConfig(c.AdvertiserKeyPath, cli.config.keyConfig)
	if err != nil {
		return fmt.Errorf("ReadKeyConfig: %w", err)
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
	if err := d.Decrypt(ctx, c.NumThreads, salt, advertiserKey); err != nil {
		return fmt.Errorf("pair.Decrypt: %w", err)
	}

	return nil
}
