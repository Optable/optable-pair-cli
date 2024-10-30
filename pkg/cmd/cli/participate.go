package cli

import (
	"fmt"

	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/pair"
)

type (
	ParticipateCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		Input              string `cmd:"" short:"i" help:"The input file containing the advertiser data to be hashed and encrypted. If given a directory, all files in the directory will be processed."`
		AdvertiserKeyPath  string `cmd:"" short:"k" help:"The path to the advertiser private key to use for the operation. If not provided, the key saved in the configuration file will be used."`
		Output             string `cmd:"" short:"o" help:"The output file to write the advertiser data to, default to stdout."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Defaults to 1, and maximum is 8."`
	}
)

func (c *ParticipateCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	advertiserKey, err := ReadKeyConfig(c.AdvertiserKeyPath, cli.config.keyConfig)
	if err != nil {
		return fmt.Errorf("ReadKeyConfig: %w", err)
	}

	// instantiate pair config
	pairCfg, err := NewPAIRConfig(ctx, c.PairCleanroomToken, c.NumThreads, advertiserKey)
	if err != nil {
		return err
	}

	fs, err := io.FileReaders(c.Input)
	if err != nil {
		return fmt.Errorf("io.FileReaders: %w", err)
	}
	in := io.MultiReader(fs...)

	// Allow testing with local files.
	if !io.IsGCSBucketURL(c.Output) {
		out, err := io.FileWriter(c.Output)
		if err != nil {
			return fmt.Errorf("io.FileWriter: %w", err)
		}

		rw, err := pair.NewPAIRIDReadWriter(in, out)
		if err != nil {
			return fmt.Errorf("NewPAIRIDReadWriter: %w", err)
		}

		return rw.HashEncrypt(ctx, c.NumThreads, pairCfg.salt, pairCfg.key)
	}

	return pairCfg.hashEncryt(ctx, c.Input)
}
