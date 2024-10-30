package cli

import (
	"fmt"
	"os"

	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/pair"
)

type (
	ReEncryptCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		Input              string `cmd:"" short:"i" help:"The GCS bucket URL containing objects of publisher's encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		Output             string `cmd:"" short:"o" help:"The GCS bucket URL to write the re-encrypted publisher PAIR IDs to. If given a file path, it will write to the file instead. If not provided, it will write to stdout."`
		AdvertiserKeyPath  string `cmd:"" short:"k" help:"The path to the advertiser private key to use for the operation. If not provided, the key saved in the configuration file will be used."`
		NumThreads         int    `cmd:"" short:"n" help:"The number of threads to use for the operation. Defaults to the number of the available cores on the machine."`
		PublisherPAIRIDs   string `cmd:"" short:"s" name:"publisher-pair-ids" help:"Save the publisher's PAIR IDs in the provided directory, to be used later. If not provided, the publisher's PAIR IDs will not be saved."`
	}
)

func (c *ReEncryptCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	advertiserKey, err := ReadKeyConfig(c.AdvertiserKeyPath, cli.config.keyConfig)
	if err != nil {
		return fmt.Errorf("ReadKeyConfig: %w", err)
	}
	if c.NumThreads == 0 {
		c.NumThreads = defaultThreadCount
	}
	// instantiate pair config
	pairCfg, err := NewPAIRConfig(ctx, c.PairCleanroomToken, c.NumThreads, advertiserKey)
	if err != nil {
		return err
	}

	// Allow testing with local files.
	if !io.IsGCSBucketURL(c.Input) && !io.IsGCSBucketURL(c.Output) {
		in, err := io.FileReaders(c.Input)
		if err != nil {
			return fmt.Errorf("fileReaders: %w", err)
		}

		out, err := io.FileWriter(c.Output)
		if err != nil {
			return fmt.Errorf("fileWriters: %w", err)
		}

		opts := []pair.ReadWriterOption{}
		if c.PublisherPAIRIDs != "" {
			// create the publisher data directory if it does not exist
			if err := os.MkdirAll(c.PublisherPAIRIDs, os.ModePerm); err != nil {
				return fmt.Errorf("io.CreateDirectory: %w", err)
			}

			w, err := io.FileWriter(fmt.Sprintf("%s/pair_ids.csv", c.PublisherPAIRIDs))
			if err != nil {
				return fmt.Errorf("fileWriters: %w", err)
			}

			opts = append(opts, pair.WithSecondaryWriter(w))
		}

		rw, err := pair.NewPAIRIDReadWriter(io.MultiReader(in...), out, opts...)
		if err != nil {
			return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
		}

		return rw.ReEncrypt(ctx, c.NumThreads, pairCfg.salt, pairCfg.key)
	}

	return pairCfg.reEncrypt(ctx, c.PublisherPAIRIDs)
}
