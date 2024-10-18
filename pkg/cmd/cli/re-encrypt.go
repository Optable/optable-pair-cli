package cli

import (
	"errors"
	"fmt"
	"os"

	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/pair"
)

type (
	ReEncryptCmd struct {
		PairCleanroomToken   string `arg:"" help:"The PAIR clean room token to use for the operation."`
		Input                string `cmd:"" short:"i" help:"The GCS bucket URL containing objects of publisher's encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		Output               string `cmd:"" short:"o" help:"The GCS bucket URL to write the re-encrypted publisher PAIR IDs to. If given a file path, it will write to the file instead. If not provided, it will write to stdout."`
		AdvertiserKey        string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		NumThreads           int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
		SavePublisherPAIRIDs bool   `cmd:"" short:"s" name:"save-publisher-pair-ids" help:"Save the publisher's PAIR IDs to a file named publisher_triple_encrypted_pair_ids.csv, to be used later. If not provided, the publisher's PAIR IDs will not be saved."`
	}
)

func (c *ReEncryptCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	if c.AdvertiserKey == "" {
		c.AdvertiserKey = cli.config.keyConfig.Key
		if c.AdvertiserKey == "" {
			return errors.New("advertiser key is required, please either provide one or generate one.")
		}
	}

	// instantiate pair config
	pairCfg, err := NewPAIRConfig(ctx, c.PairCleanroomToken, c.NumThreads, c.AdvertiserKey)
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
		if c.SavePublisherPAIRIDs {
			// create the publisher data directory if it does not exist
			if err := os.MkdirAll(publisherTripleEncryptedDataPath, os.ModePerm); err != nil {
				return fmt.Errorf("io.CreateDirectory: %w", err)
			}

			w, err := io.FileWriter(fmt.Sprintf("%s/pair_ids.csv", publisherTripleEncryptedDataPath))
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

	return pairCfg.reEncrypt(ctx, c.SavePublisherPAIRIDs)
}
