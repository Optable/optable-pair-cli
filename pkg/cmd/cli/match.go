package cli

import (
	"errors"
	"fmt"
	"os"

	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/pair"
)

type (
	MatchCmd struct {
		PairCleanroomToken       string `arg:"" help:"The PAIR clean room token to use for the operation."`
		AdvertiserInput          string `cmd:"" short:"a" help:"If given a file path, it will read from the file. If not provided, it will read from the GCS path specified from the token."`
		PublisherInput           string `cmd:"" short:"p" help:"If given a file path, it will read from the file. If not provided, it will read from the GCS path specified from the token."`
		OutputDir                string `cmd:"" short:"o" help:"The output directory path to write the decrypted and matched double encrypted PAIR IDs. Each thread will write one single file in the given directory path. If none are provided, all matched and decrypted PAIR IDs will be written to stdout."`
		AdvertiserKey            string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		NumThreads               int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
		UseSavedPublisherPAIRIDs bool   `cmd:"" short:"s" help:"If set, it will use the saved publisher PAIR IDs locally in the publisher_triple_encrypted_data directory instead of fetching from GCS."`
	}
)

func (c *MatchCmd) Help() string {
	return `
This operation produces the match rate of this PAIR clean room operation,
and output the list of decrypted and matched PAIR IDs.

Please be aware that this operation is for demo purposes only for
resonable data size.
`
}

func (c *MatchCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	if c.AdvertiserKey == "" {
		c.AdvertiserKey = cli.config.keyConfig.Key
		if c.AdvertiserKey == "" {
			return errors.New("advertiser key is required, please either provide one or generate one.")
		}
	}

	pairCfg, err := NewPAIRConfig(ctx, c.PairCleanroomToken, c.NumThreads, c.AdvertiserKey)
	if err != nil {
		return err
	}

	// Allow testing with local files.
	if c.AdvertiserInput != "" && c.PublisherInput != "" && !io.IsGCSBucketURL(c.AdvertiserInput) && !io.IsGCSBucketURL(c.PublisherInput) {
		adv, err := io.FileReaders(c.AdvertiserInput)
		if err != nil {
			return fmt.Errorf("fileReaders: %w", err)
		}

		pub, err := io.FileReaders(c.PublisherInput)
		if err != nil {
			return fmt.Errorf("fileWriters: %w", err)
		}

		if c.OutputDir != "" {
			if err := os.MkdirAll(c.OutputDir, os.ModePerm); err != nil {
				return fmt.Errorf("os.MkdirAll: %w", err)
			}
		}

		matcher, err := pair.NewMatcher(adv, pub, c.OutputDir)
		if err != nil {
			return fmt.Errorf("pair.NewMatcher: %w", err)
		}

		return matcher.Match(ctx, c.NumThreads, pairCfg.salt, pairCfg.key)
	}

	return pairCfg.match(ctx, c.OutputDir, c.UseSavedPublisherPAIRIDs)
}

func readersFromReadClosers(rs []io.ReadCloser) []io.Reader {
	readers := make([]io.Reader, len(rs))
	for i, r := range rs {
		readers[i] = r
	}
	return readers
}
