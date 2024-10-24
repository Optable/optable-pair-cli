package cli

import (
	"fmt"
	"os"

	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/pair"
)

type (
	MatchCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		AdvertiserInput    string `cmd:"" short:"a" help:"If given a file path, it will read from the file. If not provided, it will read from the GCS path specified from the token."`
		PublisherInput     string `cmd:"" short:"p" help:"If given a file path, it will read from the file. If not provided, it will read from the GCS path specified from the token."`
		OutputDir          string `cmd:"" short:"o" help:"The output directory path to write the decrypted and matched double encrypted PAIR IDs. Each thread will write one single file in the given directory path. If none are provided, all matched and decrypted PAIR IDs will be written to stdout."`
		AdvertiserKeyPath  string `cmd:"" short:"k" help:"The path to the advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
		PublisherPAIRIDs   string `cmd:"" short:"s" name:"publisher-pair-ids" help:"Use the publisher's PAIR IDs from a path."`
	}
)

func (c *MatchCmd) Help() string {
	return `
This operation produces the match rate of this PAIR clean room operation,
and output the list of decrypted and matched PAIR IDs.
`
}

func (c *MatchCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	advertiserKey, err := ReadKeyConfig(c.AdvertiserKeyPath, cli.config.keyConfig)
	if err != nil {
		return fmt.Errorf("ReadKeyConfig: %w", err)
	}

	pairCfg, err := NewPAIRConfig(ctx, c.PairCleanroomToken, c.NumThreads, advertiserKey)
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

	return pairCfg.match(ctx, c.OutputDir, c.PublisherPAIRIDs)
}

func readersFromReadClosers(rs []io.ReadCloser) []io.Reader {
	readers := make([]io.Reader, len(rs))
	for i, r := range rs {
		readers[i] = r
	}
	return readers
}
