package cli

import (
	"errors"
	"fmt"

	"optable-pair-cli/pkg/bucket"
	"optable-pair-cli/pkg/internal"
	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/keys"
	"optable-pair-cli/pkg/pair"
)

type (
	MatchCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		AdvertiserInput    string `cmd:"" short:"a" help:"The GCS bucket URL containing objects of advertiser's triple encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		PublisherInput     string `cmd:"" short:"p" help:"The GCS bucket URL containing objects of publisher's triple encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		Output             string `cmd:"" short:"o" help:"The file path to write the decrypted and matched double encrypted PAIR IDs. If given a directory, the output will be files each containing up to 1 million IDs, if given a file, it will contain all the IDs. If none are provided, it will write to stdout."`
		AdvertiserKey      string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
	}
)

func (c *MatchCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	if c.PairCleanroomToken == "" {
		return errors.New("pair clean room token is required")
	}

	cleanroomToken, err := internal.ParseCleanroomToken(c.PairCleanroomToken)
	if err != nil {
		return fmt.Errorf("failed to parse clean room token: %w", err)
	}

	saltStr := cleanroomToken.HashSalt

	client, err := internal.NewCleanroomClient(cleanroomToken)
	if err != nil {
		return fmt.Errorf("failed to create clean room client: %w", err)
	}

	gcsToken, err := client.GetDownScopedToken(ctx)
	if err != nil {
		return fmt.Errorf("failed to get down scoped token: %w", err)
	}

	if c.AdvertiserKey == "" {
		c.AdvertiserKey = cli.config.keyConfig.Key
		if c.AdvertiserKey == "" {
			return errors.New("advertiser key is required, please either provide one or generate one.")
		}
	}

	// validate the private key
	if _, err := keys.NewPAIRPrivateKey(saltStr, c.AdvertiserKey); err != nil {
		return fmt.Errorf("failed to create PAIR private key: %w", err)
	}

	// Allow testing with local files.
	if c.AdvertiserInput != "" && c.PublisherInput != "" && !isGCSBucketURL(c.AdvertiserInput) && !isGCSBucketURL(c.PublisherInput) {
		adv, err := io.FileReaders(c.AdvertiserInput)
		if err != nil {
			return fmt.Errorf("fileReaders: %w", err)
		}

		pub, err := io.FileReaders(c.PublisherInput)
		if err != nil {
			return fmt.Errorf("fileWriters: %w", err)
		}

		matcher, err := pair.NewMatcher(adv, pub, c.Output)
		if err != nil {
			return fmt.Errorf("pair.NewMatcher: %w", err)
		}

		return matcher.Match(ctx, c.NumThreads, saltStr, c.AdvertiserKey)
	}

	// Get I/O from cleanroom configs
	clrConfig, err := client.GetConfig(ctx)
	if err != nil {
		return fmt.Errorf("client.GetCleanroom: %w", err)
	}

	advPath := clrConfig.GetAdvertiserTripleEncryptedDataUrl()
	pubPath := clrConfig.GetPublisherTripleEncryptedDataUrl()

	b, err := bucket.NewBucketReaders(ctx, gcsToken, advPath, pubPath)
	if err != nil {
		return fmt.Errorf("bucket.NewBucket: %w", err)
	}
	defer b.Close()

	matcher, err := pair.NewMatcher(readersFromReadClosers(b.AdvReader), readersFromReadClosers(b.PubReader), c.Output)
	if err != nil {
		return fmt.Errorf("pair.NewMatcher: %w", err)
	}

	return matcher.Match(ctx, c.NumThreads, saltStr, c.AdvertiserKey)
}

func readersFromReadClosers(rs []io.ReadCloser) []io.Reader {
	readers := make([]io.Reader, len(rs))
	for i, r := range rs {
		readers[i] = r
	}
	return readers
}
