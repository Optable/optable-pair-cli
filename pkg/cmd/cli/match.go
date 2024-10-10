package cli

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"optable-pair-cli/pkg/bucket"
	cio "optable-pair-cli/pkg/cmd/cli/io"
	"optable-pair-cli/pkg/keys"
	"optable-pair-cli/pkg/pair"
)

type (
	MatchCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		// TODO(Justin): read token from GetCleanroom using the PairCleanroomToken.
		GCSToken        string `arg:"" help:"The GCS token to use for the operation."`
		AdvertiserInput string `cmd:"" short:"a" help:"The GCS bucket URL containing objects of advertiser's triple encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		PublisherInput  string `cmd:"" short:"p" help:"The GCS bucket URL containing objects of publisher's triple encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		Output          string `cmd:"" short:"o" help:"The file path to write the decrypted and matched double encrypted PAIR IDs. If given a directory, the output will be files each containing up to 1 million IDs, if given a file, it will contain all the IDs. If none are provided, it will write to stdout."`
		AdvertiserKey   string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		NumThreads      int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
	}
)

func (c *MatchCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

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

	// Allow testing with local files.
	if !isGCSBucketURL(c.AdvertiserInput) && !isGCSBucketURL(c.PublisherInput) {
		adv, err := cio.FileReaders(c.AdvertiserInput)
		if err != nil {
			return fmt.Errorf("fileReaders: %w", err)
		}

		pub, err := cio.FileReaders(c.PublisherInput)
		if err != nil {
			return fmt.Errorf("fileWriters: %w", err)
		}

		matcher, err := pair.NewMatcher(adv, pub, c.Output)
		if err != nil {
			return fmt.Errorf("pair.NewMatcher: %w", err)
		}

		return matcher.Match(ctx, c.NumThreads, saltStr, c.AdvertiserKey)
	}

	// TODO (Justin): use token to read from GCS.
	if c.GCSToken == "" {
		return errors.New("GCS token is required")
	}

	b, err := bucket.NewBucketReadWriter(ctx, c.GCSToken, c.Output, bucket.WithSourceURL(c.AdvertiserInput))
	if err != nil {
		return fmt.Errorf("bucket.NewBucket: %w", err)
	}
	defer b.Close()

	for _, rw := range b.ReadWriters {
		pairRW, err := pair.NewPAIRIDReadWriter(rw.Reader, rw.Writer)
		if err != nil {
			return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
		}

		if err := pairRW.ReEncrypt(ctx, c.NumThreads, saltStr, c.AdvertiserKey); err != nil {
			return fmt.Errorf("pairRW.ReEncrypt: %w", err)
		}
	}

	return b.Complete(ctx)
}
