package cli

import (
	"errors"
	"fmt"
	"net/url"

	"optable-pair-cli/pkg/bucket"
	"optable-pair-cli/pkg/internal"
	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/keys"
	"optable-pair-cli/pkg/pair"

	"gocloud.dev/blob/gcsblob"
)

type (
	ReEncryptCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		Input              string `cmd:"" short:"i" help:"The GCS bucket URL containing objects of publisher's encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		Output             string `cmd:"" short:"o" help:"The GCS bucket URL to write the re-encrypted publisher PAIR IDs to. If given a file path, it will write to the file instead. If not provided, it will write to stdout."`
		AdvertiserKey      string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
	}
)

func (c *ReEncryptCmd) Run(cli *CliContext) error {
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
	if c.Input != "" && c.Output != "" && !isGCSBucketURL(c.Input) && !isGCSBucketURL(c.Output) {
		in, err := io.FileReaders(c.Input)
		if err != nil {
			return fmt.Errorf("fileReaders: %w", err)
		}

		out, err := io.FileWriter(c.Output)
		if err != nil {
			return fmt.Errorf("fileWriters: %w", err)
		}

		rw, err := pair.NewPAIRIDReadWriter(io.MultiReader(in...), out)
		if err != nil {
			return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
		}

		return rw.ReEncrypt(ctx, c.NumThreads, saltStr, c.AdvertiserKey)
	}

	// Get I/O from cleanroom config
	clrConfig, err := client.GetConfig(ctx)
	if err != nil {
		return err
	}

	inputPath := clrConfig.GetPublisherTwiceEncryptedDataUrl()
	outputPath := clrConfig.GetPublisherTripleEncryptedDataUrl()

	b, err := bucket.NewBucketReadWriter(ctx, gcsToken, outputPath, bucket.WithSourceURL(inputPath))
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

func isGCSBucketURL(path string) bool {
	url, err := url.Parse(path)
	if err != nil {
		return false
	}

	return url.Scheme == gcsblob.Scheme
}
