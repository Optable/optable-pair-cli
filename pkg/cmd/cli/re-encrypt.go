package cli

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"

	"optable-pair-cli/pkg/bucket"
	cio "optable-pair-cli/pkg/cmd/cli/io"
	"optable-pair-cli/pkg/keys"
	"optable-pair-cli/pkg/pair"

	"gocloud.dev/blob/gcsblob"
)

type (
	ReEncryptCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		// TODO(Justin): read token from GetCleanroom using the PairCleanroomToken.
		GCSToken      string `arg:"" help:"The GCS token to use for the operation."`
		Input         string `cmd:"" short:"i" help:"The GCS bucket URL containing objects of publisher's encrypted PAIR IDs. If given a file path, it will read from the file instead. If not provided, it will read from stdin."`
		Output        string `cmd:"" short:"o" help:"The GCS bucket URL to write the re-encrypted publisher PAIR IDs to. If given a file path, it will write to the file instead. If not provided, it will write to stdout."`
		AdvertiserKey string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		NumThreads    int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
	}
)

func (c *ReEncryptCmd) Run(cli *CliContext) error {
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
	if !isGCSBucketURL(c.Input) && !isGCSBucketURL(c.Output) {
		in, err := cio.FileReaders(c.Input)
		if err != nil {
			return fmt.Errorf("fileReaders: %w", err)
		}

		out, err := cio.FileWriter(c.Output)
		if err != nil {
			return fmt.Errorf("fileWriters: %w", err)
		}

		rw, err := pair.NewPAIRIDReadWriter(io.MultiReader(in...), out)
		if err != nil {
			return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
		}

		return rw.ReEncrypt(ctx, c.NumThreads, saltStr, c.AdvertiserKey)
	}

	// TODO (Justin): use token to read and write to GCS.
	if c.GCSToken == "" {
		return errors.New("GCS token is required")
	}

	if !isGCSBucketURL(c.Output) {
		return errors.New("output must be a GCS bucket URL")
	}

	b, err := bucket.NewBucketReadWriter(ctx, c.GCSToken, c.Output, bucket.WithSourceURL(c.Input))
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
