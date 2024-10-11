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
	ParticipateCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		Input              string `cmd:"" short:"i" help:"The input file containing the advertiser data to be hashed and encrypted. If given a directory, all files in the directory will be processed."`
		AdvertiserKey      string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		Output             string `cmd:"" short:"o" help:"The output file to write the advertiser data to, default to stdout."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is 8."`
	}
)

func (c *ParticipateCmd) Run(cli *CliContext) error {
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

	fs, err := io.FileReaders(c.Input)
	if err != nil {
		return fmt.Errorf("io.FileReaders: %w", err)
	}
	in := io.MultiReader(fs...)

	// Allow testing with local files.
	if c.Output != "" && !isGCSBucketURL(c.Output) {
		out, err := io.FileWriter(c.Output)
		if err != nil {
			return fmt.Errorf("io.FileWriter: %w", err)
		}

		rw, err := pair.NewPAIRIDReadWriter(in, out)
		if err != nil {
			return fmt.Errorf("NewPAIRIDReadWriter: %w", err)
		}

		return rw.HashEncrypt(ctx, c.NumThreads, saltStr, c.AdvertiserKey)
	}

	// get cleanroom output path
	clrConfig, err := client.GetConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get clean room: %w", err)
	}

	outputPath := clrConfig.GetAdvertiserTwiceEncryptedDataUrl()

	b, err := bucket.NewBucketReadWriter(ctx, gcsToken, outputPath, bucket.WithReader(in))
	if err != nil {
		return fmt.Errorf("bucket.NewBucket: %w", err)
	}
	defer b.Close()

	if len(b.ReadWriters) != 1 {
		return errors.New("failed to create NewBucket: invalid number of read writers")
	}

	pairRW, err := pair.NewPAIRIDReadWriter(b.FileReader, b.ReadWriters[0].Writer)
	if err != nil {
		return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
	}

	if err := pairRW.HashEncrypt(ctx, c.NumThreads, saltStr, c.AdvertiserKey); err != nil {
		return fmt.Errorf("pairRW.HashEncrypt: %w", err)
	}

	return b.Complete(ctx)
}
