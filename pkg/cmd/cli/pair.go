package cli

import (
	"context"
	"errors"
	"fmt"
	"optable-pair-cli/pkg/bucket"
	"optable-pair-cli/pkg/internal"
	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/keys"
	"optable-pair-cli/pkg/pair"
	"os"

	"github.com/rs/zerolog"
)

type pairConfig struct {
	downscopedToken string
	threads         int
	salt            string
	key             string
	cleanroomClient *internal.CleanroomClient
	advTwicePath    string
	advTriplePath   string
	pubTwicePath    string
	pubTriplePath   string
}

func NewPAIRConfig(ctx context.Context, token string, threads int, key string) (*pairConfig, error) {
	if token == "" {
		return nil, errors.New("pair clean room token is required")
	}

	cleanroomToken, err := internal.ParseCleanroomToken(token)
	if err != nil {
		return nil, fmt.Errorf("failed to parse clean room token: %w", err)
	}

	// validate the private key
	if _, err := keys.NewPAIRPrivateKey(cleanroomToken.HashSalt, key); err != nil {
		return nil, fmt.Errorf("failed to create PAIR private key: %w", err)
	}

	client, err := internal.NewCleanroomClient(cleanroomToken)
	if err != nil {
		return nil, fmt.Errorf("failed to create clean room client: %w", err)
	}

	gcsToken, err := client.GetDownScopedToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get down scoped token: %w", err)
	}

	clrConfig, err := client.GetConfig(ctx)
	if err != nil {
		return nil, err
	}

	return &pairConfig{
		downscopedToken: gcsToken,
		threads:         threads,
		salt:            cleanroomToken.HashSalt,
		key:             key,
		cleanroomClient: client,
		advTwicePath:    clrConfig.GetAdvertiserTwiceEncryptedDataUrl(),
		advTriplePath:   clrConfig.GetAdvertiserTripleEncryptedDataUrl(),
		pubTwicePath:    clrConfig.GetPublisherTwiceEncryptedDataUrl(),
		pubTriplePath:   clrConfig.GetPublisherTripleEncryptedDataUrl(),
	}, nil
}

func (c *pairConfig) hashEncryt(ctx context.Context, input string) (err error) {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("Step 1: Hash and encrypt the advertiser data.")

	fs, err := io.FileReaders(input)
	if err != nil {
		return fmt.Errorf("io.FileReaders: %w", err)
	}
	in := io.MultiReader(fs...)

	// defer statements are executed in Last In First Out order, so we will write the completed file last.
	bucketCompleter, err := bucket.NewBucketCompleter(ctx, c.downscopedToken, c.advTwicePath)
	if err != nil {
		return fmt.Errorf("bucket.NewBucketCompleter: %w", err)
	}
	defer func() {
		if err != nil {
			return
		}

		if err := bucketCompleter.Complete(ctx); err != nil {
			logger.Error().Err(err).Msg("failed to write .Completed file to bucket")
			return
		}
	}()

	b, err := bucket.NewBucketReadWriter(ctx, c.downscopedToken, c.advTwicePath, bucket.WithReader(in))
	if err != nil {
		return fmt.Errorf("bucket.NewBucket: %w", err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close bucket")
			return
		}
	}()

	if len(b.ReadWriters) != 1 {
		return errors.New("failed to create NewBucket: invalid number of read writers")
	}

	pairRW, err := pair.NewPAIRIDReadWriter(b.FileReader, b.ReadWriters[0].Writer)
	if err != nil {
		return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
	}

	if err := pairRW.HashEncrypt(ctx, c.threads, c.salt, c.key); err != nil {
		return fmt.Errorf("pairRW.HashEncrypt: %w", err)
	}

	logger.Info().Msg("Step 1: Hash and encrypt the advertiser data completed.")

	return
}

func (c *pairConfig) reEncrypt(ctx context.Context, publisherPAIRIDsPath string) (err error) {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("Step 2: Re-encrypt the publisher's hashed and encrypted PAIR IDs.")

	// defer statements are executed in Last In First Out order, so we will write the completed file last.
	bucketCompleter, err := bucket.NewBucketCompleter(ctx, c.downscopedToken, c.pubTriplePath)
	if err != nil {
		return fmt.Errorf("bucket.NewBucketCompleter: %w", err)
	}
	defer func() {
		if err != nil {
			return
		}

		if err := bucketCompleter.Complete(ctx); err != nil {
			logger.Error().Err(err).Msg("failed to write .Completed file to bucket")
			return
		}
	}()

	b, err := bucket.NewBucketReadWriter(ctx, c.downscopedToken, c.pubTriplePath, bucket.WithSourceURL(c.pubTwicePath))
	if err != nil {
		return fmt.Errorf("bucket.NewBucket: %w", err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close bucket")
			return
		}
	}()

	if publisherPAIRIDsPath != "" {
		// create the publisher data directory if it does not exist
		if err := os.MkdirAll(publisherPAIRIDsPath, os.ModePerm); err != nil {
			return fmt.Errorf("io.CreateDirectory: %w", err)
		}
	}

	for i, rw := range b.ReadWriters {
		opt := []pair.ReadWriterOption{}
		if publisherPAIRIDsPath != "" {
			w, err := io.FileWriter(fmt.Sprintf("%s/pair_ids_%d.csv", publisherPAIRIDsPath, i))
			if err != nil {
				return fmt.Errorf("io.FileWriter: %w", err)
			}

			opt = append(opt, pair.WithSecondaryWriter(w))
		}

		pairRW, err := pair.NewPAIRIDReadWriter(rw.Reader, rw.Writer, opt...)
		if err != nil {
			return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
		}

		if err := pairRW.ReEncrypt(ctx, c.threads, c.salt, c.key); err != nil {
			return fmt.Errorf("pairRW.ReEncrypt: %w", err)
		}
	}

	logger.Info().Msg("Step 2: Re-encrypt the publisher's hashed and encrypted PAIR IDs completed.")

	return
}

func (c *pairConfig) match(ctx context.Context, outputPath string, publisherPAIRIDsPath string) error {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("waiting for publisher to re-encrypt advertiser data")

	if err := c.cleanroomClient.ReadyForMatch(ctx); err != nil {
		return fmt.Errorf("failed to wait for publisher: %w", err)
	}

	logger.Info().Msg("Step 3: Match the two sets of triple encrypted PAIR IDs.")

	if outputPath != "" {
		if err := os.MkdirAll(outputPath, os.ModePerm); err != nil {
			return fmt.Errorf("os.MkdirAll: %w", err)
		}
	}

	opts := []bucket.BucketOption{}
	if publisherPAIRIDsPath != "" {
		fs, err := io.FileReaders(publisherPAIRIDsPath)
		if err != nil {
			return fmt.Errorf("io.FileReaders: %w", err)
		}

		opts = append(opts, bucket.WithReader(io.MultiReader(fs...)))
	} else {
		opts = append(opts, bucket.WithSourceURL(c.pubTriplePath))
	}

	b, err := bucket.NewBucketReaders(ctx, c.downscopedToken, c.advTriplePath, opts...)
	if err != nil {
		return fmt.Errorf("bucket.NewBucket: %w", err)
	}
	defer b.Close()

	matcher, err := pair.NewMatcher(readersFromReadClosers(b.AdvReader), readersFromReadClosers(b.PubReader), outputPath)
	if err != nil {
		return fmt.Errorf("pair.NewMatcher: %w", err)
	}

	if err := matcher.Match(ctx, c.threads, c.salt, c.key); err != nil {
		return fmt.Errorf("matcher.Match: %w", err)
	}

	logger.Info().Msg("Step 3: Match the two sets of triple encrypted PAIR IDs completed.")

	return nil
}
