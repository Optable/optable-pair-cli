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

func (c *pairConfig) hashEncryt(ctx context.Context, input string) error {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("Step 1: Hash and encrypt the advertiser data.")

	fs, err := io.FileReaders(input)
	if err != nil {
		return fmt.Errorf("io.FileReaders: %w", err)
	}
	in := io.MultiReader(fs...)

	b, err := bucket.NewBucketReadWriter(ctx, c.downscopedToken, c.advTwicePath, bucket.WithReader(in))
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

	if err := pairRW.HashEncrypt(ctx, c.threads, c.salt, c.key); err != nil {
		return fmt.Errorf("pairRW.HashEncrypt: %w", err)
	}

	bucketCompleter, err := bucket.NewBucketCompleter(ctx, c.downscopedToken, c.advTwicePath)
	if err != nil {
		return fmt.Errorf("bucket.NewBucketCompleter: %w", err)
	}
	defer bucketCompleter.Close()

	if err := bucketCompleter.Complete(ctx); err != nil {
		return fmt.Errorf("bucket.Complete: %w", err)
	}

	logger.Info().Msg("Step 1: Hash and encrypt the advertiser data completed.")

	return nil
}

func (c *pairConfig) reEncrypt(ctx context.Context) error {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("Step 2: Re-encrypt the publisher's hashed and encrypted PAIR IDs.")

	b, err := bucket.NewBucketReadWriter(ctx, c.downscopedToken, c.pubTriplePath, bucket.WithSourceURL(c.pubTwicePath))
	if err != nil {
		return fmt.Errorf("bucket.NewBucket: %w", err)
	}
	defer b.Close()

	for _, rw := range b.ReadWriters {
		pairRW, err := pair.NewPAIRIDReadWriter(rw.Reader, rw.Writer)
		if err != nil {
			return fmt.Errorf("pair.NewPAIRIDReadWriter: %w", err)
		}

		if err := pairRW.ReEncrypt(ctx, c.threads, c.salt, c.key); err != nil {
			return fmt.Errorf("pairRW.ReEncrypt: %w", err)
		}
	}

	bucketCompleter, err := bucket.NewBucketCompleter(ctx, c.downscopedToken, c.advTwicePath)
	if err != nil {
		return fmt.Errorf("bucket.NewBucketCompleter: %w", err)
	}
	defer bucketCompleter.Close()

	if err := bucketCompleter.Complete(ctx); err != nil {
		return fmt.Errorf("bucket.Complete: %w", err)
	}

	logger.Info().Msg("Step 2: Re-encrypt the publisher's hashed and encrypted PAIR IDs completed.")

	return nil
}

func (c *pairConfig) match(ctx context.Context, outputPath string) error {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("waiting for publisher to re-encrypt advertiser data")

	if err := c.cleanroomClient.ReadyForMatch(ctx); err != nil {
		return fmt.Errorf("failed to wait for publisher: %w", err)
	}

	logger.Info().Msg("Step 3: Match the two sets of triple encrypted PAIR IDs.")

	b, err := bucket.NewBucketReaders(ctx, c.downscopedToken, c.advTriplePath, c.pubTriplePath)
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
