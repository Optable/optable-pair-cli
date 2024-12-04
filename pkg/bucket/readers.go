package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	ErrInvalidBucketOptions = errors.New("invalid bucket options")
	ErrTokenRequired        = errors.New("downscopedToken is required")
)

type (

	// Readers contains the storage client and readers from two source buckets.
	Readers struct {
		client            *storage.Client
		AdvReader         []io.ReadCloser
		PubReader         []io.ReadCloser
		AdvPrefixedBucket *prefixedBucket
		PubPrefixedBucket *prefixedBucket
		PubFileReader     io.Reader
	}
)

func NewReaders(ctx context.Context, downScopedToken, advURL string, opts ...Option) (*Readers, error) {
	if downScopedToken == "" {
		return nil, errors.New("downscopedToken is required")
	}

	client, err := storage.NewClient(
		ctx,
		option.WithTokenSource(
			oauth2.StaticTokenSource(
				&oauth2.Token{
					AccessToken: downScopedToken,
				},
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	bucketOption := &bucketOptions{}
	for _, opt := range opts {
		opt(bucketOption)
	}

	advPrefixedBucket, err := bucketFromObjectURL(advURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URL: %w", err)
	}

	bucket := &Readers{
		client:            client,
		AdvPrefixedBucket: advPrefixedBucket,
	}

	if pubURL := bucketOption.sourceURL; pubURL != "" {
		pubPrefixedBucket, err := bucketFromObjectURL(pubURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse destination URL: %w", err)
		}

		bucket.PubPrefixedBucket = pubPrefixedBucket
	}

	if reader := bucketOption.reader; reader != nil {
		bucket.PubFileReader = reader
	}

	if err := bucket.newObjectReaders(ctx); err != nil {
		return nil, err
	}

	return bucket, nil
}

// newObjectReaders lists the objects specified by the advPrefixedBucket and pubPrefixedBucket and opens a reader for each object,
// except for the .Completed file.
func (b *Readers) newObjectReaders(ctx context.Context) error {
	advReaders, err := readersFromPrefixedBucket(ctx, b.client, b.AdvPrefixedBucket)
	if err != nil {
		return err
	}

	b.AdvReader = advReaders

	if b.PubFileReader != nil {
		b.PubReader = []io.ReadCloser{io.NopCloser(b.PubFileReader)}
		return nil
	}

	if b.PubPrefixedBucket == nil {
		return errors.New("missing publisher bucket URL")
	}

	pubReaders, err := readersFromPrefixedBucket(ctx, b.client, b.PubPrefixedBucket)
	if err != nil {
		return err
	}

	b.PubReader = pubReaders

	return nil
}

func readersFromPrefixedBucket(ctx context.Context, client *storage.Client, pBucket *prefixedBucket) ([]io.ReadCloser, error) {
	logger := zerolog.Ctx(ctx)
	query := &storage.Query{Prefix: pBucket.prefix + "/"}

	bucket := client.Bucket(pBucket.bucket)

	it := bucket.Objects(ctx, query)
	var readers []io.ReadCloser

	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			logger.Debug().Err(err).Msgf("failed to list objects from source bucket %s", pBucket.prefix)
			return nil, err
		}

		if strings.HasSuffix(obj.Name, CompletedFile) || strings.HasSuffix(obj.Name, "/") || obj.Size == 0 {
			continue
		}

		r, err := bucket.Object(obj.Name).NewReader(ctx)
		if err != nil {
			return nil, err
		}

		readers = append(readers, r)
	}

	return readers, nil
}

// Close closes the client and all read writers.
func (b *Readers) Close() error {
	for _, rc := range b.AdvReader {
		if err := rc.Close(); err != nil {
			return err
		}
	}

	for _, rc := range b.PubReader {
		if err := rc.Close(); err != nil {
			return err
		}
	}

	return b.client.Close()
}
