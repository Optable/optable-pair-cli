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

	//BucketReaders contains the storage client and readers from two source buckets.
	BucketReaders struct {
		client            *storage.Client
		AdvReader         []io.ReadCloser
		PubReader         []io.ReadCloser
		AdvPrefixedBucket *prefixedBucket
		PubPrefixedBucket *prefixedBucket
	}

	bucketReadersOptions struct {
		files  *fileReaders
		bucket *bucketURL
	}

	bucketURL struct {
		downscopedToken string
		advURL          string
		pubURL          string
	}

	fileReaders struct {
		advReader io.ReadCloser
		pubReader io.ReadCloser
	}

	// BucketOption allows to configure the behavior of the Bucket.
	BucketReadersOption func(*bucketReadersOptions)
)

func WithFiles(advReader, pubReader io.ReadCloser) BucketReadersOption {
	return func(o *bucketReadersOptions) {
		o.files = &fileReaders{
			advReader: advReader,
			pubReader: pubReader,
		}
	}
}

func WithGCS(downscopedToken, advURL, pubURL string) BucketReadersOption {
	return func(o *bucketReadersOptions) {
		o.bucket = &bucketURL{
			downscopedToken: downscopedToken,
			advURL:          advURL,
			pubURL:          pubURL,
		}
	}
}

func NewBucketReaders(ctx context.Context, opts ...BucketReadersOption) (*BucketReaders, error) {
	b := &bucketReadersOptions{}
	for _, opt := range opts {
		opt(b)
	}

	if b.files != nil {
		return &BucketReaders{
			AdvReader: []io.ReadCloser{b.files.advReader},
			PubReader: []io.ReadCloser{b.files.pubReader},
		}, nil
	}

	if b.bucket == nil {
		return nil, ErrInvalidBucketOptions
	}

	if b.bucket.downscopedToken == "" {
		return nil, errors.New("downscopedToken is required")
	}

	client, err := storage.NewClient(
		ctx,
		option.WithTokenSource(
			oauth2.StaticTokenSource(
				&oauth2.Token{
					AccessToken: b.bucket.downscopedToken,
				},
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	advPrefixedBucket, err := bucketFromObjectURL(b.bucket.advURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URL: %w", err)
	}

	pubPrefixedBucket, err := bucketFromObjectURL(b.bucket.pubURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URL: %w", err)
	}

	return &BucketReaders{
		client:            client,
		AdvPrefixedBucket: advPrefixedBucket,
		PubPrefixedBucket: pubPrefixedBucket,
	}, nil
}

// newObjectReaders lists the objects specified by the advPrefixedBucket and pubPrefixedBucket and opens a reader for each object,
// except for the .Completed file.
func (b *BucketReaders) newObjectReaders(ctx context.Context) error {
	advReaders, err := readersFromPrefixedBucket(ctx, b.client, b.AdvPrefixedBucket)
	if err != nil {
		return err
	}

	pubReaders, err := readersFromPrefixedBucket(ctx, b.client, b.PubPrefixedBucket)
	if err != nil {
		return err
	}

	b.AdvReader = advReaders
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
func (b *BucketReaders) Close() error {
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
