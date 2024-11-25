package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
	"gocloud.dev/blob/gcsblob"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const CompletedFile = ".Completed"

type (
	// BucketReadWriter contains the storage client and read writers for the source and destination buckets.
	// Optionally, it can use a file reader instead of the source bucket.
	BucketReadWriter struct {
		client            *storage.Client
		FileReader        io.Reader
		srcPrefixedBucket *prefixedBucket
		dstPrefixedBucket *prefixedBucket
		ReadWriters       []*ReadWriteCloser
	}

	BucketCompleter struct {
		client            *storage.Client
		dstPrefixedBucket *prefixedBucket
	}

	prefixedBucket struct {
		bucket string
		prefix string
	}

	// ReadWriteCloser contains the name of the object, its reader and a writer.
	ReadWriteCloser struct {
		name   string
		Reader io.ReadCloser
		Writer io.WriteCloser
	}

	bucketOptions struct {
		reader    io.Reader
		sourceURL string
	}

	// BucketOption allows to configure the behavior of the Bucket.
	BucketOption func(*bucketOptions)
)

// WithReader allows to specify a reader to be used for the bucket.
func WithReader(reader io.Reader) BucketOption {
	return func(o *bucketOptions) {
		o.reader = reader
	}
}

// WithSourceURL allows to specify a source URL to be used for the bucket.
func WithSourceURL(srcURL string) BucketOption {
	return func(o *bucketOptions) {
		o.sourceURL = srcURL
	}
}

// NewBucketCompleter creates a new BucketCompleter object which is used to signal that the transfer is complete.
// Caller needs to call Close() on the returned BucketCompleter object to ensure
func NewBucketCompleter(ctx context.Context, downscopedToken string, dstURL string) (*BucketCompleter, error) {
	if downscopedToken == "" {
		return nil, ErrTokenRequired
	}

	client, err := storage.NewClient(
		ctx,
		option.WithTokenSource(
			oauth2.StaticTokenSource(
				&oauth2.Token{
					AccessToken: downscopedToken,
				},
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	dstPrefixedBucket, err := bucketFromObjectURL(dstURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URL: %w", err)
	}

	return &BucketCompleter{
		client:            client,
		dstPrefixedBucket: dstPrefixedBucket,
	}, nil
}

// Complete writes a .Completed file to the destination bucket to signal that the transfer is complete.
// It then closes the client.
func (b *BucketCompleter) Complete(ctx context.Context) error {
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)
	completedWriter := dstBucket.Object(fmt.Sprintf("%s/%s", b.dstPrefixedBucket.prefix, CompletedFile)).NewWriter(ctx)
	if _, err := completedWriter.Write([]byte{}); err != nil {
		return fmt.Errorf("failed to write completed file: %w", err)
	}

	_, err := completedWriter.Write([]byte("v1"))
	if err != nil {
		return fmt.Errorf("failed to write completed file: %w", err)
	}

	if err := completedWriter.Close(); err != nil {
		return fmt.Errorf("failed to close completed file: %w", err)
	}

	return b.client.Close()
}

// Checks if the .Completed file exists in the destination bucket.
func (b *BucketCompleter) HasCompleted(ctx context.Context) (bool, error) {
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)
	_, err := dstBucket.Object(fmt.Sprintf("%s/%s", b.dstPrefixedBucket.prefix, CompletedFile)).Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return false, nil
	}
	return err == nil, err
}

// NewBucketReadWriter creates a new Bucket object and opens readers and writers for the specified source and destination URLs.
// Caller needs to call Close() on the returned Bucket object to release resources.
func NewBucketReadWriter(ctx context.Context, downscopedToken string, dstURL string, opts ...BucketOption) (*BucketReadWriter, error) {
	if downscopedToken == "" {
		return nil, ErrTokenRequired
	}

	bucketOption := &bucketOptions{}
	for _, opt := range opts {
		opt(bucketOption)
	}

	client, err := storage.NewClient(
		ctx,
		option.WithTokenSource(
			oauth2.StaticTokenSource(
				&oauth2.Token{
					AccessToken: downscopedToken,
				},
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	dstPrefixedBucket, err := bucketFromObjectURL(dstURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URL: %w", err)
	}

	b := &BucketReadWriter{
		client:            client,
		dstPrefixedBucket: dstPrefixedBucket,
	}

	if src := bucketOption.sourceURL; src != "" {
		srcPrefixedBucket, err := bucketFromObjectURL(src)
		if err != nil {
			return nil, fmt.Errorf("failed to parse source URL: %w", err)
		}

		b.srcPrefixedBucket = srcPrefixedBucket

		if err := b.newObjectReadWriteCloser(ctx); err != nil {
			return nil, fmt.Errorf("failed to create read writers: %w", err)
		}

		return b, nil
	}

	if reader := bucketOption.reader; reader != nil {
		b.FileReader = reader

		rw, err := b.newObjectWriteCloser(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create read writers: %w", err)
		}

		b.ReadWriters = append(b.ReadWriters, rw)
	} else {
		return nil, ErrInvalidBucketOptions
	}

	return b, nil
}

// bucketFromObjectURL parses a valid objectURL and returns a Bucket.
func bucketFromObjectURL(objectURL string) (*prefixedBucket, error) {
	url, err := url.Parse(objectURL)
	if err != nil {
		return nil, err
	}

	if url.Scheme != gcsblob.Scheme {
		return nil, fmt.Errorf("invalid object URL: %s", objectURL)
	}

	return &prefixedBucket{
		bucket: url.Host,
		prefix: strings.TrimLeft(url.Path, "/"),
	}, nil
}

// newObjectReadWriteCloser lists the objects specified by the srcPrefixedBucket and opens a reader for each object,
// except for the .Completed file.
// It then opens a writer for each object under the same name specified by the destinationPrefix.
func (b *BucketReadWriter) newObjectReadWriteCloser(ctx context.Context) error {
	logger := zerolog.Ctx(ctx)
	query := &storage.Query{Prefix: b.srcPrefixedBucket.prefix + "/"}

	srcBucket := b.client.Bucket(b.srcPrefixedBucket.bucket)
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)

	it := srcBucket.Objects(ctx, query)
	var rwc []*ReadWriteCloser

	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			logger.Debug().Err(err).Msgf("failed to list objects from source bucket %s", b.srcPrefixedBucket.prefix)
			return err
		}

		if strings.HasSuffix(obj.Name, CompletedFile) || strings.HasSuffix(obj.Name, "/") || obj.Size == 0 {
			continue
		}

		reader, err := srcBucket.Object(obj.Name).NewReader(ctx)
		if err != nil {
			return err
		}

		rwc = append(rwc, &ReadWriteCloser{
			name:   blobFromObjectName(obj.Name),
			Reader: reader,
			Writer: dstBucket.Object(objectPathWithPrefix(obj.Name, b.dstPrefixedBucket.prefix)).NewWriter(ctx),
		})
	}

	b.ReadWriters = rwc

	return nil
}

// newObjectWriteCloser creates a new writer for the destination bucket.
func (b *BucketReadWriter) newObjectWriteCloser(ctx context.Context) (*ReadWriteCloser, error) {
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)
	writer := dstBucket.Object(fmt.Sprintf("%s/data_%s.csv", b.dstPrefixedBucket.prefix, shortHex())).NewWriter(ctx)
	return &ReadWriteCloser{
		name:   CompletedFile,
		Writer: writer,
	}, nil
}

// Close closes the client and all read writers.
func (b *BucketReadWriter) Close() error {
	for _, rw := range b.ReadWriters {
		if rw.Reader != nil {
			if err := rw.Reader.Close(); err != nil {
				return err
			}
		}

		if err := rw.Writer.Close(); err != nil {
			return err
		}
	}

	return b.client.Close()
}

func objectPathWithPrefix(objectName string, prefix string) string {
	return fmt.Sprintf("%s/%s", prefix, blobFromObjectName(objectName))
}

func blobFromObjectName(objectName string) string {
	split := strings.Split(objectName, "/")
	obj := split[len(split)-1]
	split = strings.Split(obj, ".")
	return fmt.Sprintf("%s-%s.%s", split[0], shortHex(), split[1])
}

func shortHex() string {
	return fmt.Sprintf("%08x", rand.Int32())
}
