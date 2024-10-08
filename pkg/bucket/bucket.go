package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob/gcsblob"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const CompletedFile = ".Completed"

type (
	Bucket struct {
		client            *storage.Client
		FileReader        io.Reader
		srcPrefixedBucket *prefixedBucket
		dstPrefixedBucket *prefixedBucket
		ReadWriters       []*ReadWriteCloser
	}

	prefixedBucket struct {
		bucket string
		prefix string
	}

	ReadWriteCloser struct {
		name   string
		Reader io.ReadCloser
		Writer io.WriteCloser
	}

	bucketOptions struct {
		reader    io.Reader
		sourceURL string
	}

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

// NewBucket creates a new Bucket object and opens readers and writers for the specified source and destination URLs.
// Caller needs to call Close() on the returned Bucket object to release resources.
func NewBucket(ctx context.Context, downscopedToken string, dstURL string, opts ...BucketOption) (*Bucket, error) {
	if downscopedToken == "" {
		return nil, errors.New("downscopedToken is required")
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

	b := &Bucket{
		client:            client,
		dstPrefixedBucket: dstPrefixedBucket,
	}

	if src := bucketOption.sourceURL; src != "" {
		srcPrefixedBucket, err := bucketFromObjectURL(src)
		if err != nil {
			return nil, fmt.Errorf("failed to parse source URL: %w", err)
		}

		b.srcPrefixedBucket = srcPrefixedBucket

		rws, err := b.newObjectReadWriteCloser(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create read writers: %w", err)
		}

		b.ReadWriters = rws
	}

	if reader := bucketOption.reader; reader != nil {
		b.FileReader = reader

		rw, err := b.newObjectWriteCloser(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create read writers: %w", err)
		}

		b.ReadWriters = append(b.ReadWriters, rw)
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
func (b *Bucket) newObjectReadWriteCloser(ctx context.Context) ([]*ReadWriteCloser, error) {
	query := &storage.Query{Prefix: b.srcPrefixedBucket.prefix}

	srcBucket := b.client.Bucket(b.srcPrefixedBucket.bucket)
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)

	it := srcBucket.Objects(ctx, query)
	var rwc []*ReadWriteCloser

	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return nil, err
		}

		if strings.HasSuffix(obj.Name, CompletedFile) || strings.HasSuffix(obj.Name, "/") || obj.Size == 0 {
			continue
		}

		reader, err := srcBucket.Object(obj.Name).NewReader(ctx)
		if err != nil {
			return nil, err
		}

		rwc = append(rwc, &ReadWriteCloser{
			name:   blobFromObjectName(obj.Name),
			Reader: reader,
			Writer: dstBucket.Object(objectPathWithPrefix(obj.Name, b.dstPrefixedBucket.prefix)).NewWriter(ctx),
		})
	}

	return rwc, nil
}

// newObjectWriteCloser creates a new writer for the destination bucket.
func (b *Bucket) newObjectWriteCloser(ctx context.Context) (*ReadWriteCloser, error) {
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)
	writer := dstBucket.Object(fmt.Sprintf("%s/%s", b.dstPrefixedBucket.prefix, "data.csv")).NewWriter(ctx)
	return &ReadWriteCloser{
		name:   CompletedFile,
		Writer: writer,
	}, nil
}

// Complete writes a .Completed file to the destination bucket to signal that the transfer is complete.
func (b *Bucket) Complete(ctx context.Context) error {
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)
	completedWriter := dstBucket.Object(fmt.Sprintf("%s/%s", b.dstPrefixedBucket.prefix, CompletedFile)).NewWriter(ctx)
	if _, err := completedWriter.Write([]byte{}); err != nil {
		return fmt.Errorf("failed to write completed file: %w", err)
	}

	if err := completedWriter.Close(); err != nil {
		return fmt.Errorf("failed to close completed file: %w", err)
	}

	return nil
}

// Close closes the client and all read writers.
func (b *Bucket) Close() error {
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
	return split[len(split)-1]
}
