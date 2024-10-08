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
)

func NewBucket(ctx context.Context, downscopedToken string, srcURL, dstURL string) (*Bucket, error) {
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

	srcPrefixedBucket, err := bucketFromObjectURL(srcURL)
	if err != nil {
		return nil, err
	}

	dstPrefixedBucket, err := bucketFromObjectURL(dstURL)
	if err != nil {
		return nil, err
	}

	b := &Bucket{
		client:            client,
		srcPrefixedBucket: srcPrefixedBucket,
		dstPrefixedBucket: dstPrefixedBucket,
	}

	rws, err := b.newObjectReadWriteCloser(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create read writers: %w", err)
	}

	b.ReadWriters = rws

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

// newObjectReadWriteCloser lists the obects specified by the objectURL and opens a reader for each object,
// except for the .COMPLETED file.
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

func (b *Bucket) Complete(ctx context.Context) error {
	dstBucket := b.client.Bucket(b.dstPrefixedBucket.bucket)
	completedWriter := dstBucket.Object(fmt.Sprintf("%s/%s", b.dstPrefixedBucket.prefix, CompletedFile)).NewWriter(ctx)
	if _, err := completedWriter.Write([]byte{}); err != nil {
		return err
	}

	return completedWriter.Close()
}

func (b *Bucket) Close() error {
	for _, rw := range b.ReadWriters {
		if err := rw.Reader.Close(); err != nil {
			return err
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
