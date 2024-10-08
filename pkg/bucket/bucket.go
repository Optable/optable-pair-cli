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

type (
	Bucket struct {
		client     *storage.Client
		bucketName string
		prefix     string
	}

	ReadWriteCloser struct {
		name string
		r    io.ReadCloser
		w    io.WriteCloser
	}
)

func NewClient(ctx context.Context, downscopedToken string) (*storage.Client, error) {
	return storage.NewClient(
		ctx,
		option.WithTokenSource(
			oauth2.StaticTokenSource(
				&oauth2.Token{
					AccessToken: downscopedToken,
				},
			),
		),
	)
}

func NewBucket(client *storage.Client, objectURL string) (*Bucket, error) {
	return bucketFromObjectURL(client, objectURL)
}

// bucketFromObjectURL parses a valid objectURL and returns a Bucket.
func bucketFromObjectURL(client *storage.Client, objectURL string) (*Bucket, error) {
	url, err := url.Parse(objectURL)
	if err != nil {
		return nil, err
	}

	if url.Scheme != gcsblob.Scheme {
		return nil, fmt.Errorf("invalid object URL: %s", objectURL)
	}

	return &Bucket{
		client:     client,
		bucketName: url.Hostname(),
		prefix:     strings.TrimLeft(url.Path, "/"),
	}, nil
}

// NewObjectReadWriteCloser lists the obects specified by the objectURL and opens a reader for each object,
// except for the .COMPLETED file.
// It then opens a writer for each object under the same name specified by the destinationPrefix.
func (b *Bucket) NewObjectReadWriteCloser(ctx context.Context, destinationPrefix string) ([]*ReadWriteCloser, error) {
	query := &storage.Query{Prefix: b.prefix}

	bucket := b.client.Bucket(b.bucketName)

	it := bucket.Objects(ctx, query)
	var rwc []*ReadWriteCloser

	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return nil, err
		}

		if strings.HasSuffix(obj.Name, ".COMPLETED") || strings.HasSuffix(obj.Name, "/") || obj.Size == 0 {
			continue
		}

		reader, err := bucket.Object(obj.Name).NewReader(ctx)
		if err != nil {
			return nil, err
		}

		rwc = append(rwc, &ReadWriteCloser{
			name: blobFromObjectName(obj.Name),
			r:    reader,
			w:    bucket.Object(objectPathWithPrefix(obj.Name, destinationPrefix)).NewWriter(ctx),
		})
	}

	return rwc, nil
}

func objectPathWithPrefix(objectName string, prefix string) string {
	return fmt.Sprintf("%s/%s", prefix, blobFromObjectName(objectName))
}

func blobFromObjectName(objectName string) string {
	split := strings.Split(objectName, "/")
	return split[len(split)-1]
}
