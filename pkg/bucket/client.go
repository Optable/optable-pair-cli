package bucket

import (
	"context"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
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
