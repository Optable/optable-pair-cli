package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	v1 "github.com/optable/match-api/v2/gen/optable/external/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	CleanroomClient struct {
		client        *http.Client
		url           string
		token         string
		cleanroomName string
	}
)

func NewCleanroomClient(token *CleanroomToken) (*CleanroomClient, error) {
	hostURL := strings.TrimRight(token.IssuerHost, "/")
	host, err := url.Parse(hostURL)
	if err != nil {
		return nil, err
	}

	if host.Scheme == "" {
		hostURL = "https://" + hostURL
	}

	return &CleanroomClient{
		client:        http.DefaultClient,
		token:         token.Raw,
		cleanroomName: token.Cleanroom,
		url:           hostURL,
	}, nil
}

func (c *CleanroomClient) GetCleanroom(ctx context.Context, sensitive bool) (*v1.Cleanroom, error) {
	req := &v1.GetCleanroomRequest{
		Name: c.cleanroomName,
		View: v1.GetCleanroomRequest_FULL,
	}

	if sensitive {
		req.View = v1.GetCleanroomRequest_SENSITIVE
	}

	return c.do(ctx, req)
}

func (c *CleanroomClient) RefreshToken(ctx context.Context) (*v1.Cleanroom, error) {
	req := &v1.RefreshTokenRequest{
		Name: c.cleanroomName,
	}

	return c.do(ctx, req)
}

func (c *CleanroomClient) GetDownScopedToken(ctx context.Context) (string, error) {
	cleanroom, err := c.GetCleanroom(ctx, true)
	if err != nil {
		return "", err
	}

	if tk := cleanroom.GetConfig().GetPairConfig().GetToken(); tk != nil {
		if tk.GetExpireTime().AsTime().Before(time.Now()) {
			// refresh
			cleanroom, err = c.RefreshToken(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to refresh token: %w", err)
			}

			return cleanroom.GetConfig().GetPairConfig().GetToken().GetToken(), nil
		}

		return tk.GetToken(), nil
	}

	return "", fmt.Errorf("token not found")
}

func (c *CleanroomClient) GetConfig(ctx context.Context) (*v1.Cleanroom_Config_PairConfig, error) {
	cleanroom, err := c.GetCleanroom(ctx, false)
	if err != nil {
		return nil, err
	}

	return cleanroom.GetConfig().GetPairConfig(), nil
}

func (c *CleanroomClient) do(ctx context.Context, req proto.Message) (*v1.Cleanroom, error) {
	msg, err := protojson.Marshal(req)
	if err != nil {
		return nil, err
	}

	var path string
	switch req.(type) {
	case *v1.GetCleanroomRequest:
		path = "/admin/api/external/v1/cleanroom/get"
	case *v1.RefreshTokenRequest:
		path = "/admin/api/external/v1/cleanroom/refresh-token"
	default:
		return nil, fmt.Errorf("unknown request type")
	}

	reqPath := fmt.Sprintf("%s%s", c.url, path)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", reqPath, bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Add("Authorization", "Bearer "+c.token)
	httpReq.Header.Add("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", httpResp.StatusCode)
	}

	res := &v1.Cleanroom{}
	if err := protojson.Unmarshal(body, res); err != nil {
		return nil, err
	}
	return res, nil
}
