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
	"google.golang.org/protobuf/proto"
)

const waitTime = 1 * time.Hour

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

	if tk := cleanroom.GetConfig().GetPair().GetGcsToken(); tk != nil {
		if tk.GetExpireTime().AsTime().Before(time.Now()) {
			// refresh
			cleanroom, err = c.RefreshToken(ctx)
			if err != nil {
				return "", fmt.Errorf("failed to refresh token: %w", err)
			}

			return cleanroom.GetConfig().GetPair().GetGcsToken().GetValue(), nil
		}

		return tk.GetValue(), nil
	}

	return "", fmt.Errorf("token not found")
}

func (c *CleanroomClient) GetConfig(ctx context.Context) (*v1.Cleanroom_Config_PairConfig, error) {
	cleanroom, err := c.GetCleanroom(ctx, false)
	if err != nil {
		return nil, err
	}

	return cleanroom.GetConfig().GetPair(), nil
}

func (c *CleanroomClient) ReadyForMatch(ctx context.Context) error {
	return c.WaitForState(
		ctx,
		[]v1.Cleanroom_Participant_State{
			v1.Cleanroom_Participant_DATA_TRANSFORMED,
			v1.Cleanroom_Participant_RUNNING,
			v1.Cleanroom_Participant_SUCCEEDED,
		},
	)
}

func (c *CleanroomClient) WaitForState(ctx context.Context, states []v1.Cleanroom_Participant_State) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	timer := time.NewTimer(1 * time.Hour)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timeout after %v", waitTime)
		case <-ticker.C:
			// check state
		}

		cleanroom, err := c.GetCleanroom(ctx, false)
		if err != nil {
			return err
		}

		participants := cleanroom.GetParticipants()
		var publisher *v1.Cleanroom_Participant
		for _, p := range participants {
			if p.GetRole() == v1.Cleanroom_Participant_PUBLISHER {
				publisher = p
				break
			}
		}

		for _, state := range states {
			if publisher.GetState() == state {
				return nil
			}
		}
	}
}

func (c *CleanroomClient) do(ctx context.Context, req proto.Message) (*v1.Cleanroom, error) {
	msg, err := proto.Marshal(req)
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
	httpReq.Header.Add("Content-Type", "application/protobuf")

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
	if err := proto.Unmarshal(body, res); err != nil {
		return nil, err
	}
	return res, nil
}
