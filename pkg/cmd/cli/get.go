package cli

import (
	"errors"
	"fmt"
	"optable-pair-cli/pkg/internal"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
)

type (
	GetCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation. You can find this by logging into the Optable PAIR Connector UI to which you were invited."`
	}
)

func (c *GetCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	if c.PairCleanroomToken == "" {
		return errors.New("pair clean room token is required")
	}

	cleanroomToken, err := internal.ParseCleanroomToken(c.PairCleanroomToken)
	if err != nil {
		return fmt.Errorf("failed to parse clean room token: %w", err)
	}

	client, err := internal.NewCleanroomClient(cleanroomToken)
	if err != nil {
		return fmt.Errorf("failed to create clean room client: %w", err)
	}

	cleanroom, err := client.GetCleanroom(ctx, true)
	if err != nil {
		return err
	}

	config := cleanroom.GetConfig().GetPair()
	shouldTokenRefresh := config.GcsToken == nil || config.GcsToken.ExpireTime.AsTime().Before(time.Now())
	if shouldTokenRefresh {
		cleanroom, err = client.RefreshToken(ctx)
		if err != nil {
			return err
		}
	}

	marshaler := protojson.MarshalOptions{
		Multiline:       true,
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}

	fmt.Println(marshaler.Format(cleanroom))

	return nil
}
