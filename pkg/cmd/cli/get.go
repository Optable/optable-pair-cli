package cli

import (
	"errors"
	"fmt"
	"optable-pair-cli/pkg/internal"

	"google.golang.org/protobuf/encoding/protojson"
)

type (
	GetCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
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

	cleanroom, err := client.GetCleanroom(ctx, false)
	if err != nil {
		return err
	}

	marshaler := protojson.MarshalOptions{
		Multiline:       true,
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}

	fmt.Println(marshaler.Format(cleanroom))

	return nil
}
