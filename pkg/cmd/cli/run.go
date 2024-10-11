package cli

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/optable/match-api/v2/gen/optable/external/v1"
)

type (
	RunCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		AdvertiserKey      string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		Input              string `cmd:"" short:"i" help:"The input file containing the advertiser data to be hashed and encrypted. If given a directory, all files in the directory will be processed."`
		Output             string `cmd:"" short:"o" help:"The output file to write the advertiser data to, default to stdout."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is the number of cores."`
	}
)

func (c *RunCmd) Help() string {
	return `
This command runs the whole lifecyle of the PAIR clean room operations.
It first hashes and encrypts the provided advertiser raw identifiers
using the shared hash salt and advertiser private key,
and writes the advertiser encrypted identifiers to a secure cloud storage location.
It then reads the publisher's encrypted identifiers,
and re-encrypts the publisher encrypted identifiers using the same advertiser private key.
Finally, it matches the two sets of triple encrypted PAIR IDs to then produce the match rate,
and output the intersected PAIR IDs decrypted with the advertiser private key.

This command can recover from a failure at any step, and will resume from the last successful step.
`
}

func (c *RunCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	if c.AdvertiserKey == "" {
		c.AdvertiserKey = cli.config.keyConfig.Key
		if c.AdvertiserKey == "" {
			return errors.New("advertiser key is required, please either provide one or generate one.")
		}
	}

	// instantiate the pair configuration
	pairCfg, err := NewPAIRConfig(ctx, c.PairCleanroomToken, c.NumThreads, c.AdvertiserKey)
	if err != nil {
		return err
	}

	cleanroom, err := pairCfg.cleanroomClient.GetCleanroom(ctx, false)
	if err != nil {
		return fmt.Errorf("GetCleanroom: %w", err)
	}

	// Get the state of the publisher and advertiser
	var (
		publisherState  v1.Cleanroom_Participant_State
		advertiserState v1.Cleanroom_Participant_State
	)

	for _, p := range cleanroom.GetParticipants() {
		switch p.GetRole() {
		case v1.Cleanroom_Participant_PUBLISHER:
			publisherState = p.GetState()
		case v1.Cleanroom_Participant_ADVERTISER:
			advertiserState = p.GetState()
		}
	}

	// PAIR runs in the following 3 steps:
	// 1. Hash and encrypt the advertiser data.
	//    advertiser state go from INVITED to DATA_CONTRIBUTED.
	// 2. Re-encrypt the publisher's hashed and encrypted PAIR IDs.
	//    advertiser state go from DATA_CONTRIBUTED to DATA_TRANSFORMED.
	// 3. Match the two sets of triple encrypted PAIR IDs and output the intersected PAIR IDs to output.
	//
	// Depending on the state of the publisher and advertiser, we can start from any of the steps.
	switch publisherState {
	case v1.Cleanroom_Participant_DATA_CONTRIBUTED:
		if advertiserState == v1.Cleanroom_Participant_INVITED {
			return startFromStepOne(ctx, pairCfg, c.Input, c.Output)
		}

		if advertiserState == v1.Cleanroom_Participant_DATA_CONTRIBUTED {
			return startFromStepTwo(ctx, pairCfg, c.Output)
		}

		if advertiserState == v1.Cleanroom_Participant_DATA_TRANSFORMED {
			return startFromStepThree(ctx, pairCfg, c.Output)
		}
	case v1.Cleanroom_Participant_DATA_TRANSFORMING:
		fallthrough
	case v1.Cleanroom_Participant_DATA_TRANSFORMED:
		if advertiserState == v1.Cleanroom_Participant_DATA_CONTRIBUTED {
			return startFromStepTwo(ctx, pairCfg, c.Output)
		}

		if advertiserState == v1.Cleanroom_Participant_DATA_TRANSFORMED {
			return startFromStepThree(ctx, pairCfg, c.Output)
		}
	case v1.Cleanroom_Participant_RUNNING:
		fallthrough
	case v1.Cleanroom_Participant_SUCCEEDED:
		if advertiserState == v1.Cleanroom_Participant_DATA_TRANSFORMED {
			return startFromStepThree(ctx, pairCfg, c.Output)
		}
	default:
		return fmt.Errorf("unexpected publisher state: %s", publisherState)
	}

	return fmt.Errorf("unexpected advertiser state: %s", advertiserState)
}

func startFromStepOne(ctx context.Context, pairCfg *pairConfig, input, output string) error {
	// Step 1 Hash and encrypt the advertiser data and output to advTwicePath.
	if err := pairCfg.hashEncryt(ctx, input); err != nil {
		return fmt.Errorf("hashEncryt: %w", err)
	}

	// Step 2. Re-encrypt the publisher's hashed and encrypted PAIR IDs and output to pubTriplePath.
	if err := pairCfg.reEncrypt(ctx); err != nil {
		return fmt.Errorf("reEncrypt: %w", err)
	}

	// Step 3. Match the two sets of triple encrypted PAIR IDs and output the intersected PAIR IDs to output.
	if err := pairCfg.match(ctx, output); err != nil {
		return fmt.Errorf("match: %w", err)
	}

	return nil
}

func startFromStepTwo(ctx context.Context, pairCfg *pairConfig, output string) error {
	// Step 2. Re-encrypt the publisher's hashed and encrypted PAIR IDs and output to pubTriplePath.
	if err := pairCfg.reEncrypt(ctx); err != nil {
		return fmt.Errorf("reEncrypt: %w", err)
	}

	// Step 3. Match the two sets of triple encrypted PAIR IDs and output the intersected PAIR IDs to output.
	if err := pairCfg.match(ctx, output); err != nil {
		return fmt.Errorf("match: %w", err)
	}

	return nil
}

func startFromStepThree(ctx context.Context, pairCfg *pairConfig, output string) error {
	// Step 3. Match the two sets of triple encrypted PAIR IDs and output the intersected PAIR IDs to output.
	if err := pairCfg.match(ctx, output); err != nil {
		return fmt.Errorf("match: %w", err)
	}

	return nil
}
