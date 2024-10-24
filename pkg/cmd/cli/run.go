package cli

import (
	"context"
	"fmt"

	v1 "github.com/optable/match-api/v2/gen/optable/external/v1"
)

type (
	RunCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		AdvertiserKeyPath  string `cmd:"" short:"k" help:"The path to the advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		Input              string `cmd:"" short:"i" help:"The input file containing the advertiser data to be hashed and encrypted. If a directory path is provided, all files within the directory will be processed."`
		NumThreads         int    `cmd:"" short:"n" default:"1" help:"The number of threads to use for the operation. Default to 1, and maximum is the number of cores."`
		Output             string `cmd:"" short:"o" help:"The output file to write the intersected PAIR IDs to. If not provided, the intersection will not happen."`
		PublisherPAIRIDs   string `cmd:"" short:"s" name:"publisher-pair-ids" help:"Save the publisher's PAIR IDs to a given directory. If not provided, the publisher's PAIR IDs will not be saved."`
	}
)

func (c *RunCmd) Help() string {
	return `
This command runs the whole lifecycle of the PAIR clean room operations.
It first hashes and encrypts the provided raw identifiers from the advertiser,
using the shared hash salt and advertiser's private key,
and writes the encrypted identifiers to a secure cloud storage location.
It then reads the publisher's encrypted identifiers,
and re-encrypts the publisher's encrypted identifiers using the same private key from the advertiser.
Optionally, it matches the two sets of triple-encrypted PAIR IDs to calculate the match rate,
and outputs the intersected PAIR IDs, decrypted using the advertiser private key.
Note that the matching operation happens locally on the client side, and should be reserved
for demo/testing purposes with reasonable data sizes only.

This command can recover from a failure at any step, and will resume from the last successful step.
`
}

func (c *RunCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	advertiserKey, err := ReadKeyConfig(c.AdvertiserKeyPath, cli.config.keyConfig)
	if err != nil {
		return fmt.Errorf("ReadKeyConfig: %w", err)
	}

	// instantiate the pair configuration
	pairCfg, err := NewPAIRConfig(ctx, c.PairCleanroomToken, c.NumThreads, advertiserKey)
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
	action := actionFromStates(publisherState, advertiserState)

	if action.contributeAdvertiserData {
		return startFromStepOne(ctx, pairCfg, c.Input, c.Output, c.PublisherPAIRIDs)
	}

	if action.reEncryptPublisherData {
		return startFromStepTwo(ctx, pairCfg, c.Output, c.PublisherPAIRIDs)
	}

	if action.matchData {
		return startFromStepThree(ctx, pairCfg, c.Output, c.PublisherPAIRIDs)
	}

	return fmt.Errorf("unexpected advertiser state: %s and publisher state: %s", advertiserState, publisherState)
}

type action struct {
	// contributeAdvertiserData indicates whether the advertiser should start by contribute data.
	// which is step 1 in the PAIR lifecycle.
	contributeAdvertiserData bool
	// reEncryptPublisherData indicates whether the advertiser should start by re-encrypt the publisher's data.
	// which is step 2 in the PAIR lifecycle.
	reEncryptPublisherData bool
	// matchData indicates whether the advertiser should start by matching the two sets of triple encrypted PAIR IDs.
	// which is step 3 in the PAIR lifecycle.
	matchData bool
}

func actionFromStates(publisherState, advertiserState v1.Cleanroom_Participant_State) action {
	switch publisherState {
	case v1.Cleanroom_Participant_DATA_CONTRIBUTED:
		if advertiserState == v1.Cleanroom_Participant_INVITED {
			return action{contributeAdvertiserData: true}
		}

		if advertiserState == v1.Cleanroom_Participant_DATA_CONTRIBUTED {
			return action{reEncryptPublisherData: true}
		}

		if advertiserState == v1.Cleanroom_Participant_DATA_TRANSFORMED {
			return action{matchData: true}
		}
	case v1.Cleanroom_Participant_DATA_TRANSFORMING:
		fallthrough
	case v1.Cleanroom_Participant_DATA_TRANSFORMED:
		if advertiserState == v1.Cleanroom_Participant_DATA_CONTRIBUTED {
			return action{reEncryptPublisherData: true}
		}

		if advertiserState == v1.Cleanroom_Participant_DATA_TRANSFORMED {
			return action{matchData: true}
		}
	case v1.Cleanroom_Participant_RUNNING:
		fallthrough
	case v1.Cleanroom_Participant_SUCCEEDED:
		if advertiserState == v1.Cleanroom_Participant_DATA_TRANSFORMED {
			return action{matchData: true}
		}
	default:
	}

	return action{}
}

func startFromStepOne(ctx context.Context, pairCfg *pairConfig, input, output string, publisherData string) error {
	// Step 1 Hash and encrypt the advertiser data and output to advTwicePath.
	if err := pairCfg.hashEncryt(ctx, input); err != nil {
		return fmt.Errorf("hashEncryt: %w", err)
	}

	// Step 2. Re-encrypt the publisher's hashed and encrypted PAIR IDs and output to pubTriplePath.
	if err := pairCfg.reEncrypt(ctx, publisherData); err != nil {
		return fmt.Errorf("reEncrypt: %w", err)
	}

	if output == "" {
		return nil
	}

	// Step 3. Match the two sets of triple encrypted PAIR IDs and output the intersected PAIR IDs to output.
	return pairCfg.match(ctx, output, publisherData)
}

func startFromStepTwo(ctx context.Context, pairCfg *pairConfig, output string, publisherData string) error {
	// Step 2. Re-encrypt the publisher's hashed and encrypted PAIR IDs and output to pubTriplePath.
	if err := pairCfg.reEncrypt(ctx, publisherData); err != nil {
		return fmt.Errorf("reEncrypt: %w", err)
	}

	if output == "" {
		return nil
	}

	// Step 3. Match the two sets of triple encrypted PAIR IDs and output the intersected PAIR IDs to output.
	return pairCfg.match(ctx, output, publisherData)
}

func startFromStepThree(ctx context.Context, pairCfg *pairConfig, output string, publisherData string) error {
	if output == "" {
		return nil
	}

	// Step 3. Match the two sets of triple encrypted PAIR IDs and output the intersected PAIR IDs to output.
	return pairCfg.match(ctx, output, publisherData)
}
