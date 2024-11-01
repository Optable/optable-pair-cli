package cli

import (
	"context"
	"fmt"

	v1 "github.com/optable/match-api/v2/gen/optable/external/v1"
)

type (
	RunCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation. You can find this by logging into the Optable PAIR Connector UI to which you were invited."`
		AdvertiserKeyPath  string `cmd:"" short:"k" name:"keypath" help:"The path to the advertiser clean room's private key to use. If not provided, the key saved in the configuration file will be used."`
		Input              string `cmd:"" short:"i" help:"The path to the input file containing the newline separated list of canonicalized email addresses for encrypted PAIR matching. The expected canonical form of an email address is obtained by trimming leading and trailing spaces, downcasing, and applying the SHA256 hash function without a salt. If a directory path is provided, all files within the directory will be processed."`
		NumThreads         int    `cmd:"" short:"n" help:"The number of threads to use for the operation. Defaults to the number of the available cores on the machine."`
		Output             string `cmd:"" short:"o" help:"The path to the output file to write the intersected publisher PAIR IDs to. If not provided, the intersection will not happen."`
		PublisherPAIRIDs   string `cmd:"" name:"save-publisher-encrypted-data-locally" short:"s" help:" During the encryption stages of the PAIR protocol for 2 clean rooms, the advertiser clean room must encrypt the publisher clean room dataset with the advertiser clean room's private key. The publisher triple encrypted dataset is sent to the Optable publisher clean room where it is temporarily stored in GCS so that the intersection can be computed in the final stage. Setting this flag causes the opair utility to save a local copy of the triple encrypted publisher dataset and to use the locally saved copy when calculating the intersection. If not provided, opair will download both triple encrypted datasets from the GCS location managed by the Optable publisher clean room and assume that they have not been tampered with. Note that if you specify the -s flag without specifying -o then when you later re-run with -o you must also include the -s flag from the first run."`
	}
)

func (c *RunCmd) Help() string {
	return `
The` + " `run` " + `command runs a secure and privacy protected match on encrypted data
using the 2 clean room PAIR protocol. The opair utility assumes the role of the
advertiser clean room. The command must be invoked with an argument specifying
the unique token associated with the PAIR clean room that you were invited to.
You can find the <pair-cleanroom-token> by logging into the Optable PAIR
Connector UI to which you were invited.

The` + " `run` " + `command expects to find the input file specified with the --input
flag containing a newline delimited list of canonicalized email addresses for
matching. The expected canonical form of an email address is obtained by
trimming leading and trailing spaces, downcasing, and applying the SHA256 hash
function without a salt. When invoked, the` + " `run` " + `command will perform all of
the required PAIR protocol encryption and encrypted data exchange steps.

The advertiser clean room's final step of computing the intersection of
publisher PAIR IDs and calculating the resulting match rate is only performed
when the --output flag is provided. When not providing the --output flag, the
PAIR protocol is executed without computing the advertiser clean room
intersection. The final step can be performed later by re-invoking` + " `run` " + `with
the same <pair-cleanroom-token> and specifying the --output flag.

The` + " `run` " + `command on a specified <pair-cleanroom-token> can recover from a
failure at any step, and will resume from the
last successful step.
`
}

func (c *RunCmd) Run(cli *CliContext) error {
	ctx := cli.Context()

	advertiserKey, err := ReadKeyConfig(cli.keyContext, cli.config)
	if err != nil {
		return fmt.Errorf("ReadKeyConfig: %w", err)
	}

	if c.NumThreads <= 0 {
		c.NumThreads = defaultThreadCount
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
