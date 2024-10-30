package cli

import (
	"context"

	"github.com/rs/zerolog"
)

type CliContext struct {
	ctx    context.Context
	config *Config
}

type (
	CleanroomCmd struct {
		Get         GetCmd         `cmd:"" help:"Get the current status and configuration associated with the specified Optable PAIR clean room."`
		Participate ParticipateCmd `cmd:"" hidden:"" help:"Participate in the PAIR operation by contributing advertiser hashed and encrypted data."`
		ReEncrypt   ReEncryptCmd   `cmd:"" hidden:"" help:"Re-encrypt publisher's PAIR IDs with the advertiser key."`
		Match       MatchCmd       `cmd:"" hidden:"" help:"Match publisher's PAIR IDs with advertiser's PAIR IDs."`
		Run         RunCmd         `cmd:"" help:"As the advertiser clean room, run the PAIR match protocol with the publisher that has invited you to the specified Optable PAIR clean room."`
		Decrypt     DecryptCmd     `cmd:"" help:"Decrypt a list of previously matched triple encrypted PAIR IDs using the advertiser clean room's private key."`
	}

	KeyCmd struct {
		Create CreateCmd `cmd:"" help:"Generate a new advertiser clean room private key and store it locally."`
		Path   PathCmd   `cmd:"" help:"Print the path to the current advertiser clean room private key."`
	}
	Cli struct {
		Verbose int `short:"v" type:"counter" help:"Enable debug mode."`

		Version VersionCmd `cmd:"" help:"Print utility version"`

		CleanroomCmd CleanroomCmd `cmd:"" name:"cleanroom" help:"Commands for interacting with Optable PAIR clean rooms."`
		KeyCmd       KeyCmd       `cmd:"" name:"key" help:"Commands for managing advertiser clean room private keys."`
	}
)

func (c *Cli) NewContext(conf *Config) (*CliContext, error) {
	cliCtx := &CliContext{
		ctx:    NewLogger("opair", c.Verbose).WithContext(context.Background()),
		config: conf,
	}

	return cliCtx, nil
}

// Context returns a context.Context that is protected by a timeout accessible
// via the `--timeout` flag. Each invocation returns a *new* Context and thus
// resets the timeout.
func (c *CliContext) Context() context.Context {
	return c.ctx
}

func (c *CliContext) Log() *zerolog.Logger {
	return zerolog.Ctx(c.ctx)
}

type HelpCmd struct{}

func (c *HelpCmd) Run(cli *CliContext) error {
	return nil
}
