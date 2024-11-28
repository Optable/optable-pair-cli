package cli

import (
	"context"

	"github.com/rs/zerolog"
)

type CliContext struct {
	ctx        context.Context
	config     *Config
	keyContext string
}

type (
	CleanroomCmd struct {
		Get     GetCmd     `cmd:"" help:"Get the current status and configuration associated with the specified Optable PAIR clean room."`
		Run     RunCmd     `cmd:"" help:"As the advertiser clean room, run the PAIR match protocol with the publisher that has invited you to the specified Optable PAIR clean room."`
		Decrypt DecryptCmd `cmd:"" help:"Decrypt a list of previously matched triple encrypted PAIR IDs using the advertiser clean room's private key."`
	}

	KeyCmd struct {
		Create CreateCmd `cmd:"" help:"Generate a new advertiser clean room private key and store it locally."`
	}
	Cli struct {
		Verbose int `short:"v" type:"counter" help:"Enable debug mode."`

		Version VersionCmd `cmd:"" help:"Print utility version"`

		CleanroomCmd      CleanroomCmd `cmd:"" name:"cleanroom" help:"Commands for interacting with Optable PAIR clean rooms."`
		AdvertiserKeyPath string       `cmd:"" short:"k" name:"keypath" help:"The path to the advertiser clean room's private key to use for the operation. If not provided, the key saved in the configuration file will be used."`
		KeyCmd            KeyCmd       `cmd:"" name:"key" help:"Commands for managing advertiser clean room private keys."`
		Context           string       `short:"c" help:"Context name to use" default:"default"`
	}
)

func (c *Cli) NewContext(conf *Config) (*CliContext, error) {
	cliCtx := &CliContext{
		ctx:        NewLogger("opair", c.Verbose).WithContext(context.Background()),
		config:     conf,
		keyContext: c.Context,
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
