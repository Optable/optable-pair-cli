package cli

import (
	"context"

	"github.com/rs/zerolog"
)

type CliContext struct {
	ctx    context.Context
	config *Config
}

type Cli struct {
	Verbose int `short:"v" type:"counter" help:"Enable debug mode."`

	Token   string     `placeholder:"<token>" help:"Optable Auth Token"`
	Version VersionCmd `cmd:"" help:"Print version"`

	GenerateKey GenerateKeyCmd `cmd:"" help:"Generate a new advertiser private key."`

	Participate ParticipateCmd `cmd:"" hidden:"" help:"Participate in the PAIR operation by contributing advertiser hashed and encrypted data."`
	ReEncrypt   ReEncryptCmd   `cmd:"" hidden:"" help:"Re-encrypt publisher's PAIR IDs with the advertiser key."`
}

func (c *Cli) NewContext(conf *Config) (*CliContext, error) {
	cliCtx := &CliContext{
		ctx:    NewLogger("pair", c.Verbose).WithContext(context.Background()),
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
