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

	Version VersionCmd `cmd:"" help:"Print version"`

	GenerateKey GenerateKeyCmd `cmd:"" help:"Generate a new advertiser private key."`

	Get         GetCmd         `cmd:"" help:"Get the PAIR clean room."`
	Participate ParticipateCmd `cmd:"" hidden:"" help:"Participate in the PAIR operation by contributing advertiser hashed and encrypted data."`
	ReEncrypt   ReEncryptCmd   `cmd:"" hidden:"" help:"Re-encrypt publisher's PAIR IDs with the advertiser key."`
	Match       MatchCmd       `cmd:"" help:"Match publisher's PAIR IDs with advertiser's PAIR IDs."`
	Run         RunCmd         `cmd:"" help:"Run the PAIR clean room operation."`
}

func (c *MatchCmd) Help() string {
	return `
This operation produces the match rate of this PAIR clean room operation,
and output the list of decrypted and matched PAIR IDs.

Please be aware that this operation is for demo purposes only for
resonable data size.
`
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
