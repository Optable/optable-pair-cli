package cli

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

type CliContext struct {
	ctx     context.Context
	Timeout time.Duration
	config  *Config
}

type Cli struct {
	Verbose int           `short:"v" type:"counter" help:"Enable debug mode."`
	Timeout time.Duration `default:"5s" help:"Timeout for some operation formatted like go time.Duration"`

	Version VersionCmd `cmd:"" help:"Print version"`
	Token   string     `placeholder:"<token>" help:"Optable Auth Token"`
}

func (c *Cli) NewContext(conf *Config) (*CliContext, error) {
	cliCtx := &CliContext{
		ctx:     NewLogger("pair", c.Verbose).WithContext(context.Background()),
		Timeout: c.Timeout,
		config:  conf,
	}

	return cliCtx, nil
}

// Context returns a context.Context that is protected by a timeout accessible
// via the `--timeout` flag. Each invocation returns a *new* Context and thus
// resets the timeout.
func (c *CliContext) Context() context.Context {
	// We ignore cancelFn since deferring it would cancel the context
	// immediately. This leakage is fine for our usage since the lifetime is
	// scoped to the execution of the binary.

	ctx, _ := context.WithTimeout(c.ctx, c.Timeout)
	return ctx
}

func (c *CliContext) Log() *zerolog.Logger {
	return zerolog.Ctx(c.ctx)
}
