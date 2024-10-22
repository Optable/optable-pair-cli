package main

import (
	"optable-pair-cli/pkg/cmd/cli"

	"github.com/adrg/xdg"
	"github.com/alecthomas/kong"
)

const description = `
Optable PAIR CLI interface

pair is a tool for interacting with an Optable DCN from the command line
to perform a PAIR (Publisher Advertiser Identity Reconciliation) operation
in a dual data clean room environment, where Optable DCN represents
the publisher data clean room operator.

For more details on how a PAIR clean room operation works, see https://github.com/Optable/match/blob/main/pkg/pair/README.md
and https://iabtechlab.com/pair/.
`

const keyConfigPath = "opair/key/key.json"

func main() {
	var c cli.Cli
	kongCtx := kong.Parse(&c,
		kong.Name("pair"),
		kong.Description(description),
		&kong.HelpOptions{
			Compact: true,
			// Ensure that sub-commands and their children are not shown by
			// default. This removes a lot of the noise in the top-level help
			// where the total sub-commands is quite high.
			NoExpandSubcommands: true,
			WrapUpperBound:      80,
		},
	)

	configPath, err := xdg.ConfigFile(keyConfigPath)
	if err != nil {
		kongCtx.FatalIfErrorf(err)
	}

	conf, err := cli.LoadKeyConfig(configPath)
	if err != nil {
		kongCtx.FatalIfErrorf(err)
	}

	cliCtx, err := c.NewContext(conf)
	kongCtx.FatalIfErrorf(err)

	kongCtx.FatalIfErrorf(kongCtx.Run(cliCtx))
}
