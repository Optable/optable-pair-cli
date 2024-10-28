package main

import (
	"optable-pair-cli/pkg/cmd/cli"

	"github.com/adrg/xdg"
	"github.com/alecthomas/kong"
)

const description = `
Optable PAIR match utility

opair is an open-source utility used for matching encrypted Email data
with media companies that use the Optable data collaboration platform. The
utility runs a secure and privacy protected match on encrypted data, and is
based on the IAB Tech Lab's open PAIR (Publisher Advertiser Identity
Reconciliation) protocol for 2 clean rooms. The opair utility is an
implementation of the advertiser clean room side of the protocol, while media
companies using Optable benefit from a fully automated approach to PAIR match
activation.

For more details on how the PAIR protocol for 2 clean rooms works, see
https://github.com/Optable/match/blob/main/pkg/pair/README.md
and https://iabtechlab.com/pair/
`

const keyConfigPath = "opair/key/key.json"

func main() {
	var c cli.Cli
	kongCtx := kong.Parse(&c,
		kong.Name("opair"),
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
