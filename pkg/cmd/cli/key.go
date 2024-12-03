package cli

import (
	"fmt"
	"optable-pair-cli/pkg/keys"
)

type (
	CreateCmd struct {
		Force bool `cmd:"" short:"f" help:"If set, will overwrite the existing key. Please note that overwriting an existing key may affect currently running matches."`
	}
)

func (c *CreateCmd) Run(cli *CmdContext) error {
	var conf *keys.KeyConfig

	if cli.config.keyConfig == nil || c.Force {
		key, err := keys.GenerateKeyConfig()
		if err != nil {
			return err
		}

		// overwrite the key config
		conf = key
		cli.config.keyConfig = conf
		err = cli.SaveConfig(cli.keyContext)
		if err != nil {
			return err
		}

		fmt.Println("The following key has been generated and saved to: ", cli.config.configPath)
	} else {
		fmt.Printf(`Key already exists at: %s.

!!CAUTION!!
You may overwrite this existing key but this action is irreversible and may invalidate any PAIR clean rooms that are currently in progress. Use --force to proceed with overwriting.
`, cli.config.configPath)
	}

	return nil
}
