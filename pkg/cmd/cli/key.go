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

func (c *CreateCmd) Run(cli *CliContext) error {
	var conf *keys.KeyConfig

	if cli.config.keyConfig == nil || c.Force {
		key, err := keys.GenerateKeyConfig()
		if err != nil {
			return err
		}

		// overwrite the key config
		conf = key
		cli.config.keyConfig = conf
		cli.SaveConfig(cli.keyContext)

		fmt.Println("The following key has been generated and saved to: ", cli.config.configPath)
	} else {
		conf = cli.config.keyConfig
		fmt.Printf("Key already exists at: %s. Use --force to overwrite.\n", cli.config.configPath)
	}

	return nil
}
