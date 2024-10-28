package cli

import (
	"fmt"
	"optable-pair-cli/pkg/keys"
)

type (
	KeygenCmd struct {
		Force bool `cmd:"" short:"f" help:"If set, will overwrite the existing key. Please note that overwriting an existing key may affect running matches."`
	}
)

func (c *KeygenCmd) Run(cli *CliContext) error {
	var conf *keys.KeyConfig

	if cli.config.keyConfig == nil || c.Force {
		key, err := keys.GenerateKeyConfig()
		if err != nil {
			return err
		}

		// overwrite the key config
		conf = key
		cli.config.keyConfig = conf
		cli.SaveConfig()

		fmt.Println("The following key has been generated and saved to: ", cli.config.configPath)
	} else {
		conf = cli.config.keyConfig
		fmt.Printf("Key already exists at: %s. Use --force to overwrite.\n", cli.config.configPath)
	}

	return nil
}
