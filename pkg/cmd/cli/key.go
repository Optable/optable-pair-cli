package cli

import (
	"encoding/json"
	"fmt"
	"optable-pair-cli/pkg/keys"
)

type (
	GenerateKeyCmd struct {
		Force bool `cmd:"" short:"f" help:"If set, will overwrite the existing key. Please note that rotated key will invalidate ongoing PAIR operations."`
	}
)

func (c *GenerateKeyCmd) Run(cli *CliContext) error {
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
		fmt.Println("Key already exists. Use --force to overwrite.")
	}

	marshaled, err := json.MarshalIndent(conf, "", "   ")
	if err != nil {
		return err
	}

	fmt.Println(string(marshaled))

	return nil
}
