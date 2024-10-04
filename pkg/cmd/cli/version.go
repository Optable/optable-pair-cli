package cli

import (
	"fmt"
)

var version string

type (
	VersionCmd struct{}
)

func (c *VersionCmd) Run(cli *CliContext) error {
	fmt.Println(version)
	return nil
}
