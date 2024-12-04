package cli

import (
	"fmt"
)

var version string

type (
	VersionCmd struct{}
)

func (c *VersionCmd) Run(_ *CmdContext) error {
	fmt.Println(version)
	return nil
}
