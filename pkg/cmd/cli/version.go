package cli

import "fmt"

// version will be set to be the latest git tag through build flag"
var version string

type (
	VersionCmd struct{}
)

func (c *VersionCmd) Run(cli *CliContext) error {
	fmt.Println(version)
	return nil
}
