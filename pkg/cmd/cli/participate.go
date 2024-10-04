package cli

type (
	ParticipateCmd struct {
		PairCleanroomToken string `arg:"" help:"The PAIR clean room token to use for the operation."`
		Input              string `arg:"" help:"The input file containing the advertiser data to be hashed and encrypted."`
		AdvertiserKey      string `cmd:"" short:"k" help:"The advertiser private key to use for the operation. If not provided, the key saved in the cofinguration file will be used."`
		Output             string `cmd:"" short:"o" help:"The output file to write the advertiser data to, default to stdout."`
	}
)

func (c *ParticipateCmd) Run(cli *CliContext) error {
	return nil
}
