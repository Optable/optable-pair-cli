# optable-pair-cli
[![optable-pair-cli CI](https://github.com/Optable/optable-pair-cli/actions/workflows/pr.yml/badge.svg?event=push)](https://github.com/Optable/optable-pair-cli/actions/workflows/pr.yml)

The optable-pair-CLI is an open-source Command Line Interface (CLI) utility written in Golang that enables an advertiser clean room invited by an Optable publisher to run a secure PAIR (Publisher Advertiser Identity Reconciliation) match and activation. The utility implements the advertiser clean room side of the 2 clean room variant of the open PAIR protocol, while the Optable Data Collaboration Platform powers the publisher clean room side of the operation, automating matching and publisher PAIR ID activation.

The PAIR protocol enables targeting ads to matched users without learning who they are. For more details on how the PAIR protocol for 2 clean rooms works, see the [README in Optable's open-source match library](https://github.com/Optable/match/blob/main/pkg/pair/README.md) and the [IAB Tech Lab's PAIR standard](https://iabtechlab.com/pair/).

# Build
To build the CLI, run the following command:
```bash
# clone the repo and go to the directory
git clone https://github.com/Optable/optable-pair-cli.git && cd optable-pair-cli

# compile:
make

# or more specifically:
make build
```

The successfully compiled binary will be located in `bin/opair`. Alternatively, you can download the latest released binary [here](https://github.com/Optable/optable-pair-cli/releases/latest).

# Usage
## Preparing the Input File
The input file that you provide to the `opair` utility should contain a line-separated list of sha256 hashed email identifiers. Suppose the input file is named `input.csv`, you can hash the identifiers using the following command:
```bash
for id in $(cat input.csv); do echo $(echo -n $id | sha256sum | cut -d " " -f 1) >> hashed_input.csv; done
```

## Run the PAIR operation
To perform a secure PAIR clean room operation with a DCN, you must first obtain an `<pair-cleanroom-token>` from the Optable DCN's operator. You can then run the following command to generate a secret key.

```bash
bin/opair key create
```

The key is saved locally in `$XDG_CONFIG_HOME/opair/`. This directory is created with the proper file permissions to prevent snooping since it will contain private keys associated with the PAIR operation.

You can now run the PAIR operation using the following command:

```bash
token=<pair-cleanroom-token>
bin/opair cleanroom run $token -i hashed_input.csv
```

You can optionally provide the argument `-o` or `--output` to specify the output directory, which will then compute the intersection of the triple encrypted PAIR IDs locally on your machine, decrypt it using the private key, and store the result in the specified directory. You can also use the argument `-n` or `--num-threads` to control the concurrency of the operation.
