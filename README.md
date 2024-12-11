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
The successfully compiled binary will be located in `bin/opair`.

## Build on windows
To build the CLI on windows, you would need to first install make.
You can either install make directly [here](https://gnuwin32.sourceforge.net/packages/make.htm) or through [chocolatey](https://chocolatey.org/install), in which case, you can run the following command in **power shell**:
```bash
choco install make
```

After installing make, you can run the following command in **power shell** to build the CLI:
```bash
make
```
The successfully compiled binary will be located in `bin\opair.exe`.


# Download binaries
You can also download the pre-built binaries [here](https://github.com/Optable/optable-pair-cli/releases/latest) under the assets of the latest release.

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

# Pre-commit and Linting

This repsitory uses pre-commit and golangci-lint. To install pre-commit please run the following:

```
pip install pre-commit
```

Then run: `pre-commit install`

And to install golangci-lint please follow the instructions [here](https://golangci-lint.run/welcome/install/#local-installation).

# Integration Tests

## Run fake-gcs-server

Integration tests require disk space and [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) to be running on your machine.

To run integration tests, you can start `fake-gcs-server` in Docker with the following command:

```bash
docker run -d --name fake-gcs-server -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host 0.0.0.0:4443
```

[!NOTE]
The `-scheme http` and `-public-host 0.0.0.0:4443` flags are required to run the tests correctly:
- `-public-host 0.0.0.0:4443` ensures that readers are created correctly. For more details, refer to [this issue]
 (https://github.com/fsouza/fake-gcs-server/issues/201).
- `-scheme http` is required for proper local hosting without TLS checks. The GCS client has issues connecting to HTTPS servers, as it verifies the TLS certificate and fails in this setup.

## Run Integration Tests

Use the following command to run the integration tests:

```bash
STORAGE_EMULATOR_HOST=http://0.0.0.0:4443 go test ./...
```
