# optable-pair-cli
An open-source Command Line Interface (CLI) utility written in Golang to allow any PAIR partner of an Optable Data Collaboration Node (DCN) user to perform PAIR (Publisher Advertiser Identity Reconciliation) operations in a secure dual clean room environment.

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

The successfully compiled binary will be located in `bin/pair`.

# Usage
## Preparing the Input File
The input file that you provide to the `pair` utility should contain a line-separated list of sha256 hashed email identifiers, with a prefix of `e:`. Suppose the input file is named `input.csv`, you can hash the identifiers using the following command:
```bash
for id in $(input.csv); do echo e:$(echo -n $id | sha256sum | cut -d " " -f 1) >> hashed_input.csv; done
```

## Run the PAIR operation
To perform a secure PAIR clean room operation with a DCN, you must first obtain an `<pair-cleanroom-token>` from the Optable DCN's operator. You can then run the following command to generate a secret key.

```bash
bin/pair generate-key
```

The key is saved locally in `$XDG_CONFIG_HOME/pair/`. This directory is created with the proper file permissions to prevent snooping since it will contain private keys associated with the PAIR operation.

You can now run the PAIR operation using the following command:

```bash
token=<pair-cleanroom-token>
bin/pair run $token -i hashed_input.csv
```

You can optionally provide the argument `-o` or `--output` to specify the output directory, which will then compute the intersection of the triple encrypted PAIR IDs locally on your machine, decrypt it using the private key, and store the result in the specified directory. You can also use the argument `-n` or `--num-threads` to control the concurrency of the operation.
