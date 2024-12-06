package cli

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"optable-pair-cli/pkg/keys"

	"github.com/stretchr/testify/require"
)

func TestKeyCreate(t *testing.T) {
	t.Parallel()

	tmpDir := os.TempDir()
	keyConfigFile := tmpDir + "/test_config.json"

	createKeyCmd := CreateCmd{Force: false}
	cli := Cli{
		KeyCmd:  KeyCmd{Create: createKeyCmd},
		Context: "default",
	}
	cfg := &Config{
		configPath: keyConfigFile,
	}
	cmdCtx, err := cli.NewContext(cfg)
	require.NoError(t, err)

	err = createKeyCmd.Run(cmdCtx)
	require.NoError(t, err)

	info, err := os.Stat(keyConfigFile)
	require.NoError(t, err)
	require.NotEmpty(t, info.Size())

	file, err := os.Open(keyConfigFile)
	require.NoError(t, err)
	defer file.Close()

	keyData, err := io.ReadAll(file)
	require.NoError(t, err)

	keyConfig := map[string]keys.KeyConfig{}
	err = json.Unmarshal(keyData, &keyConfig)
	require.NoError(t, err)

	require.NotEmpty(t, keyConfig["default"])
	require.NotEmpty(t, keyConfig["default"].ID)
	require.NotEmpty(t, keyConfig["default"].Key)
	require.Equal(t, "1", keyConfig["default"].Mode)
	require.NotEmpty(t, keyConfig["default"].CreatedAt)

	// test force
	createKeyCmd = CreateCmd{Force: true}
	cli = Cli{
		KeyCmd:  KeyCmd{Create: createKeyCmd},
		Context: "default",
	}
	cfg = &Config{
		configPath: keyConfigFile,
	}
	cmdCtx, err = cli.NewContext(cfg)
	require.NoError(t, err)

	err = createKeyCmd.Run(cmdCtx)
	require.NoError(t, err)

	info, err = os.Stat(keyConfigFile)
	require.NoError(t, err)
	require.NotEmpty(t, info.Size())

	updatedFile, err := os.Open(keyConfigFile)
	require.NoError(t, err)
	defer updatedFile.Close()

	keyData, err = io.ReadAll(updatedFile)
	require.NoError(t, err)

	updatedKeyConfig := map[string]keys.KeyConfig{}
	err = json.Unmarshal(keyData, &updatedKeyConfig)
	require.NoError(t, err)

	require.NotEmpty(t, updatedKeyConfig["default"])
	require.NotEmpty(t, updatedKeyConfig["default"].ID)
	require.NotEmpty(t, updatedKeyConfig["default"].Key)
	require.Equal(t, "1", updatedKeyConfig["default"].Mode)
	require.NotEmpty(t, updatedKeyConfig["default"].CreatedAt)

	require.NotEqual(t, keyConfig["default"].ID, updatedKeyConfig["default"].ID)
	require.NotEqual(t, keyConfig["default"].Key, updatedKeyConfig["default"].Key)
}
