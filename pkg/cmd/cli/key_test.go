package cli

import (
	"encoding/json"
	"io"
	"os"
	"path"
	"testing"

	"optable-pair-cli/pkg/keys"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestKeyCreate(t *testing.T) {
	t.Parallel()

	tmpDir := path.Join(os.TempDir(), uuid.New().String())
	err := os.MkdirAll(tmpDir, os.ModePerm)
	require.NoError(t, err, "must create temp dir")

	keyConfigFile := path.Join(tmpDir, "test_config.json")
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err, "must remove temp dir")
	}()

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

	// test `force = true`
	createKeyCmd = CreateCmd{Force: true}

	err = createKeyCmd.Run(cmdCtx)
	require.NoError(t, err)

	file, err = os.Open(keyConfigFile)
	require.NoError(t, err)

	keyData, err = io.ReadAll(file)
	require.NoError(t, err)

	updatedKeyConfig := map[string]keys.KeyConfig{}
	err = json.Unmarshal(keyData, &updatedKeyConfig)
	require.NoError(t, err)

	require.NotEmpty(t, updatedKeyConfig["default"])
	require.NotEmpty(t, updatedKeyConfig["default"].ID)
	require.NotEmpty(t, updatedKeyConfig["default"].Key)
	require.Equal(t, "1", updatedKeyConfig["default"].Mode)
	require.NotEmpty(t, updatedKeyConfig["default"].CreatedAt)

	require.NotEqual(t, keyConfig["default"].ID, updatedKeyConfig["default"].ID, "ID should change")
	require.NotEqual(t, keyConfig["default"].Key, updatedKeyConfig["default"].Key, "Key should change")

	// test `force = false`
	createKeyCmd = CreateCmd{Force: false}
	err = createKeyCmd.Run(cmdCtx)
	require.NoError(t, err)

	file, err = os.Open(keyConfigFile)
	require.NoError(t, err)

	keyData, err = io.ReadAll(file)
	require.NoError(t, err)

	updatedKeyConfig2 := map[string]keys.KeyConfig{}
	err = json.Unmarshal(keyData, &updatedKeyConfig2)
	require.NoError(t, err)

	require.NotEmpty(t, updatedKeyConfig2["default"])
	require.Equal(t, updatedKeyConfig["default"].ID, updatedKeyConfig2["default"].ID, "ID should not change")
	require.Equal(t, updatedKeyConfig["default"].Key, updatedKeyConfig2["default"].Key, "Key should not change")
}
