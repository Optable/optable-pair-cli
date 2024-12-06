package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCleanroomRun(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/cleanroom/get":
			w.WriteHeader(http.StatusOK)
			panic("Unexpected call to /v1/cleanroom/get")
		case "/v1/cleanroom/refresh-token":
			w.WriteHeader(http.StatusOK)
			panic("Unexpected call to /v1/cleanroom/refresh-token")
		case "/v1/cleanroom/advance-advertiser-state":
			w.WriteHeader(http.StatusOK)
			panic("Unexpected call to /v1/cleanroom/advance-advertiser-state")
		default:
			w.WriteHeader(http.StatusOK)
			panic(fmt.Sprintf("Unexpected call to %s", r.URL.Path))
		}
	}))
	defer server.Close()

	cleanroomName := "cleanrooms/test"
	salt := "salt"
	token, err := generateToken(server.URL, cleanroomName, salt)
	require.NoError(t, err)
	getCmd := GetCmd{
		PairCleanroomToken: token,
		View:               "full",
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{Get: getCmd},
	}

	cfg := &Config{}
	cmdCtx, err := cli.NewContext(cfg)
	require.NoError(t, err)
	err = getCmd.Run(cmdCtx)
	require.NoError(t, err)
}

func generateToken(url, cleanroomName, salt string) (string, error) {
	now := time.Now()
	host := url

	claims := jwt.MapClaims{
		"cleanroom": cleanroomName,
		"exp":       now.Add(8 * 24 * time.Hour).Unix(),
		"iss":       host,
		"salt":      salt,
		"uuid":      uuid.NewString(),
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodES256, claims)

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	tokStr, err := tok.SignedString(privateKey)
	if err != nil {
		return "", err
	}

	return tokStr, nil
}
