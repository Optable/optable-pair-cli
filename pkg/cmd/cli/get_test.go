package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	v1 "github.com/optable/match-api/v2/gen/optable/external/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestCleanroomRun(t *testing.T) {
	t.Parallel()

	var (
		cleanroomName = "cleanrooms/test"
		salt          = "salt"
	)

	assertGetRequest := func(r *http.Request) {
		request, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		protoReq := &v1.GetCleanroomRequest{}
		err = proto.Unmarshal(request, protoReq)
		require.NoError(t, err)
		require.Equal(t, cleanroomName, protoReq.Name)
		require.Equal(t, v1.GetCleanroomRequest_FULL, protoReq.View)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/api/external/v1/cleanroom/get":
			assertGetRequest(r)

			cleanroom := v1.Cleanroom{}
			data, err := proto.Marshal(&cleanroom)
			if err != nil {
				t.Errorf("Failed to marshal response: %v", err)
			}
			_, err = w.Write(data)
			if err != nil {
				t.Errorf("Failed to write response body: %v", err)
			}
			w.WriteHeader(http.StatusOK)

		default:
			t.Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	token, err := generateToken(server.URL, cleanroomName, salt)
	require.NoError(t, err)

	getCmd := GetCmd{
		PairCleanroomToken: token,
		View:               "full",
	}

	cli := Cli{CleanroomCmd: CleanroomCmd{Get: getCmd}}
	cfg := &Config{}
	cmdCtx, err := cli.NewContext(cfg)
	require.NoError(t, err)

	err = getCmd.Run(cmdCtx)
	require.NoError(t, err)
}

func TestCleanroomRun_RequestFail(t *testing.T) {
	t.Parallel()

	var (
		cleanroomName = "cleanrooms/test"
		salt          = "salt"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/api/external/v1/cleanroom/get":
			w.WriteHeader(http.StatusNotFound)

		default:
			t.Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	token, err := generateToken(server.URL, cleanroomName, salt)
	require.NoError(t, err)

	getCmd := GetCmd{
		PairCleanroomToken: token,
		View:               "full",
	}

	cli := Cli{CleanroomCmd: CleanroomCmd{Get: getCmd}}
	cfg := &Config{}
	cmdCtx, err := cli.NewContext(cfg)
	require.NoError(t, err)

	err = getCmd.Run(cmdCtx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "404")
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
