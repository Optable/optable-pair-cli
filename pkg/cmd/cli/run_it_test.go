package cli

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	obucket "optable-pair-cli/pkg/bucket"
	"optable-pair-cli/pkg/internal"
	"optable-pair-cli/pkg/keys"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	v1 "github.com/optable/match-api/v2/gen/optable/external/v1"
	"github.com/optable/match/pkg/pair"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	sha256SaltSize = 32
	genInputNumber = 1001

	keyContext = "default"
)

var (
	sampleBucket = uuid.New().String()
	// insecureHTTPClient is used to create a client that skips TLS verification.
	// it is required for local testing with the storage emulator.
	insecureHTTPClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
)

func TestRun(t *testing.T) {
	t.Parallel()

	// STORAGE_EMULATOR_HOST is required to run this test
	bucketURL := os.Getenv("STORAGE_EMULATOR_HOST")
	if bucketURL == "" {
		t.Skip("STORAGE_EMULATOR_HOST is required to run this test")
	}

	requireCreateBucket(t)

	// update HTTP client for bucket completer
	obucket.HTTPClient = insecureHTTPClient

	var (
		tmpDir                          = os.TempDir()
		output                          = path.Join(tmpDir, "output.csv")
		salt                            = requireGenSalt(t)
		cleanroomName                   = "cleanrooms/" + uuid.New().String()
		expireTime                      = time.Now().Add(1 * time.Hour)
		bucket                          = fmt.Sprintf("gs://%s", sampleBucket)
		advertiserTwiceEncryptedFolder  = fmt.Sprintf("%s/%s/advertiser_twice_encrypted", bucket, cleanroomName)
		publisherTwiceEncryptedFolder   = fmt.Sprintf("%s/%s/publisher_twice_encrypted", bucket, cleanroomName)
		advertiserTripleEncryptedFolder = fmt.Sprintf("%s/%s/advertiser_triple_encrypted", bucket, cleanroomName)
		publisherTripleEncryptedFolder  = fmt.Sprintf("%s/%s/publisher_triple_encrypted", bucket, cleanroomName)
		publisherTwiceEncryptedData     = advertiserTwiceEncryptedFolder + "/data.csv"
		publisherTripleEncryptedData    = advertiserTripleEncryptedFolder + "/data.csv"
		cleanroom                       = v1.Cleanroom{
			Name:       cleanroomName,
			ExpireTime: timestamppb.New(expireTime),
			Config: &v1.Cleanroom_Config{
				Config: &v1.Cleanroom_Config_Pair{
					Pair: &v1.Cleanroom_Config_PairConfig{
						GcsToken: &v1.Cleanroom_Config_PairConfig_AuthToken{
							Value:      "gcsToken",
							ExpireTime: timestamppb.New(expireTime),
						},
						AdvertiserTwiceEncryptedDataUrl:  advertiserTwiceEncryptedFolder,
						PublisherTwiceEncryptedDataUrl:   publisherTwiceEncryptedFolder,
						AdvertiserTripleEncryptedDataUrl: advertiserTripleEncryptedFolder,
						PublisherTripleEncryptedDataUrl:  publisherTripleEncryptedFolder,
					},
				},
			},
			Participants: []*v1.Cleanroom_Participant{
				{
					Role:  v1.Cleanroom_Participant_PUBLISHER,
					State: v1.Cleanroom_Participant_DATA_CONTRIBUTED,
				},
				{
					Role:  v1.Cleanroom_Participant_ADVERTISER,
					State: v1.Cleanroom_Participant_INVITED,
				},
			},
		}
		nextPuplisherState  = v1.Cleanroom_Participant_DATA_TRANSFORMED
		nextAdvertiserState = v1.Cleanroom_Participant_DATA_CONTRIBUTED
	)

	// generate publisher data
	publisherKey, err := keys.NewPrivateKey(pair.PAIRSHA256Ristretto255)
	require.NoError(t, err)
	publisherPairKey, err := keys.NewPAIRPrivateKey(salt, publisherKey)
	require.NoError(t, err)

	tmpConfigFile, err := os.CreateTemp(tmpDir, "test_config.json")
	require.NoError(t, err, "must create temp file")

	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err, "must remove temp dir")
	}()

	requireWriteNewKey(t, tmpConfigFile)
	err = tmpConfigFile.Close()
	require.NoError(t, err, "must close temp file")

	requireGenPublisherTwiceEncryptedData(t, publisherPairKey, publisherTwiceEncryptedData)
	advertiserInput := requireGenAdvertiserInput(t, tmpDir)

	// init optable mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCleanroom := func(w http.ResponseWriter) {
			data, err := proto.Marshal(&cleanroom)
			if err != nil {
				t.Errorf("Failed to marshal response: %v", err)
			}
			_, err = w.Write(data)
			if err != nil {
				t.Errorf("Failed to write response body: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		}
		switch r.URL.Path {
		case internal.AdminCleanroomRefreshTokenURL, internal.AdminCleanroomGetURL:
			writeCleanroom(w)

		case internal.AdminCleanroomAdvanceURL:
			cleanroom.Participants[0].State = nextPuplisherState
			cleanroom.Participants[1].State = nextAdvertiserState
			nextPuplisherState = v1.Cleanroom_Participant_SUCCEEDED
			nextAdvertiserState = v1.Cleanroom_Participant_DATA_TRANSFORMED
			// add publisher triple encrypted data
			requireGenPublisherTripleEncryptedData(t, publisherPairKey, publisherTwiceEncryptedData, publisherTripleEncryptedData)
			writeCleanroom(w)

		default:
			t.Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	token, err := generateToken(server.URL, cleanroomName, salt)
	require.NoError(t, err)

	runCommand := RunCmd{
		PairCleanroomToken: token,
		Input:              advertiserInput,
		NumThreads:         1,
		Output:             output,
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{
			Run: runCommand,
		},
		Context: keyContext,
	}

	cfg := &Config{
		configPath: tmpConfigFile.Name(),
	}

	cmdCtx, err := cli.NewContext(cfg)
	require.NoError(t, err)

	err = runCommand.Run(cmdCtx)
	require.NoError(t, err)
}

func requireGenSalt(t *testing.T) string {
	t.Helper()
	salt := make([]byte, sha256SaltSize)
	_, err := rand.Read(salt)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(salt)
}

func requireWriteNewKey(t *testing.T, keyConfigFile *os.File) {
	keyConfig, err := keys.GenerateKeyConfig()
	require.NoError(t, err, "must generate key config")

	configs := map[string]keys.KeyConfig{
		keyContext: *keyConfig,
	}

	keyData, err := json.Marshal(&configs)
	require.NoError(t, err, "must marshal key config")

	_, err = keyConfigFile.Write(keyData)
	require.NoError(t, err, "must create temp file")
}

func requireGenAdvertiserInput(t *testing.T, dir string) string {
	t.Helper()

	tmpInputFile, err := os.CreateTemp(dir, "input.csv")
	require.NoError(t, err, "must create temp file")

	defer func() {
		err = tmpInputFile.Close()
		require.NoError(t, err, "must close temp file")
	}()

	w := csv.NewWriter(tmpInputFile)
	defer w.Flush()

	for i := range genInputNumber {
		shaEncoder := sha256.New()
		shaEncoder.Write([]byte(fmt.Sprintf("%d@gmail.com", i)))
		hem := shaEncoder.Sum(nil)
		err = w.Write([]string{fmt.Sprintf("%x", hem)})
		require.NoError(t, err, "must write email")
	}
	return tmpInputFile.Name()
}

func requireCreateGCSClient(t *testing.T) *storage.Client {
	t.Helper()
	ctx := context.Background()

	client, err := storage.NewClient(ctx,
		option.WithHTTPClient(insecureHTTPClient),
		option.WithEndpoint(os.Getenv("STORAGE_EMULATOR_HOST")+"/storage/v1/"),
	)
	require.NoError(t, err, "must create storage client")
	return client
}

func requireCreateBucket(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	client := requireCreateGCSClient(t)
	defer func() {
		err := client.Close()
		require.NoError(t, err, "must close storage client")
	}()

	err := client.Bucket(sampleBucket).Create(ctx, "_", &storage.BucketAttrs{
		Location: "US",
	})
	require.NoError(t, err, "must create bucket")
}

func requireGenPublisherTwiceEncryptedData(t *testing.T, pairKey *pair.PrivateKey, twiceEncrypted string) {
	t.Helper()
	ctx := context.Background()

	client := requireCreateGCSClient(t)
	defer func() {
		err := client.Close()
		require.NoError(t, err, "must close storage client")
	}()

	twiceEncryptedWriter := client.Bucket(sampleBucket).Object(twiceEncrypted).NewWriter(ctx)
	defer func() {
		err := twiceEncryptedWriter.Close()
		require.NoError(t, err, "must close GCS writer")
	}()
	twiceEncryptedCsvWriter := csv.NewWriter(twiceEncryptedWriter)
	defer twiceEncryptedCsvWriter.Flush()

	for i := range genInputNumber {
		shaEncoder := sha256.New()
		shaEncoder.Write([]byte(fmt.Sprintf("%d@gmail.com", i)))
		hem := shaEncoder.Sum(nil)

		twiceEnc, err := pairKey.Encrypt(hem)
		require.NoError(t, err, "must encrypt email")
		err = twiceEncryptedCsvWriter.Write([]string{string(twiceEnc)})
		require.NoError(t, err, "must write email")
	}
}

func requireGenPublisherTripleEncryptedData(t *testing.T, pairKey *pair.PrivateKey, twiceEncrypted, tripleEncrypted string) {
	t.Helper()
	ctx := context.Background()

	client := requireCreateGCSClient(t)
	defer func() {
		err := client.Close()
		require.NoError(t, err, "must close storage client")
	}()

	twiceEncrypteReader, err := client.Bucket(sampleBucket).Object(twiceEncrypted).NewReader(ctx)
	require.NoError(t, err, "must create GCS reader")
	defer func() {
		err := twiceEncrypteReader.Close()
		require.NoError(t, err, "must close GCS reader")
	}()
	twiceEncryptedCsvReader := csv.NewReader(twiceEncrypteReader)

	tripleEncryptedWriter := client.Bucket(sampleBucket).Object(tripleEncrypted).NewWriter(ctx)
	defer func() {
		err := tripleEncryptedWriter.Close()
		require.NoError(t, err, "must close GCS writer")
	}()
	tripleEncryptedCsvWriter := csv.NewWriter(tripleEncryptedWriter)
	defer tripleEncryptedCsvWriter.Flush()

	data, err := twiceEncryptedCsvReader.ReadAll()
	require.NoError(t, err, "must read twice encrypted data")
	for _, line := range data {
		require.Len(t, line, 1, "must have one record")
		record := line[0]

		tripleEnc, err := pairKey.ReEncrypt([]byte(record))
		require.NoError(t, err, "must re-encrypt record")
		err = tripleEncryptedCsvWriter.Write([]string{string(tripleEnc)})
		require.NoError(t, err, "must write email")
	}
}
