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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	obucket "optable-pair-cli/pkg/bucket"
	"optable-pair-cli/pkg/internal"
	"optable-pair-cli/pkg/keys"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	v1 "github.com/optable/match-api/v2/gen/optable/external/v1"
	"github.com/optable/match/pkg/pair"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	sha256SaltSize        = 32
	genEmailsSourceNumber = 1001

	keyContext = "default"
)

type cmdTestSuite struct {
	suite.Suite

	// suite parameters for all tests
	ctx          context.Context
	gcsClient    *storage.Client
	sampleBucket string

	// unique params for each test case
	tmpDir string
	params cmdTestParams
}

type cmdTestParams struct {
	cleanroomName               string
	expireTime                  time.Time
	emailsSource                []string
	salt                        string
	publisherPairKey            *pair.PrivateKey
	advertiserKeyConfig         *keys.KeyConfig
	advertiserInputFilePath     string
	advertiserKeyConfigFilePath string
	publisherPAIRIDsFolderPath  string
	advertiserOutputFolderPath  string
}

// TestCmd requires the STORAGE_EMULATOR_HOST environment variable to be set.
// After running fake-gcs-server:
// docker run -d --name fake-gcs-server -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host 0.0.0.0:4443
// export `STORAGE_EMULATOR_HOST=http://0.0.0.0:4443“ to run the test:
// `STORAGE_EMULATOR_HOST=http://0.0.0.0:4443 go test ./pkg/cmd/cli/... -run="TestCmd"“
func TestCmd(t *testing.T) {
	t.Parallel()

	// STORAGE_EMULATOR_HOST is required to run this test
	bucketURL := os.Getenv("STORAGE_EMULATOR_HOST")
	if bucketURL == "" {
		t.Errorf("STORAGE_EMULATOR_HOST is required to run this test")
	} else {
		suite.Run(t, new(cmdTestSuite))
	}
}

func (s *cmdTestSuite) SetupSuite() {
	s.ctx = context.Background()
	s.sampleBucket = uuid.NewString()

	// insecureHTTPClient is used to create a client that skips TLS verification.
	// it is required for local testing with the storage emulator.
	insecureGCSHTTPClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// update HTTP client for bucket completer
	obucket.GCSClientOptions = append(obucket.GCSClientOptions, option.WithHTTPClient(insecureGCSHTTPClient))

	// init GCS emulator client and bucket
	bucketURL := os.Getenv("STORAGE_EMULATOR_HOST")

	// check if fake-gcs-server is reachable.
	getBuckets, err := url.Parse(bucketURL + "/storage/v1/b")
	s.Require().NoError(err, "must parse fake-gcs-server URL")
	resp, err := insecureGCSHTTPClient.Do(&http.Request{
		Method: http.MethodGet,
		URL:    getBuckets,
	})
	s.Require().NoError(err, "must reach fake-gcs-server")
	defer resp.Body.Close()
	s.Require().Equal(http.StatusOK, resp.StatusCode, "must reach fake-gcs-server")

	client, err := storage.NewClient(s.ctx,
		option.WithHTTPClient(insecureGCSHTTPClient),
		option.WithEndpoint(bucketURL+"/storage/v1/"),
	)
	s.Require().NoError(err, "must create storage client")
	err = client.Bucket(s.sampleBucket).Create(s.ctx, "_", &storage.BucketAttrs{
		Location: "US",
	})
	s.Require().NoError(err, "must create bucket")
	s.gcsClient = client

	// generate emails source once for all tests
	s.params.emailsSource = make([]string, genEmailsSourceNumber)
	shaEncoder := sha256.New()
	for i := range genEmailsSourceNumber {
		shaEncoder.Write([]byte(fmt.Sprintf("%d@gmail.com", i)))
		hem := shaEncoder.Sum(nil)
		s.params.emailsSource[i] = fmt.Sprintf("%x", hem)
	}
}

func (s *cmdTestSuite) TearDownAllSuite() {
	defer func() {
		if s.gcsClient == nil {
			return
		}
		err := s.gcsClient.Bucket(s.sampleBucket).Delete(s.ctx)
		s.Require().NoError(err, "must delete bucket")
	}()
	defer func() {
		if s.gcsClient == nil {
			return
		}
		err := s.gcsClient.Close()
		s.Require().NoError(err, "must close storage client")
	}()
}

func (s *cmdTestSuite) SetupTest() {
	id := uuid.New().String()
	s.params.cleanroomName = "cleanrooms/" + id
	s.params.expireTime = time.Now().Add(1 * time.Hour)

	s.tmpDir = path.Join(os.TempDir(), id)
	err := os.MkdirAll(s.tmpDir, os.ModePerm)
	s.Require().NoError(err, "must create temp dir")

	s.params.publisherPAIRIDsFolderPath = path.Join(s.tmpDir, "publisher_pair_id")
	s.params.advertiserOutputFolderPath = path.Join(s.tmpDir, "output")

	// create advertiser key config file in tmp folder
	keyConfig, err := keys.GenerateKeyConfig()
	s.Require().NoError(err, "must generate key config")
	s.params.advertiserKeyConfig = keyConfig
	s.params.advertiserKeyConfigFilePath = s.requireCreateNewKeyConfig(keyConfig)
	// create advertiser input file in tmp folder
	s.params.advertiserInputFilePath = s.requireCreateAdvertiserInputFile()

	// generate salt
	salt := make([]byte, sha256SaltSize)
	_, err = rand.Read(salt)
	s.Require().NoError(err)
	s.params.salt = base64.StdEncoding.EncodeToString(salt)

	// generate publisher PAIR key
	publisherKey, err := keys.NewPrivateKey(pair.PAIRSHA256Ristretto255)
	s.Require().NoError(err)
	s.params.publisherPairKey, err = keys.NewPAIRPrivateKey(s.params.salt, publisherKey)
	s.Require().NoError(err)

	// create publisher twice encrypted data in gcs emulator
	s.requireGenPublisherTwiceEncryptedData()
}

func (s *cmdTestSuite) TearDownTest() {
	defer func() {
		err := os.RemoveAll(s.tmpDir)
		s.Require().NoError(err, "must remove temp dir")
	}()
}

func (s *cmdTestSuite) TestRun() {
	s.testRun(1, s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_INVITED))
}

func (s *cmdTestSuite) TestRun_MultipleWorkers() {
	s.testRun(4, s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_INVITED))
}

func (s *cmdTestSuite) TestRunDefaultWorkers() {
	s.testRun(-1, s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_INVITED))
}

func (s *cmdTestSuite) TestRun_StepTwo_ContributedContributed() {
	// arrange
	s.requirePrepareForStepTwo()

	// run from step two
	s.testRun(1, s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_DATA_CONTRIBUTED))
}

func (s *cmdTestSuite) TestRun_StepTwo_TransformedContributed() {
	// arrange
	s.requirePrepareForStepTwo()

	// run from step two
	s.testRun(1, s.newCleanroom(v1.Cleanroom_Participant_DATA_TRANSFORMED, v1.Cleanroom_Participant_DATA_CONTRIBUTED))
}

func (s *cmdTestSuite) requirePrepareForStepTwo() {
	cfg := &pairConfig{
		downscopedToken: "token",
		threads:         1,
		salt:            s.params.salt,
		key:             s.params.advertiserKeyConfig.Key,
		advTwicePath:    s.advertiserTwiceEncryptedGCSFolder(),
		advTriplePath:   s.advertiserTripleEncryptedGCSFolder(),
		pubTwicePath:    s.publisherTwiceEncryptedGCSFolder(),
		pubTriplePath:   s.publisherTripleEncryptedGCSFolder(),
	}
	err := cfg.hashEncryt(s.ctx, s.params.advertiserInputFilePath)
	s.Require().NoError(err)

	s.requireGenAdvertiserTripleEncryptedData()
}

func (s *cmdTestSuite) TestRun_StepThree_ContributedTransformed() {
	// arrange
	s.requirePrepareForStepThree()

	cleanroom := s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_DATA_TRANSFORMED)

	go func() {
		time.Sleep(2 * time.Second)
		// change publisher's state to data_transformed
		cleanroom.Participants[0].State = v1.Cleanroom_Participant_DATA_TRANSFORMED
	}()

	// run from step three: check if we wait on advertiser side for publisher to advance
	s.testRun(1, cleanroom)
}

func (s *cmdTestSuite) TestRun_StepThree_TransformedTransformed() {
	// arrange
	s.requirePrepareForStepThree()

	// run from step three: run match only
	s.testRun(1, s.newCleanroom(v1.Cleanroom_Participant_DATA_TRANSFORMED, v1.Cleanroom_Participant_DATA_TRANSFORMED))
}

func (s *cmdTestSuite) TestRun_StepThree_SucceededTransformed() {
	// arrange
	s.requirePrepareForStepThree()

	// run from step three: run match only
	s.testRun(1, s.newCleanroom(v1.Cleanroom_Participant_SUCCEEDED, v1.Cleanroom_Participant_DATA_TRANSFORMED))
}

func (s *cmdTestSuite) requirePrepareForStepThree() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internal.AdminCleanroomRefreshTokenURL, internal.AdminCleanroomGetURL:
			s.requireWriteCleanroomHandler(w, s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_DATA_TRANSFORMED))

		default:
			s.T().Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := internal.NewCleanroomClient(&internal.CleanroomToken{
		HashSalt:   s.params.salt,
		Cleanroom:  s.params.cleanroomName,
		Expiration: 10000,
		IssuerHost: server.URL,
	})
	s.Require().NoError(err)

	cfg := &pairConfig{
		downscopedToken: "token",
		threads:         1,
		salt:            s.params.salt,
		key:             s.params.advertiserKeyConfig.Key,
		advTwicePath:    s.advertiserTwiceEncryptedGCSFolder(),
		advTriplePath:   s.advertiserTripleEncryptedGCSFolder(),
		pubTwicePath:    s.publisherTwiceEncryptedGCSFolder(),
		pubTriplePath:   s.publisherTripleEncryptedGCSFolder(),
		cleanroomClient: client,
	}
	err = cfg.hashEncryt(s.ctx, s.params.advertiserInputFilePath)
	s.Require().NoError(err)

	err = cfg.reEncrypt(s.ctx, s.params.publisherPAIRIDsFolderPath)
	s.Require().NoError(err)

	s.requireGenAdvertiserTripleEncryptedData()
}

func (s *cmdTestSuite) TestRun_BadToken() {
	runCommand := RunCmd{
		Input:            s.params.advertiserInputFilePath,
		NumThreads:       1,
		Output:           s.params.advertiserOutputFolderPath,
		PublisherPAIRIDs: s.params.publisherPAIRIDsFolderPath,
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{
			Run: runCommand,
		},
		Context: keyContext,
	}

	cfg := &Config{
		configPath: s.params.advertiserKeyConfigFilePath,
	}

	cmdCtx, err := cli.NewContext(cfg)
	s.Require().NoError(err)

	// empty token
	runCommand.PairCleanroomToken = ""
	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "token is required")

	// invalid token
	runCommand.PairCleanroomToken = "invalid_token"
	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "failed to parse clean room token")
}

func (s *cmdTestSuite) TestRun_InvalidKey() {
	runCommand := RunCmd{
		PairCleanroomToken: s.requireGenerateToken("http://example.com", s.params.cleanroomName, s.params.salt),
		Input:              s.params.advertiserInputFilePath,
		NumThreads:         1,
		Output:             s.params.advertiserOutputFolderPath,
		PublisherPAIRIDs:   s.params.publisherPAIRIDsFolderPath,
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{
			Run: runCommand,
		},
		Context: keyContext,
	}

	cfg := &Config{
		configPath: s.params.advertiserKeyConfigFilePath,
	}

	cmdCtx, err := cli.NewContext(cfg)
	s.Require().NoError(err)

	// no key
	err = os.Remove(s.params.advertiserKeyConfigFilePath)
	s.Require().NoError(err, "must remove key config file")

	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "ReadKeyConfig")

	// invalid key
	s.params.advertiserKeyConfigFilePath = s.requireCreateNewKeyConfig(&keys.KeyConfig{
		ID:  uuid.NewString(),
		Key: "invalid_key",
	})
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "malformed key configuration")
}

func (s *cmdTestSuite) TestRun_FailGetCleanroom() {
	// get cleanroom is called in few places. iteration is used to simulate different responses.
	// first 2 calls will return cleanroom, the third one will return a 505 error.
	iteration := 0

	cleanroom := s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_INVITED)

	// init optable mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internal.AdminCleanroomGetURL:
			if iteration < 2 {
				s.requireWriteCleanroomHandler(w, cleanroom)
				iteration++
				return
			}
			w.WriteHeader(http.StatusInternalServerError)

		default:
			s.T().Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	runCommand := RunCmd{
		PairCleanroomToken: s.requireGenerateToken(server.URL, s.params.cleanroomName, s.params.salt),
		Input:              s.params.advertiserInputFilePath,
		NumThreads:         1,
		Output:             s.params.advertiserOutputFolderPath,
		PublisherPAIRIDs:   s.params.publisherPAIRIDsFolderPath,
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{
			Run: runCommand,
		},
		Context: keyContext,
	}

	cfg := &Config{
		configPath: s.params.advertiserKeyConfigFilePath,
	}

	cmdCtx, err := cli.NewContext(cfg)
	s.Require().NoError(err)

	// fail to get cleanroom in RunCmd (third call)
	iteration = 0
	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "GetCleanroom", "get cleanroom must fail")
	s.Require().Contains(err.Error(), "500", "must contain status code")

	// fail to get cleanroom for pair config (second call)
	iteration = 1
	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "unexpected status code", "get cleanroom must fail")
	s.Require().Contains(err.Error(), "500", "must contain status code")

	// fail to get downscoped token (first call)
	iteration = 2
	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "failed to get down scoped token", "get cleanroom must fail")
	s.Require().Contains(err.Error(), "500", "must contain status code")
}

func (s *cmdTestSuite) TestRun_FailToAdvance() {
	cleanroom := s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_INVITED)

	// init optable mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internal.AdminCleanroomRefreshTokenURL, internal.AdminCleanroomGetURL:
			s.requireWriteCleanroomHandler(w, cleanroom)

		case internal.AdminCleanroomAdvanceURL:
			w.WriteHeader(http.StatusInternalServerError)

		default:
			s.T().Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	runCommand := RunCmd{
		PairCleanroomToken: s.requireGenerateToken(server.URL, s.params.cleanroomName, s.params.salt),
		Input:              s.params.advertiserInputFilePath,
		NumThreads:         1,
		Output:             s.params.advertiserOutputFolderPath,
		PublisherPAIRIDs:   s.params.publisherPAIRIDsFolderPath,
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{
			Run: runCommand,
		},
		Context: keyContext,
	}

	cfg := &Config{
		configPath: s.params.advertiserKeyConfigFilePath,
	}

	cmdCtx, err := cli.NewContext(cfg)
	s.Require().NoError(err)

	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "500")
}

func (s *cmdTestSuite) TestRun_UnspecifiedParticipants() {
	cleanroom := s.newCleanroom(v1.Cleanroom_Participant_DATA_CONTRIBUTED, v1.Cleanroom_Participant_INVITED)
	// change participant states to unspecified
	cleanroom.Participants[0].Role = v1.Cleanroom_Participant_ROLE_UNSPECIFIED
	cleanroom.Participants[1].Role = v1.Cleanroom_Participant_ROLE_UNSPECIFIED

	// init optable mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internal.AdminCleanroomGetURL:
			s.requireWriteCleanroomHandler(w, cleanroom)

		default:
			s.T().Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	runCommand := RunCmd{
		PairCleanroomToken: s.requireGenerateToken(server.URL, s.params.cleanroomName, s.params.salt),
		Input:              s.params.advertiserInputFilePath,
		NumThreads:         1,
		Output:             s.params.advertiserOutputFolderPath,
		PublisherPAIRIDs:   s.params.publisherPAIRIDsFolderPath,
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{
			Run: runCommand,
		},
		Context: keyContext,
	}

	cfg := &Config{
		configPath: s.params.advertiserKeyConfigFilePath,
	}

	cmdCtx, err := cli.NewContext(cfg)
	s.Require().NoError(err)

	err = runCommand.Run(cmdCtx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "role unspecified for participant")
}

func (s *cmdTestSuite) requireWriteCleanroomHandler(w http.ResponseWriter, cleanroom *v1.Cleanroom) {
	w.WriteHeader(http.StatusOK)
	data, err := proto.Marshal(cleanroom)
	s.Require().NoError(err, "must marshal response")
	_, err = w.Write(data)
	s.Require().NoError(err, "failed to write response body")
}

func (s *cmdTestSuite) testRun(workersNum int, cleanroom *v1.Cleanroom) {
	// next states for the participants to advance the cleanroom
	nextState, stateExists := map[v1.Cleanroom_Participant_State]v1.Cleanroom_Participant_State{
		v1.Cleanroom_Participant_INVITED:          v1.Cleanroom_Participant_DATA_CONTRIBUTED,
		v1.Cleanroom_Participant_DATA_CONTRIBUTED: v1.Cleanroom_Participant_DATA_TRANSFORMED,
		v1.Cleanroom_Participant_DATA_TRANSFORMED: v1.Cleanroom_Participant_SUCCEEDED,
		v1.Cleanroom_Participant_SUCCEEDED:        v1.Cleanroom_Participant_SUCCEEDED, // do not change the state
	}, false

	// init optable mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internal.AdminCleanroomRefreshTokenURL, internal.AdminCleanroomGetURL:
			s.requireWriteCleanroomHandler(w, cleanroom)

		case internal.AdminCleanroomAdvanceURL:
			// advance the state of the participants
			cleanroom.Participants[0].State, stateExists = nextState[cleanroom.Participants[0].State]
			if !stateExists {
				s.T().Errorf("Unexpected state")
			}
			cleanroom.Participants[1].State, stateExists = nextState[cleanroom.Participants[1].State]
			if !stateExists {
				s.T().Errorf("Unexpected state")
			}

			if cleanroom.Participants[0].State == v1.Cleanroom_Participant_DATA_TRANSFORMED {
				s.requireGenAdvertiserTripleEncryptedData() // publisher writes to advertiser triple encrypted folder
			}

			s.requireWriteCleanroomHandler(w, cleanroom)

		default:
			s.T().Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	runCommand := RunCmd{
		PairCleanroomToken: s.requireGenerateToken(server.URL, s.params.cleanroomName, s.params.salt),
		Input:              s.params.advertiserInputFilePath,
		NumThreads:         workersNum,
		Output:             s.params.advertiserOutputFolderPath,
		PublisherPAIRIDs:   s.params.publisherPAIRIDsFolderPath,
	}

	cli := Cli{
		CleanroomCmd: CleanroomCmd{
			Run: runCommand,
		},
		Context: keyContext,
	}

	cfg := &Config{
		configPath: s.params.advertiserKeyConfigFilePath,
	}

	cmdCtx, err := cli.NewContext(cfg)
	s.Require().NoError(err)

	err = runCommand.Run(cmdCtx)
	s.Require().NoError(err)

	// check the result
	s.requireLocalContentEqualToGCSContent(s.params.advertiserOutputFolderPath, s.publisherTwiceEncryptedFolder())
	s.requireLocalContentEqualToGCSContent(s.params.publisherPAIRIDsFolderPath, s.publisherTripleEncryptedFolder())
}

// creates new cleanroom with the given name and expire time
func (s *cmdTestSuite) newCleanroom(publisherState, advertiserState v1.Cleanroom_Participant_State) *v1.Cleanroom {
	s.T().Helper()

	return &v1.Cleanroom{
		Name:       s.params.cleanroomName,
		ExpireTime: timestamppb.New(s.params.expireTime),
		Config: &v1.Cleanroom_Config{
			Config: &v1.Cleanroom_Config_Pair{
				Pair: &v1.Cleanroom_Config_PairConfig{
					GcsToken: &v1.Cleanroom_Config_PairConfig_AuthToken{
						Value:      "gcsToken",
						ExpireTime: timestamppb.New(s.params.expireTime),
					},
					AdvertiserTwiceEncryptedDataUrl:  s.advertiserTwiceEncryptedGCSFolder(),
					PublisherTwiceEncryptedDataUrl:   s.publisherTwiceEncryptedGCSFolder(),
					AdvertiserTripleEncryptedDataUrl: s.advertiserTripleEncryptedGCSFolder(),
					PublisherTripleEncryptedDataUrl:  s.publisherTripleEncryptedGCSFolder(),
				},
			},
		},
		Participants: []*v1.Cleanroom_Participant{
			{
				Role:  v1.Cleanroom_Participant_PUBLISHER,
				State: publisherState,
			},
			{
				Role:  v1.Cleanroom_Participant_ADVERTISER,
				State: advertiserState,
			},
		},
	}
}

func (s *cmdTestSuite) requireCreateNewKeyConfig(keyConfig *keys.KeyConfig) string {
	s.T().Helper()

	tmpConfigFile, err := os.Create(path.Join(s.tmpDir, "test_config.json"))
	s.Require().NoError(err, "must create temp file")

	defer func() {
		err = tmpConfigFile.Close()
		s.Require().NoError(err, "must close temp file")
	}()

	configs := map[string]keys.KeyConfig{
		keyContext: *keyConfig,
	}

	keyData, err := json.Marshal(&configs)
	s.Require().NoError(err, "must marshal key config")

	_, err = tmpConfigFile.Write(keyData)
	s.Require().NoError(err, "must create temp file")

	return tmpConfigFile.Name()
}

func (s *cmdTestSuite) requireCreateAdvertiserInputFile() string {
	s.T().Helper()

	tmpInputFile, err := os.Create(path.Join(s.tmpDir, "input.csv"))
	s.Require().NoError(err, "must create temp file")

	defer func() {
		err = tmpInputFile.Close()
		s.Require().NoError(err, "must close temp file")
	}()

	w := csv.NewWriter(tmpInputFile)
	defer func() {
		err := w.Error()
		s.Require().NoError(err, "must flush writer")
		w.Flush()
	}()

	for _, email := range s.params.emailsSource {
		err = w.Write([]string{email})
		s.Require().NoError(err, "must write email")
	}
	return tmpInputFile.Name()
}

func (s *cmdTestSuite) requireGenPublisherTwiceEncryptedData() {
	s.T().Helper()

	twiceEncryptedWriter := s.gcsClient.Bucket(s.sampleBucket).Object(
		s.publisherTwiceEncryptedDataFile(),
	).NewWriter(s.ctx)
	defer func() {
		err := twiceEncryptedWriter.Close()
		s.Require().NoError(err, "must close GCS writer")
	}()

	twiceEncryptedCsvWriter := csv.NewWriter(twiceEncryptedWriter)
	defer func() {
		err := twiceEncryptedCsvWriter.Error()
		s.Require().NoError(err, "must flush writer")
		twiceEncryptedCsvWriter.Flush()
	}()

	for _, email := range s.params.emailsSource {
		twiceEnc, err := s.params.publisherPairKey.Encrypt([]byte(email))
		s.Require().NoError(err, "must encrypt email")
		err = twiceEncryptedCsvWriter.Write([]string{string(twiceEnc)})
		s.Require().NoError(err, "must write email")
	}
}

func (s *cmdTestSuite) requireGenAdvertiserTripleEncryptedData() {
	s.T().Helper()

	readClosers, err := obucket.ReadersFromPrefixedBucket(s.ctx, s.gcsClient,
		&obucket.PrefixedBucket{
			Bucket: s.sampleBucket,
			Prefix: s.advertiserTwiceEncryptedFolder(),
		},
	)
	s.Require().NoError(err, "must create readers")
	s.Require().NotEmpty(readClosers, "must have twice encrypted data")

	readers := make([]io.Reader, len(readClosers))
	for i, r := range readClosers {
		defer func(r io.ReadCloser) {
			err := r.Close()
			s.Require().NoError(err, "must close GCS reader")
		}(r)
		readers[i] = r
	}
	twiceEncryptedCsvReader := csv.NewReader(io.MultiReader(readers...))

	tripleEncryptedWriter := s.gcsClient.Bucket(s.sampleBucket).Object(s.advertiserTripleEncryptedFile()).NewWriter(s.ctx)
	defer func() {
		err := tripleEncryptedWriter.Close()
		s.Require().NoError(err, "must close GCS writer")
	}()
	tripleEncryptedCsvWriter := csv.NewWriter(tripleEncryptedWriter)
	defer func() {
		err := tripleEncryptedCsvWriter.Error()
		s.Require().NoError(err, "must flush writer")
		tripleEncryptedCsvWriter.Flush()
	}()

	data, err := twiceEncryptedCsvReader.ReadAll()
	s.Require().NoError(err, "must read twice encrypted data")
	for _, line := range data {
		s.Require().Len(line, 1, "must have one record")
		record := line[0]

		tripleEnc, err := s.params.publisherPairKey.ReEncrypt([]byte(record))
		s.Require().NoError(err, "must re-encrypt record")
		err = tripleEncryptedCsvWriter.Write([]string{string(tripleEnc)})
		s.Require().NoError(err, "must write email")
	}
}

func (s *cmdTestSuite) requireLocalContentEqualToGCSContent(localFolder, gcsFolder string) {
	s.T().Helper()

	var localFileReadClosers []io.ReadCloser
	err := filepath.Walk(localFolder, func(path string, info os.FileInfo, err error) error {
		s.Require().NoError(err, "must walk through the local path")
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		s.Require().NoError(err, "must open output file")
		localFileReadClosers = append(localFileReadClosers, file)
		return nil
	})
	s.Require().NoError(err, "must walk through the local path")
	s.Require().NotEmpty(localFileReadClosers, "must have output files")

	localFileReaders := make([]io.Reader, len(localFileReadClosers))
	for i, r := range localFileReadClosers {
		defer func(r io.ReadCloser) {
			err := r.Close()
			s.Require().NoError(err, "must close local file reader")
		}(r)
		localFileReaders[i] = r
	}

	localFileCsvReader := csv.NewReader(io.MultiReader(localFileReaders...))
	localRecords, err := localFileCsvReader.ReadAll()
	s.Require().NoError(err, "must read output records")

	localValuesMap := make(map[string]struct{})
	for _, record := range localRecords {
		s.Require().Len(record, 1, "must have one record")
		localValuesMap[record[0]] = struct{}{}
	}

	readClosers, err := obucket.ReadersFromPrefixedBucket(s.ctx, s.gcsClient, &obucket.PrefixedBucket{
		Bucket: s.sampleBucket,
		Prefix: gcsFolder,
	})
	s.Require().NoError(err, "must create readers")
	s.Require().NotEmpty(readClosers, "must have twice encrypted data")

	readers := make([]io.Reader, len(readClosers))
	for i, r := range readClosers {
		defer func(r io.ReadCloser) {
			err := r.Close()
			s.Require().NoError(err, "must close GCS reader")
		}(r)
		readers[i] = r
	}
	gcsFolderCsvReader := csv.NewReader(io.MultiReader(readers...))
	gcsRecords, err := gcsFolderCsvReader.ReadAll()
	s.Require().NoError(err, "must read GCS records")

	gcsMap := make(map[string]struct{})
	for _, record := range gcsRecords {
		s.Require().Len(record, 1, "must have one record")
		gcsMap[record[0]] = struct{}{}
	}

	// check if all records in the output file are in the GCS folder
	for _, records := range localRecords {
		_, ok := gcsMap[records[0]]
		s.Require().True(ok, "record must be in GCS folder")
		delete(gcsMap, records[0])
	}
	s.Require().Empty(gcsMap, "all records in the local file must be in the GCS folder")

	// check if all records in the GCS folder are in the local file
	for _, records := range gcsRecords {
		_, ok := localValuesMap[records[0]]
		s.Require().True(ok, "record must be in the local file")
		delete(localValuesMap, records[0])
	}
	s.Require().Empty(localValuesMap, "all records in the GCS folder must be in the local file")
}

func (s *cmdTestSuite) requireGenerateToken(url, cleanroomName, salt string) string {
	token, err := generateToken(url, cleanroomName, salt)
	s.Require().NoError(err)
	return token
}

func (s *cmdTestSuite) advertiserTwiceEncryptedFolder() string {
	return fmt.Sprintf("%s/advertiser_twice_encrypted", s.params.cleanroomName)
}

func (s *cmdTestSuite) publisherTwiceEncryptedFolder() string {
	return fmt.Sprintf("%s/publisher_twice_encrypted", s.params.cleanroomName)
}

func (s *cmdTestSuite) advertiserTripleEncryptedFolder() string {
	return fmt.Sprintf("%s/advertiser_triple_encrypted", s.params.cleanroomName)
}

func (s *cmdTestSuite) publisherTripleEncryptedFolder() string {
	return fmt.Sprintf("%s/publisher_triple_encrypted", s.params.cleanroomName)
}

func (s *cmdTestSuite) advertiserTwiceEncryptedGCSFolder() string {
	return fmt.Sprintf("gs://%s/%s", s.sampleBucket, s.advertiserTwiceEncryptedFolder())
}

func (s *cmdTestSuite) publisherTwiceEncryptedGCSFolder() string {
	return fmt.Sprintf("gs://%s/%s", s.sampleBucket, s.publisherTwiceEncryptedFolder())
}

func (s *cmdTestSuite) advertiserTripleEncryptedGCSFolder() string {
	return fmt.Sprintf("gs://%s/%s", s.sampleBucket, s.advertiserTripleEncryptedFolder())
}

func (s *cmdTestSuite) publisherTripleEncryptedGCSFolder() string {
	return fmt.Sprintf("gs://%s/%s", s.sampleBucket, s.publisherTripleEncryptedFolder())
}

func (s *cmdTestSuite) publisherTwiceEncryptedDataFile() string {
	return s.publisherTwiceEncryptedFolder() + "/data.csv"
}

func (s *cmdTestSuite) advertiserTripleEncryptedFile() string {
	return s.advertiserTripleEncryptedFolder() + "/data.csv"
}
