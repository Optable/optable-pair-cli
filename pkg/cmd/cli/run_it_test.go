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
	// generate emails
	s.params.emailsSource = make([]string, genEmailsSourceNumber)
	shaEncoder := sha256.New()
	for i := range genEmailsSourceNumber {
		shaEncoder.Write([]byte(fmt.Sprintf("%d@gmail.com", i)))
		hem := shaEncoder.Sum(nil)
		s.params.emailsSource[i] = fmt.Sprintf("%x", hem)
	}

	id := uuid.New().String()
	s.params.cleanroomName = "cleanrooms/" + id
	s.params.expireTime = time.Now().Add(1 * time.Hour)

	s.tmpDir = path.Join(os.TempDir(), id)
	err := os.MkdirAll(s.tmpDir, os.ModePerm)
	s.Require().NoError(err, "must create temp dir")

	s.params.publisherPAIRIDsFolderPath = path.Join(s.tmpDir, "publisher_pair_id")
	s.params.advertiserOutputFolderPath = path.Join(s.tmpDir, "output")

	// create advertiser key config file in tmp folder
	s.params.advertiserKeyConfigFilePath = s.requireCreateNewKeyConfig()
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
	s.testRun(1)
}

func (s *cmdTestSuite) TestRun_MultipleWorkers() {
	s.testRun(4)
}

func (s *cmdTestSuite) TestRun_FailToAdvance() {
	// init optable mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internal.AdminCleanroomRefreshTokenURL, internal.AdminCleanroomGetURL:
			cleanroom := s.newCleanroom()
			data, err := proto.Marshal(&cleanroom)
			if err != nil {
				s.T().Errorf("Failed to marshal response: %v", err)
			}
			_, err = w.Write(data)
			if err != nil {
				s.T().Errorf("Failed to write response body: %v", err)
			}
			w.WriteHeader(http.StatusOK)

		case internal.AdminCleanroomAdvanceURL:
			w.WriteHeader(http.StatusInternalServerError)

		default:
			s.T().Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	token, err := generateToken(server.URL, s.params.cleanroomName, s.params.salt)
	s.Require().NoError(err)

	runCommand := RunCmd{
		PairCleanroomToken: token,
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

func (s *cmdTestSuite) testRun(workersNum int) {
	var (
		cleanroom = s.newCleanroom()
		// next states for the participants to be changed on each advance call
		nextPuplisherState  = v1.Cleanroom_Participant_DATA_TRANSFORMED
		nextAdvertiserState = v1.Cleanroom_Participant_DATA_CONTRIBUTED
	)

	// init optable mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCleanroom := func(w http.ResponseWriter) {
			data, err := proto.Marshal(&cleanroom)
			if err != nil {
				s.T().Errorf("Failed to marshal response: %v", err)
			}
			_, err = w.Write(data)
			if err != nil {
				s.T().Errorf("Failed to write response body: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		}
		switch r.URL.Path {
		case internal.AdminCleanroomRefreshTokenURL, internal.AdminCleanroomGetURL:
			writeCleanroom(w)

		case internal.AdminCleanroomAdvanceURL:
			// advance the state of the participants
			cleanroom.Participants[0].State = nextPuplisherState
			cleanroom.Participants[1].State = nextAdvertiserState

			if cleanroom.Participants[0].State == v1.Cleanroom_Participant_DATA_TRANSFORMED {
				// add publisher triple encrypted data on this step
				s.requireGenAdvertiserTripleEncryptedData()
			}

			nextPuplisherState = v1.Cleanroom_Participant_SUCCEEDED
			nextAdvertiserState = v1.Cleanroom_Participant_DATA_TRANSFORMED

			writeCleanroom(w)

		default:
			s.T().Errorf("Unexpected call %s", r.URL.Path)
		}
	}))
	defer server.Close()

	token, err := generateToken(server.URL, s.params.cleanroomName, s.params.salt)
	s.Require().NoError(err)

	runCommand := RunCmd{
		PairCleanroomToken: token,
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

// creates new cleanroom with the given name and expire time. The cleanroom has two participants:
// - publisher with DATA_CONTRIBUTED state;
// - advertiser with INVITED state.
func (s *cmdTestSuite) newCleanroom() v1.Cleanroom {
	s.T().Helper()

	return v1.Cleanroom{
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
				State: v1.Cleanroom_Participant_DATA_CONTRIBUTED,
			},
			{
				Role:  v1.Cleanroom_Participant_ADVERTISER,
				State: v1.Cleanroom_Participant_INVITED,
			},
		},
	}
}

func (s *cmdTestSuite) requireCreateNewKeyConfig() string {
	s.T().Helper()

	tmpConfigFile, err := os.Create(path.Join(s.tmpDir, "test_config.json"))
	s.Require().NoError(err, "must create temp file")

	defer func() {
		err = tmpConfigFile.Close()
		s.Require().NoError(err, "must close temp file")
	}()

	keyConfig, err := keys.GenerateKeyConfig()
	s.Require().NoError(err, "must generate key config")

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
	defer w.Flush()

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
	defer twiceEncryptedCsvWriter.Flush()

	for _, email := range s.params.emailsSource {
		twiceEnc, err := s.params.publisherPairKey.Encrypt([]byte(email))
		s.Require().NoError(err, "must encrypt email")
		err = twiceEncryptedCsvWriter.Write([]string{string(twiceEnc)})
		s.Require().NoError(err, "must write email")
	}
}

func (s *cmdTestSuite) requireGenAdvertiserTripleEncryptedData() {
	s.T().Helper()
	ctx := context.Background()

	readClosers, err := obucket.ReadersFromPrefixedBucket(ctx, s.gcsClient,
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

	tripleEncryptedWriter := s.gcsClient.Bucket(s.sampleBucket).Object(s.advertiserTripleEncryptedFile()).NewWriter(ctx)
	defer func() {
		err := tripleEncryptedWriter.Close()
		s.Require().NoError(err, "must close GCS writer")
	}()
	tripleEncryptedCsvWriter := csv.NewWriter(tripleEncryptedWriter)
	defer tripleEncryptedCsvWriter.Flush()

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
	ctx := context.Background()

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

	readClosers, err := obucket.ReadersFromPrefixedBucket(ctx, s.gcsClient, &obucket.PrefixedBucket{
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
