package pair

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"optable-pair-cli/pkg/io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	commonStart  int = 100
	commonEnd    int = 1000
	commoLen     int = 900
	firstWorker  int = 250
	secondWorker int = 500
	thirdWorker  int = 750
	nEmails      int = 1100
)

func TestMatch(t *testing.T) {
	t.Parallel()

	// arrange
	ctx := context.Background()
	salt := requireGenSalt(t)
	publisherKey, advertiserKey := requireGenKey(t), requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, nEmails)
	publisherEncryptedEmails := requireEncryptEmails(t, emails[:commonEnd], salt, publisherKey)
	advertiserEncryptedEmails := requireEncryptEmails(t, emails[commonStart:], salt, advertiserKey)
	publisherTwiceEncryptedEmails := requireReEncryptEmails(t, publisherEncryptedEmails, salt, advertiserKey)
	advertiserTwiceEncryptedEmails := requireReEncryptEmails(t, advertiserEncryptedEmails, salt, publisherKey)
	advertiserReader, publisherReader := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	requireWriteEmails(t, publisherReader, publisherTwiceEncryptedEmails)
	requireWriteEmails(t, advertiserReader, advertiserTwiceEncryptedEmails)

	// create map to access encrypted emails faster
	expectContain := make(map[string]struct{}, commoLen)
	for _, email := range publisherEncryptedEmails[commonStart:] {
		expectContain[email] = struct{}{}
	}

	dir, err := os.MkdirTemp("", "match_test")
	require.NoError(t, err, "must create temp dir")
	defer func() {
		err = os.RemoveAll(dir)
		require.NoError(t, err, "must remove temp dir")
	}()

	// act
	matcher, err := NewMatcher([]io.Reader{advertiserReader}, []io.Reader{publisherReader}, dir)
	require.NoError(t, err, "must create Matcher")

	err = matcher.Match(ctx, 1, salt, advertiserKey)
	require.NoError(t, err, "must Match")

	// assert
	info, err := os.Stat(dir + "/result_0.csv")
	require.NoError(t, err, "must stat result file")
	require.NotZero(t, info.Size(), "must contain data")

	f, err := os.Open(dir + "/result_0.csv")
	require.NoError(t, err, "must open result file")

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	require.NoError(t, err, "must read csv data")
	require.Len(t, records, commoLen, "must contain 800 emails")

	for _, line := range records {
		require.Len(t, line, 1, "must contain 1 element")
		_, exists := expectContain[line[0]]
		require.True(t, exists, "must exist in the hash-encrypted list")
		delete(expectContain, line[0])
	}
}

func TestMatch_MultipleWorkers(t *testing.T) {
	t.Parallel()

	// arrange
	ctx := context.Background()
	salt := requireGenSalt(t)
	publisherKey, advertiserKey := requireGenKey(t), requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, nEmails)
	publisherEncryptedEmails := requireEncryptEmails(t, emails[:commonEnd], salt, publisherKey)
	advertiserEncryptedEmails := requireEncryptEmails(t, emails[commonStart:], salt, advertiserKey)
	publisherTwiceEncryptedEmails := requireReEncryptEmails(t, publisherEncryptedEmails, salt, advertiserKey)
	advertiserTwiceEncryptedEmails := requireReEncryptEmails(t, advertiserEncryptedEmails, salt, publisherKey)
	rA1, rA2, rA3, rA4 := bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil)
	rP1, rP2, rP3, rP4 := bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	requireWriteEmails(t, rA1, publisherTwiceEncryptedEmails[:firstWorker])
	requireWriteEmails(t, rA2, publisherTwiceEncryptedEmails[firstWorker:secondWorker])
	requireWriteEmails(t, rA3, publisherTwiceEncryptedEmails[secondWorker:thirdWorker])
	requireWriteEmails(t, rA4, publisherTwiceEncryptedEmails[thirdWorker:])

	requireWriteEmails(t, rP1, advertiserTwiceEncryptedEmails[:firstWorker])
	requireWriteEmails(t, rP2, advertiserTwiceEncryptedEmails[firstWorker:secondWorker])
	requireWriteEmails(t, rP3, advertiserTwiceEncryptedEmails[secondWorker:thirdWorker])
	requireWriteEmails(t, rP4, advertiserTwiceEncryptedEmails[thirdWorker:])

	// create map to access encrypted emails faster
	expectContain := make(map[string]struct{}, commoLen)
	for _, email := range publisherEncryptedEmails[commonStart:] {
		expectContain[email] = struct{}{}
	}

	// act
	dir, err := os.MkdirTemp("", "match_test")
	require.NoError(t, err, "must create temp dir")
	matcher, err := NewMatcher([]io.Reader{rA1, rA2, rA3, rA4}, []io.Reader{rP1, rP2, rP3, rP4}, dir)
	require.NoError(t, err, "must create Matcher")

	err = matcher.Match(ctx, 4, salt, advertiserKey)
	require.NoError(t, err, "must Match")

	// assert
	matchRate := 0
	for i := 0; i < 4; i++ {
		fileName := fmt.Sprintf("/result_%d.csv", i)
		info, err := os.Stat(dir + fileName)
		require.NoError(t, err, "must stat result file")
		require.NotZero(t, info.Size(), "must contain data")

		f, err := os.Open(dir + fileName)
		require.NoError(t, err, "must open result file")

		csvReader := csv.NewReader(f)
		records, err := csvReader.ReadAll()
		require.NoError(t, err, "must read csv data")

		// check if the records exist in the hash-encrypted list
		for _, line := range records {
			require.Len(t, line, 1, "must contain 1 element")
			_, exists := expectContain[line[0]]
			require.True(t, exists, "must exist in the hash-encrypted list")
			delete(expectContain, line[0])
		}

		// count the matched emails
		matchRate += len(records)
	}

	require.Equal(t, commoLen, matchRate, "must match 900 emails")
}
