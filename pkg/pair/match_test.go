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

func TestMatch(t *testing.T) {
	t.Parallel()

	// arrange
	lenEmails := 1001
	ctx := context.Background()
	salt := requireGenSalt(t)
	key1, key2 := requireGenKey(t), requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, lenEmails)
	encryptedEmails1 := requireEncryptEmails(t, emails[:900], salt, key1)
	encryptedEmails2 := requireEncryptEmails(t, emails[100:], salt, key2)
	twiceEncryptedEmails1 := requireReEncryptEmails(t, encryptedEmails1, salt, key2)
	twiceEncryptedEmails2 := requireReEncryptEmails(t, encryptedEmails2, salt, key1)
	r1, r2 := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	requireWriteEmails(t, r1, twiceEncryptedEmails1)
	requireWriteEmails(t, r2, twiceEncryptedEmails2)

	// create map to access encrypted emails faster
	expectContain := make(map[string]struct{}, 900)
	for _, email := range encryptedEmails2[:800] {
		expectContain[email] = struct{}{}
	}

	dir, err := os.MkdirTemp("", "match_test")
	require.NoError(t, err, "must create temp dir")
	defer func() {
		err = os.RemoveAll(dir)
		require.NoError(t, err, "must remove temp dir")
	}()

	// act
	matcher, err := NewMatcher([]io.Reader{r1}, []io.Reader{r2}, dir)
	require.NoError(t, err, "must create Matcher")

	err = matcher.Match(ctx, 1, salt, key1)
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
	require.Len(t, records, 800, "must contain 800 emails")

	for _, line := range records {
		require.Len(t, line, 1, "must contain 1 element")
		_, exists := expectContain[line[0]]
		require.True(t, exists, "must exist in the hash-encrypted list")
	}
}

func TestMatch_MultipleWorkers(t *testing.T) {
	t.Parallel()

	// arrange
	lenEmails := 1100
	ctx := context.Background()
	salt := requireGenSalt(t)
	key1, key2 := requireGenKey(t), requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, lenEmails)
	encryptedEmails1 := requireEncryptEmails(t, emails[:1000], salt, key1)
	encryptedEmails2 := requireEncryptEmails(t, emails[100:], salt, key2)
	twiceEncryptedEmails1 := requireReEncryptEmails(t, encryptedEmails1, salt, key2)
	twiceEncryptedEmails2 := requireReEncryptEmails(t, encryptedEmails2, salt, key1)
	rA1, rA2, rA3, rA4 := bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil)
	rP1, rP2, rP3, rP4 := bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	requireWriteEmails(t, rA1, twiceEncryptedEmails1[:250])
	requireWriteEmails(t, rA2, twiceEncryptedEmails1[250:500])
	requireWriteEmails(t, rA3, twiceEncryptedEmails1[500:750])
	requireWriteEmails(t, rA4, twiceEncryptedEmails1[750:])

	requireWriteEmails(t, rP1, twiceEncryptedEmails2[:250])
	requireWriteEmails(t, rP2, twiceEncryptedEmails2[250:500])
	requireWriteEmails(t, rP3, twiceEncryptedEmails2[500:750])
	requireWriteEmails(t, rP4, twiceEncryptedEmails2[750:])

	// create map to access encrypted emails faster
	expectContain := make(map[string]struct{}, 1000)
	for _, email := range encryptedEmails2 {
		expectContain[email] = struct{}{}
	}

	// act
	dir, err := os.MkdirTemp("", "match_test")
	require.NoError(t, err, "must create temp dir")
	matcher, err := NewMatcher([]io.Reader{rA1, rA2, rA3, rA4}, []io.Reader{rP1, rP2, rP3, rP4}, dir)
	require.NoError(t, err, "must create Matcher")

	err = matcher.Match(ctx, 4, salt, key1)
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

		// check if the records exiet in the hash-encrypted list
		for _, line := range records {
			require.Len(t, line, 1, "must contain 1 element")
			_, exists := expectContain[line[0]]
			require.True(t, exists, "must exist in the hash-encrypted list")
		}

		// count the matched emails
		matchRate += len(records)
	}

	require.Equal(t, 900, matchRate, "must match 900 emails")
}
