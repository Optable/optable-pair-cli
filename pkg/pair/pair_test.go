package pair

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"optable-pair-cli/pkg/keys"
	"testing"

	"github.com/optable/match/pkg/pair"
	"github.com/stretchr/testify/require"
)

func TestPAIRIDReadWriter_HashEncrypt(t *testing.T) {
	t.Parallel()
	// arrange
	lenEmails := 1001
	ctx := context.Background()
	salt := requireGenSalt(t)
	key := requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, lenEmails)
	expected := requireEncryptEmails(t, emails, salt, key)
	r, w := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	// set emails in csv format for PAIRIDReadWriter to read
	requireWriteEmails(t, r, emails)

	// act
	rw, err := NewPAIRIDReadWriter(r, w)
	require.NoError(t, err, "must create PAIRIDReadWriter")

	err = rw.HashEncrypt(ctx, 1, salt, key)
	require.NoError(t, err, "must hash and encrypt emails")

	// assert
	csvData := csv.NewReader(w)
	hashEncryptedData, err := csvData.ReadAll()
	require.NoError(t, err, "must read csv data")
	require.Len(t, hashEncryptedData, len(expected), "must contain all emails")

	for i, hashEncrypted := range hashEncryptedData {
		require.Len(t, hashEncrypted, 1, "must contain one csv column")
		require.Equal(t, expected[i], hashEncrypted[0], "encrypted email must match")
	}
}

func TestPAIRIDReadWriter_ReEncrypt(t *testing.T) {
	t.Parallel()
	// arrange
	lenEmails := 10000
	ctx := context.Background()
	salt := requireGenSalt(t)
	key := requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, lenEmails)
	encryptedEmails := requireEncryptEmails(t, emails, salt, key)
	twiceEncryptedEmails := requireReEncryptEmails(t, encryptedEmails, salt, key)
	r, w := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	// set twice encrypted emails in csv format for PAIRIDReadWriter to read
	requireWriteEmails(t, r, encryptedEmails)

	// in this test we check encrypted emails are encrypted correctly and shuffled
	expected := twiceEncryptedEmails

	// act
	rw, err := NewPAIRIDReadWriter(r, w)
	require.NoError(t, err, "must create PAIRIDReadWriter")

	err = rw.ReEncrypt(ctx, 1, salt, key)
	require.NoError(t, err, "must re-encrypt emails")

	// assert
	csvData := csv.NewReader(w)
	reEncryptedData, err := csvData.ReadAll()
	require.NoError(t, err, "must read csv data")
	require.Len(t, reEncryptedData, len(expected), "must contain all emails")

	notShuffled := 0
	for i, reEncrypted := range reEncryptedData {
		require.Len(t, reEncrypted, 1, "must contain one csv column")

		// check how many emails stay at the same place
		if reEncrypted[0] == expected[i] {
			notShuffled++
		}

		// must find the encrypted email in the expected slice
		found := false
		for _, e := range expected {
			if e == reEncrypted[0] {
				found = true
				break
			}
		}
		require.True(t, found, "re-encrypted email must match")
	}

	require.Less(t, float64(notShuffled), float64(lenEmails)*0.01, "must shuffle more than 99% of emails")
}

func TestPAIRIDReadWriter_HashDecrypt(t *testing.T) {
	t.Parallel()
	// arrange
	lenEmails := 1001
	ctx := context.Background()
	salt := requireGenSalt(t)
	key := requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, lenEmails)
	encryptedEmails := requireEncryptEmails(t, emails, salt, key)
	twiceEncryptedEmails := requireReEncryptEmails(t, encryptedEmails, salt, key)
	r, w := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	// set twice encrypted emails in csv format for PAIRIDReadWriter to read
	requireWriteEmails(t, r, twiceEncryptedEmails)

	// in this test we check twice encrypted emails are decrypted correctly, i.e.
	// decrypt(encrypt(encrypt(data))) = encrypt(data)
	expected := encryptedEmails

	// act
	rw, err := NewPAIRIDReadWriter(r, w)
	require.NoError(t, err, "must create PAIRIDReadWriter")

	err = rw.Decrypt(ctx, 1, salt, key)
	require.NoError(t, err, "must decrypt emails")

	// assert
	csvData := csv.NewReader(w)
	decryptedData, err := csvData.ReadAll()
	require.NoError(t, err, "must read csv data")
	require.Len(t, decryptedData, len(expected), "must contain all emails")

	for i, decrypted := range decryptedData {
		require.Len(t, decrypted, 1, "must contain one csv column")
		require.Equal(t, expected[i], decrypted[0], "encrypted email must match")
	}
}

func TestPAIRIDReadWriter_InputBelowThreshold(t *testing.T) {
	t.Parallel()
	// arrange
	lenEmails := 999
	ctx := context.Background()
	salt := requireGenSalt(t)
	key := requireGenKey(t)
	emails := requireGenRandomHashedEmails(t, lenEmails)
	encryptedEmails := requireEncryptEmails(t, emails, salt, key)
	twiceEncryptedEmails := requireReEncryptEmails(t, encryptedEmails, salt, key)

	t.Run("HashEncrypt", func(t *testing.T) {
		t.Parallel()
		r, w := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

		// set emails in csv format for PAIRIDReadWriter to read
		requireWriteEmails(t, r, emails)

		rw, err := NewPAIRIDReadWriter(r, w)
		require.NoError(t, err, "must create PAIRIDReadWriter")

		err = rw.HashEncrypt(ctx, 1, salt, key)
		require.Error(t, err, "must return error when input is below threshold")
		require.Equal(t, ErrInputBelowThreshold, err)
	})

	t.Run("ReEncrypt", func(t *testing.T) {
		t.Parallel()
		r, w := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

		// set encrypted emails in csv format for PAIRIDReadWriter to read
		requireWriteEmails(t, r, encryptedEmails)

		rw, err := NewPAIRIDReadWriter(r, w)
		require.NoError(t, err, "must create PAIRIDReadWriter")

		err = rw.ReEncrypt(ctx, 1, salt, key)
		require.Error(t, err, "must return error when input is below threshold")
		require.Equal(t, ErrInputBelowThreshold, err)
	})

	t.Run("Decrypt", func(t *testing.T) {
		t.Parallel()
		r, w := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

		// set twice encrypted emails in csv format for PAIRIDReadWriter to read
		requireWriteEmails(t, r, twiceEncryptedEmails)

		rw, err := NewPAIRIDReadWriter(r, w)
		require.NoError(t, err, "must create PAIRIDReadWriter")

		err = rw.Decrypt(ctx, 1, salt, key)
		require.Error(t, err, "must return error when input is below threshold")
		require.Equal(t, ErrInputBelowThreshold, err)
	})
}

func requireGenRandomHashedEmails(t *testing.T, emailsCount int) []string {
	t.Helper()
	shaEncoder := sha256.New()
	hems := make([]string, emailsCount)
	for i := range hems {
		shaEncoder.Write([]byte(fmt.Sprintf("%d@gmail.com", i)))
		hem := shaEncoder.Sum(nil)
		hems[i] = fmt.Sprintf("%x", hem)
	}
	return hems
}

func requireWriteEmails(t *testing.T, w io.Writer, emails []string) {
	csvWriter := csv.NewWriter(w)
	for _, email := range emails {
		err := csvWriter.Write([]string{email})
		require.NoError(t, err)
	}
	csvWriter.Flush()
}

func requireEncryptEmails(t *testing.T, emails []string, salt, key string) []string {
	t.Helper()
	pk, err := keys.NewPAIRPrivateKey(salt, key)
	require.NoError(t, err)

	encryptedEmails := make([]string, len(emails))
	for i, email := range emails {
		encrypted, err := pk.Encrypt([]byte(email))
		require.NoError(t, err)
		encryptedEmails[i] = string(encrypted)
	}
	return encryptedEmails
}

func requireReEncryptEmails(t *testing.T, emails []string, salt, key string) []string {
	t.Helper()
	pk, err := keys.NewPAIRPrivateKey(salt, key)
	require.NoError(t, err)

	encryptedEmails := make([]string, len(emails))
	for i, email := range emails {
		encrypted, err := pk.ReEncrypt([]byte(email))
		require.NoError(t, err)
		encryptedEmails[i] = string(encrypted)
	}
	return encryptedEmails
}

func requireGenSalt(t *testing.T) string {
	t.Helper()
	salt := make([]byte, SHA256SaltSize)
	_, err := rand.Read(salt)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(salt)
}

func requireGenKey(t *testing.T) string {
	t.Helper()
	key, err := keys.NewPrivateKey(pair.PAIRSHA256Ristretto255)
	require.NoError(t, err)
	return key
}
