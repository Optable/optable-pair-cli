package keys

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/optable/match/pkg/pair"

	// Register the SHA256 hash function
	_ "crypto/sha256"
)

const mode = pair.PAIRSHA256Ristretto255

type KeyConfig struct {
	// Unique identifier for the key.
	ID string `json:"id"`
	// base64 encoded key data
	Key string `json:"key"`
	// Key is created using which PAIR mode
	Mode string `json:"mode"`
	// timestamp of when the key was created
	// RFC3339 format
	// e.g. 2021-09-01T12:00:00Z
	CreatedAt string `json:"created_at"`
}

func GenerateKeyConfig() (*KeyConfig, error) {
	key, err := NewPrivateKey(mode)
	if err != nil {
		return nil, fmt.Errorf("NewPrivateKey: %w", err)
	}

	return &KeyConfig{
		ID:        uuid.New().String(),
		Key:       key,
		Mode:      strconv.Itoa(int(pair.PAIRSHA256Ristretto255)),
		CreatedAt: time.Now().Format(time.RFC3339),
	}, nil
}

func NewPAIRPrivateKey(hashSalt, privateKey string) (*pair.PrivateKey, error) {
	salt, err := hashSaltFromString(hashSalt)
	if err != nil {
		return nil, fmt.Errorf("hashSaltFromString: %w", err)
	}

	sk, err := privateKeyFromString(mode, privateKey)
	if err != nil {
		return nil, fmt.Errorf("PrivateKeyFromString: %w", err)
	}

	return mode.New(salt, sk)
}

func hashSaltFromString(s string) ([]byte, error) {
	salt, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}

	return salt, nil
}
