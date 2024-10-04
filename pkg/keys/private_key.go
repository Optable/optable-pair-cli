package keys

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/gtank/ristretto255"
	"github.com/optable/match/pkg/pair"
)

const (
	ristretto255RandomByteSize = 64
)

func NewPrivateKey(mode pair.PAIRMode) (string, error) {
	var (
		key []byte
		err error
	)

	switch mode {
	case pair.PAIRSHA256Ristretto255:
		key, err = newRistretto255Key()
		if err != nil {
			return "", fmt.Errorf("newRistretto255: %w", err)
		}
	default:
	}

	return base64.StdEncoding.EncodeToString(key), nil
}

func privateKeyFromString(mode pair.PAIRMode, key string) ([]byte, error) {
	switch mode {
	case pair.PAIRSHA256Ristretto255:
		return ristretto255KeyFromString(key)
	default:
	}

	return nil, fmt.Errorf("unsupported mode: %s", mode)
}

func newRistretto255Key() ([]byte, error) {
	randSrc := make([]byte, ristretto255RandomByteSize)

	// note that we don't need to check the number of bytes read
	// since crypto/rand.Read will only returns n == len(b) iff err == nil.
	_, err := rand.Read(randSrc)
	if err != nil {
		return nil, fmt.Errorf("rand.Read: %w", err)
	}

	key := ristretto255.NewScalar().FromUniformBytes(randSrc)

	// marshal
	return key.MarshalText()
}

func ristretto255KeyFromString(key string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("base64.StdEncoding.DecodeString: %w", err)
	}

	// make sure the key is valid
	s := ristretto255.NewScalar()
	if err := s.UnmarshalText(b); err != nil {
		return nil, fmt.Errorf("ristretto255.UnmarshalText: %w", err)
	}

	return s.MarshalText()
}
