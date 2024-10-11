package internal

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

type CleanroomToken struct {
	Raw        string
	Cleanroom  string  `json:"cleanroom"`
	Expiration float64 `json:"exp"`
	IssuerHost string  `json:"iss"`
	HashSalt   string  `json:"salt"`
}

func (c *CleanroomToken) Valid() error {
	if c.Cleanroom == "" {
		return fmt.Errorf("cleanroom is empty")
	}

	if time.Now().Unix() > int64(c.Expiration) {
		return fmt.Errorf("token is expired")
	}

	if c.IssuerHost == "" {
		return fmt.Errorf("issuer host is empty")
	}

	if c.HashSalt == "" {
		return fmt.Errorf("hash salt is empty")
	}

	return nil
}

func ParseCleanroomToken(token string) (*CleanroomToken, error) {
	claims := &CleanroomToken{Raw: token}

	parser := jwt.Parser{
		ValidMethods: []string{jwt.SigningMethodES256.Alg()},
	}

	_, _, err := parser.ParseUnverified(token, claims)
	if err != nil {
		return nil, err
	}

	return claims, err
}
