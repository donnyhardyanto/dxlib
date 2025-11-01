package rand

import (
	"crypto/rand"
	"encoding/binary"
	"math/big"
)

// Int returns a cryptographically secure random integer in [0, max)
// This is a drop-in replacement for math/rand.Intn()
// Uses rejection sampling to avoid modulo bias
func Int(max int) int {
	if max <= 0 {
		return 0
	}

	// Use crypto/rand's built-in unbiased random integer generator
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		// crypto/rand failure is a serious error - panic is appropriate for security-critical code
		panic("crypto/rand failed: " + err.Error())
	}
	return int(n.Int64())
}

// Float64 returns a cryptographically secure random float in [0.0, 1.0)
// This is a drop-in replacement for math/rand.Float64()
func Float64() float64 {
	var n uint64
	err := binary.Read(rand.Reader, binary.BigEndian, &n)
	if err != nil {
		// crypto/rand failure is a serious error - panic is appropriate for security-critical code
		panic("crypto/rand failed: " + err.Error())
	}
	// Convert to float64 in range [0, 1)
	return float64(n) / float64(1<<64)
}

// RandomString returns a cryptographically secure random string of length n from the given charset
// If no charset is provided, it uses alphanumeric characters (a-zA-Z0-9)
func RandomString(n int, charset ...string) string {
	const defaultCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	letters := defaultCharset
	if len(charset) > 0 && charset[0] != "" {
		letters = charset[0]
	}

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[Int(len(letters))]
	}
	return string(b)
}