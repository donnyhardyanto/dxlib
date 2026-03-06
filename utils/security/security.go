package security

import (
	"crypto/sha512"
	"crypto/subtle"
	"strings"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

func IsDDL(statement string) bool {
	upperStatement := strings.ToUpper(statement)
	keywords := []string{"CREATE", "DROP", "ALTER", "TRUNCATE", "COMMENT", "RENAME"}
	for _, v := range keywords {
		if strings.Contains(upperStatement, v) {
			return true
		}
	}
	return false
}

func StringCheckPossibleSQLInjection(s string) bool {
	// Single characters that are dangerous on their own
	if strings.ContainsAny(s, "';#") {
		return true
	}
	// Multi-character SQL injection patterns
	if strings.Contains(s, "--") ||
		strings.Contains(s, "/*") ||
		strings.Contains(s, "*/") ||
		strings.Contains(s, "||") {
		return true
	}
	return false
}

func PartSQLStringCheckPossibleSQLInjection(s string) bool {
	if strings.ContainsAny(s, "#;") {
		return true
	}
	s = strings.ToUpper(s)
	if strings.Contains(s, "INSERT") {
		return true
	}
	if strings.Contains(s, "UPDATE") {
		return true
	}
	if strings.Contains(s, "DROP") {
		return true
	}
	if strings.Contains(s, "DELETE") {
		return true
	}
	if strings.Contains(s, "EXEC") {
		return true
	}
	if strings.Contains(s, "DATABASE") {
		return true
	}
	if strings.Contains(s, "TABLE") {
		return true
	}
	if strings.Contains(s, "VIEW") {
		return true
	}
	if strings.Contains(s, "SELECT") {
		return true
	}
	if strings.Contains(s, "FROM") {
		return true
	}
	if strings.Contains(s, "WHERE") {
		return true
	}
	if strings.Contains(s, "INTO") {
		return true
	}
	if strings.Contains(s, "PROCEDURE") {
		return true
	}
	return false
}

func HashSHA512(data []byte) []byte {
	hashed := sha512.Sum512(data)
	return hashed[:]
}

func HashBcrypt(data []byte) ([]byte, error) {
	hashed, err := bcrypt.GenerateFromPassword(data, bcrypt.DefaultCost)
	return hashed, err
}

func HashBcryptVerify(hashedPassword, password []byte) error {
	return bcrypt.CompareHashAndPassword(hashedPassword, password)
}

func HashArgon2id(password, salt []byte) []byte {
	return argon2.IDKey(password, salt, 1, 64*1024, 4, 32)
}

func HashArgon2idVerify(password, salt, expectedHash []byte) bool {
	computedHash := HashArgon2id(password, salt)
	return subtle.ConstantTimeCompare(computedHash, expectedHash) == 1
}
