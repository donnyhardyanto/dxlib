package sql

import (
	"crypto/sha512"
	"encoding/hex"
	"golang.org/x/crypto/bcrypt"
	"strings"
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
	if strings.ContainsAny(s, " ')-#/*!;+|") {
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

func HashPassword(password string) []byte {
	hashed := sha512.Sum512([]byte(password))
	return hashed[:]
}

func HashPasswordToHexString(password string) string {
	hashed := HashPassword(password)
	return hex.EncodeToString(hashed)
}

func HashSHA512(data []byte) []byte {
	hashed := sha512.Sum512(data)
	return hashed[:]
}

func HashBcrypt(data []byte) ([]byte, error) {
	hashed, err := bcrypt.GenerateFromPassword(data, bcrypt.MaxCost)
	return hashed, err
}
