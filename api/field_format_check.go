package api

import (
	"regexp"
	"strings"
)

const FormatEmailMaxLength = 254
const FormatPhoneNumberMinimumLength = 3
const FormatPhoneNumberMaxLength = 25

func FormatEMailCheckValid(s string) bool {
	// Check total length

	if len(s) > FormatEmailMaxLength {
		return false
	}

	// Split email to validate local and domain parts separately
	parts := strings.Split(s, "@")
	if len(parts) != 2 {
		return false
	}

	// Check local part length
	if len(parts[0]) > 64 {
		return false
	}

	// Check domain part length
	if len(parts[1]) > 255 {
		return false
	}

	// Regex validation
	pattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	regex := regexp.MustCompile(pattern)
	return regex.MatchString(s)
}

func FormatPhoneNumberCheckValid(s string) bool {

	// Regex validation for phone number
	pattern := `^\+?[0-9\-\(\)]+$`
	regex := regexp.MustCompile(pattern)
	return regex.MatchString(s)
}
