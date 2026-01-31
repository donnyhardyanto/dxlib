package api

import (
	"github.com/donnyhardyanto/dxlib/errors"
	"regexp"
	"strconv"
	"strings"
)

const FormatEmailMaxLength = 254
const FormatPhoneNumberMinimumLength = 3
const FormatPhoneNumberMaxLength = 25

const (
	// NPWP (with separators) can be 20 or 22 characters long
	// 15 digits: XX.XXX.XXX.X-XXX.XX
	// 16 digits: XX.XXX.XXX.X-XXX.XXX
	FormatNPWPWithSeparatorsLengthShort = 20
	FormatNPWPWithSeparatorsLengthLong  = 22

	// NPWP without separators can be 15 or 16 digits long
	FormatNPWPDigitsLengthShort = 15
	FormatNPWPDigitsLengthLong  = 16
)

// NIK is 16 digits long
const FormatNIKLength = 16

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
	// Step 1: Check length constraints
	if len(s) < FormatPhoneNumberMinimumLength || len(s) > FormatPhoneNumberMaxLength {
		return false
	}

	// Step a: Remove all formatting characters to count actual digits
	digitsOnly := regexp.MustCompile("[^0-9]").ReplaceAllString(s, "")

	// Most international numbers need at least 7 digits and rarely exceed 15
	if len(digitsOnly) < FormatPhoneNumberMinimumLength || len(digitsOnly) > FormatPhoneNumberMaxLength {
		return false
	}

	// Step 2: Pattern for international format validation
	// Handles:
	// - Optional + at the beginning
	// - Country code (optional)
	// - Area codes (with or without parentheses)
	// - Separators (spaces, dots, hyphens)
	// - Extensions with 'x' or 'ext'
	// - Must have at least 7 digits total
	pattern := `^(?:(?:\+|00)[1-9]\d{0,3})?` + // Optional country code with + or 00
		`(?:` +
		`(?:\s|\.|-)?\(?\d{1,4}\)?(?:\s|\.|-)?\d{1,4}(?:\s|\.|-)?\d{1,4}` + // Standard formats
		`|` +
		`\d{7,15}` + // All digits together
		`)` +
		`(?:(?:\s|\.|-)?(?:x|ext|extension)\.?(?:\s|\.|-)?[0-9]{1,5})?$` // Optional extension

	regex := regexp.MustCompile(pattern)

	// Step 3: Special case handling for North American format (NPA-NXX-XXXX)
	if len(digitsOnly) == 10 || (len(digitsOnly) == 11 && digitsOnly[0] == '1') {
		// North American formats should follow specific area code rules
		// Area codes can't start with 0 or 1
		if len(digitsOnly) == 10 && (digitsOnly[0] == '0' || digitsOnly[0] == '1') {
			return false
		} else if len(digitsOnly) == 11 && digitsOnly[0] == '1' && (digitsOnly[1] == '0' || digitsOnly[1] == '1') {
			return false
		}
	}

	return regex.MatchString(s)
}

// Helper function to normalize phone numbers to E.164 format
func NormalizePhoneNumber(s string) (string, error) {
	if !FormatPhoneNumberCheckValid(s) {
		return "", errors.Errorf("invalid phone number format")
	}

	// Remove all non-digit characters except leading +
	normalized := ""
	if strings.HasPrefix(s, "+") {
		normalized = "+"
		s = s[1:]
	}

	// Extract digits only
	digits := regexp.MustCompile("[0-9]").FindAllString(s, -1)
	normalized += strings.Join(digits, "")

	// Handle extensions
	parts := regexp.MustCompile("(?i)(?:x|ext|extension)").Split(s, 2)
	if len(parts) > 1 {
		extDigits := regexp.MustCompile("[0-9]").FindAllString(parts[1], -1)
		if len(extDigits) > 0 {
			normalized += "x" + strings.Join(extDigits, "")
		}
	}

	return normalized, nil
}

// FormatNPWPorNIKCheckValid validates if a string is a valid NPWP or NIK
func FormatNPWPorNIKCheckValid(s string) bool {
	if s == "" {
		return false
	}

	// Remove all non-digit characters to check digit count
	digitsOnly := regexp.MustCompile("[^0-9]").ReplaceAllString(s, "")

	// Check if it's an NPWP (15 or 16 digits)
	if len(digitsOnly) == FormatNPWPDigitsLengthShort || len(digitsOnly) == FormatNPWPDigitsLengthLong {
		// If it contains separators, validate the format
		if strings.Contains(s, ".") || strings.Contains(s, "-") {
			// Pattern for both 15 and 16 digit formats
			pattern := `^\d{2}\.\d{3}\.\d{3}\.\d{1}-\d{3}\.\d{2,3}$`
			regex := regexp.MustCompile(pattern)
			return regex.MatchString(s)
		}
		// If no separators, the digits-only string is valid
		return true
	}

	// Check if it's an NIK (16 digits)
	if len(digitsOnly) == FormatNIKLength {
		// Basic NIK format validation
		// First 6 digits: geographic codes (province, regency, district)
		// Next 6 digits: date of birth in DDMMYY format (for females, 40 is added to date)
		// Last 4 digits: sequential number

		// Get the date part
		day, err := strconv.Atoi(digitsOnly[6:8])
		if err != nil {
			return false
		}

		month, err := strconv.Atoi(digitsOnly[8:10])
		if err != nil {
			return false
		}

		// Basic date validation
		// For females, 40 is added to the day, so day can be between 1-31 for males and 41-71 for females
		isValidDay := (day >= 1 && day <= 31) || (day >= 41 && day <= 71)
		isValidMonth := month >= 1 && month <= 12

		return isValidDay && isValidMonth
	}

	// Neither NPWP nor NIK
	return false
}
