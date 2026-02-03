package language

import (
	"bufio"
	"strings"

	"github.com/donnyhardyanto/dxlib/utils/string_template"
)

// DXLanguage represents supported languages for translations
type DXLanguage string

// DXTranslateFallbackMode defines how to handle missing translations
type DXTranslateFallbackMode string

const (
	DXLanguageIndonesian DXLanguage = "id"
	DXLanguageEnglish    DXLanguage = "en"
)

var (
	DXLanguageEnumSetAll = []any{DXLanguageEnglish, DXLanguageIndonesian}
)

// DXLanguageDefault is the framework default language (can be overridden by applications)
var DXLanguageDefault DXLanguage = DXLanguageEnglish

const (
	DXTranslateFallbackModeOriginal  DXTranslateFallbackMode = "original"  // Return original key
	DXTranslateFallbackModeEmpty     DXTranslateFallbackMode = "empty"     // Return empty string
	DXTranslateFallbackModeTitleCase DXTranslateFallbackMode = "titlecase" // Convert snake_case to Title Case
)

var DXTranslateFallbackModeEnumSetAll = []any{DXTranslateFallbackModeOriginal, DXTranslateFallbackModeEmpty, DXTranslateFallbackModeTitleCase}

// Dictionaries holds preloaded translations (populated by application at startup)
var Dictionaries = make(map[DXLanguage]map[string]string)

// LoadDictionary loads a dictionary from content string for a specific language
func LoadDictionary(lang DXLanguage, content string) {
	Dictionaries[lang] = ParseDictionary(content)
}

// ParseDictionary parses key=value format dictionary content
func ParseDictionary(content string) map[string]string {
	dict := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			dict[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return dict
}

// Translate returns the translated string for a key, with fallback behavior
func Translate(key string, lang DXLanguage, fallback DXTranslateFallbackMode) string {
	if dict, ok := Dictionaries[lang]; ok {
		if val, ok := dict[key]; ok {
			return val
		}
	}

	// Fallback
	switch fallback {
	case DXTranslateFallbackModeEmpty:
		return ""
	case DXTranslateFallbackModeTitleCase:
		return string_template.SnakeCaseToTitleCase(key)
	default: // DXTranslateFallbackModeOriginal
		return key
	}
}
