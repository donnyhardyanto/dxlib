package string_template

import (
	"fmt"
	"regexp"
	"strings"
)

func ReplaceTagWithValue(originalString string, prefixTag string, kv map[string]any) string {
	// Dynamically create the regex based on the prefix provided
	// Result: <prefix\.(\w+)>
	pattern := fmt.Sprintf(`<%s\.(\w+)>`, regexp.QuoteMeta(prefixTag))
	re := regexp.MustCompile(pattern)

	return re.ReplaceAllStringFunc(originalString, func(match string) string {
		// Strip the "<prefix." and ">" to get the key
		key := strings.TrimSuffix(strings.TrimPrefix(match, "<"+prefixTag+"."), ">")

		if val, ok := kv[key].(string); ok {
			return val
		}
		return match
	})
}

func SnakeCaseToTitleCase(s string) string {
	words := strings.Split(s, "_")
	for i, word := range words {
		if len(word) > 0 {
			words[i] = strings.ToUpper(word[:1]) + word[1:]
		}
	}
	return strings.Join(words, " ")
}
