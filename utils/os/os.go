package os

import (
	"os"
	"strconv"

	"github.com/donnyhardyanto/dxlib/errors"
)

import (
	"bufio"
	"strings"
)

func LoadEnvFile(filename string) error {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "ERROR_IN_LOAD_ENV_FILE_STAT_FILENAME")
	}

	file, err := os.Open(filename)
	if err != nil {
		return errors.Wrap(err, "ERROR_IN_LOAD_ENV_FILE_OPEN")
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]
		err := os.Setenv(key, value)
		if err != nil {
			return errors.Wrap(err, "ERROR_IN_LOAD_ENV_FILE_SETENV")
		}
	}

	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "ERROR_IN_LOAD_ENV_FILE_SCAN")
	}

	return nil
}

func GetEnvDefaultValue(key string, defaultValue string) string {
	value, isPresent := os.LookupEnv(key)
	if !isPresent {
		value = defaultValue
	}
	return value
}

// GetEnvDefaultWithFallback gets environment variable with fallback chain
// Tries keys in order, returns first non-empty value, or defaultValue if all are empty/unset
// Example: GetEnvDefaultWithFallback([]string{"ENCRYPTION_VAULT_ADDRESS", "VAULT_ADDRESS"}, "http://localhost:8200")
func GetEnvDefaultWithFallback(keys []string, defaultValue string) string {
	for _, key := range keys {
		value, isPresent := os.LookupEnv(key)
		if isPresent && value != "" {
			return value
		}
	}
	return defaultValue
}

func GetEnvDefaultValueAsInt(key string, defaultValue int) int {
	value, isPresent := os.LookupEnv(key)
	if !isPresent {
		return defaultValue
	}
	valueInt, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return valueInt
}

func GetEnvDefaultValueAsBool(key string, defaultValue bool) bool {
	value, isPresent := os.LookupEnv(key)
	if !isPresent {
		return defaultValue
	}
	valueBool := (strings.ToUpper(value) == "TRUE") || (value == "1")
	return valueBool
}
