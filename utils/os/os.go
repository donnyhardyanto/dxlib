package os

import (
	"github.com/pkg/errors"
	"os"
	"strconv"
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
		return errors.Wrap(err, "error occured")
	}

	file, err := os.Open(filename)
	if err != nil {
		return errors.Wrap(err, "error occured")
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
			return errors.Wrap(err, "error occured")
		}
	}

	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "error occured")
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
