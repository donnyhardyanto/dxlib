package utils

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go/types"
	"math"
	"net"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/donnyhardyanto/dxlib/log"
	"github.com/pkg/errors"
)

type JSON = map[string]any

func ArrayToJSON[T any](arr []T) (string, error) {
	jsonBytes, err := json.Marshal(arr)
	if err != nil {
		return "", errors.Errorf("failed to marshal array: %w", err)
	}
	return string(jsonBytes), nil
}

func StringsToJSON(arr []string) string {
	jsonBytes, _ := json.Marshal(arr)
	return string(jsonBytes)
}
func IntsToJSON(arr []int) string {
	jsonBytes, err := json.Marshal(arr)
	if err != nil {
		return "[]" // Return empty array in extremely unlikely error case
	}
	return string(jsonBytes)
}
func Int64sToJSON(arr []int64) string {
	jsonBytes, _ := json.Marshal(arr)
	return string(jsonBytes)
}

func Int64sToStrings(arr []int64) []string {
	r := make([]string, len(arr))
	for i, v := range arr {
		r[i] = strconv.FormatInt(v, 10)
	}
	return r
}

func Float64sToJSON(arr []float64) string {
	jsonBytes, _ := json.Marshal(arr)
	return string(jsonBytes)
}
func StringToJSON(s string) (JSON, error) {
	v := JSON{}
	err := json.Unmarshal([]byte(s), &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func JSONToString(v JSON) (string, error) {
	s, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

func JSONToBytes(v JSON) ([]byte, error) {
	s, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return s, nil
}
func StringsIsContains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

func GetAllMachineIP4s() []string {
	address, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}

	var ips []string
	for _, addr := range address {
		if ipNetwork, ok := addr.(*net.IPNet); ok && !ipNetwork.IP.IsLoopback() {
			if ipNetwork.IP.To4() != nil {
				ips = append(ips, ipNetwork.IP.String())
			}
		}
	}
	return ips
}

func GetAllActualBindingAddress(configuredBindingAddress string) []string {

	// Split the config value to get the IP and port
	splitConfig := strings.Split(configuredBindingAddress, ":")
	configIP := splitConfig[0]
	port := splitConfig[1]

	// Get all binding IPs
	ips := GetAllMachineIP4s()

	// Check if the config IP is in the list of binding IPs
	var validIPs []string
	for _, ip := range ips {
		if ip == configIP {
			validIPs = append(validIPs, ip)
			break
		}
	}

	// If the config IP is not in the list of binding IPs, use all IPs
	if len(validIPs) == 0 {
		validIPs = ips
	}

	var r []string
	// Append the port to each IP and print
	for _, ip := range validIPs {
		r = append(r, ip+":"+port)
	}
	return r
}

func TCPIPPortCanConnect(ip string, port string) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, port), time.Second*3)
	if err != nil {
		fmt.Println("Failed to connect:", err.Error())
		return false
	}
	if conn != nil {
		defer func() {
			_ = conn.Close()
		}()
	}
	return true
}

func TCPAddressCanConnect(address string) bool {
	conn, err := net.DialTimeout("tcp", address, time.Second*3)
	if err != nil {
		fmt.Println("Failed to connect:", err.Error())
		return false
	}
	if conn != nil {
		defer func() {
			_ = conn.Close()
		}()
	}
	return true
}

func NowAsString() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func IfFloatIsInt(f float64) bool {
	fi := int64(f)
	if (f - float64(fi)) > 0 {
		return false
	}
	return true
}

func TypeAsString(v any) string {
	return fmt.Sprintf("%T", v)
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func GetValueFromNestedMap(data map[string]interface{}, key string) (interface{}, error) {
	keys := strings.Split(key, ".")
	var value interface{}

	value = data
	for _, k := range keys {
		valueMap, ok := value.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("key %s does not exist", k)
		}
		value, ok = valueMap[k]
		if !ok {
			return nil, errors.Errorf("key %s does not exist", k)
		}
	}
	return value, nil
}

func SetValueInNestedMap(data map[string]interface{}, key string, value interface{}) {
	keys := strings.Split(key, ".")
	lastKeyIndex := len(keys) - 1

	for i, k := range keys {
		if i == lastKeyIndex {
			data[k] = value
		} else {
			nextMap, ok := data[k].(map[string]interface{})
			if !ok {
				nextMap = make(map[string]interface{})
				data[k] = nextMap
			}
			data = nextMap
		}
	}
	return
}

func IfStringInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

func RandomData(l int) (r []byte) {
	r = make([]byte, l)
	_, err := rand.Read(r)
	if err != nil {
		fmt.Println("RandomData: rand.read error:", err.Error())
		return
	}
	return r
}

func TimeSubToString(t1 any, t2 any) (r string) {
	if t1 == nil {
		return ""
	}
	if t2 == nil {
		return ""
	}
	dt1 := t1.(time.Time)
	dt2 := t2.(time.Time)
	d := dt2.Sub(dt1)
	return d.String()
}

func ConvertToInterfaceBoolFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		break
	case bool:
		r = v
		break
	case string:
		v, err := strconv.ParseBool(v.(string))
		if err != nil {
			return nil, err
		}
		r = v
		break
	case int:
		r = v.(int) != 0
		break
	case int64:
		r = v.(int64) != 0
		break
	case float32:
		r = v.(float32) != 0
		break
	case float64:
		r = v.(float64) != 0
		break
	default:
		err := errors.New(fmt.Sprintf("TYPE_IS_NOT_CONVERTABLE_TO_INT64:%T", v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceIntFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		break
	case string:
		v, err := strconv.Atoi(v.(string))
		if err != nil {
			return nil, err
		}
		r = v
		break
	case int:
		r = v.(int)
		break
	case int64:
		r = int(v.(int64))
		break
	case float32:
		f := float64(v.(float32))
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(fmt.Sprintf("FLOAT_NUMBER_IS_NOT_INTEGER:%v", v))
			return nil, err
		}
		r = int(f)
		break
	case float64:
		f := v.(float64)
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(fmt.Sprintf("FLOAT_NUMBER_IS_NOT_INTEGER:%v", v))
			return nil, err
		}
		r = int(f)
		break
	default:
		err := errors.New(fmt.Sprintf("TYPE_IS_NOT_CONVERTABLE_TO_INT:%T", v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceInt64FromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		break
	case string:
		v, err := strconv.ParseInt(v.(string), 10, 64)
		if err != nil {
			return nil, err
		}
		r = v
		break
	case int:
		r = int64(v.(int))
		break
	case int64:
		r = v.(int64)
		break
	case float32:
		f := float64(v.(float32))
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(fmt.Sprintf("FLOAT_NUMBER_IS_NOT_INTEGER:%v", v))
			return nil, err
		}
		r = int64(f)
		break
	case float64:
		f := v.(float64)
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(fmt.Sprintf("FLOAT_NUMBER_IS_NOT_INTEGER:%v", v))
			return nil, err
		}
		r = int64(f)
		break
	default:
		err := errors.New(fmt.Sprintf("TYPE_IS_NOT_CONVERTABLE_TO_INT64:%T", v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceFloat64FromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		break
	case int64:
		r = float64(v.(int64))
		break
	case float64:
		r = v.(float64)
		break
	case string:
		vs, err := strconv.ParseFloat(v.(string), 64)
		if err != nil {
			return nil, err
		}
		r = vs
		break
	default:
		err := errors.New(fmt.Sprintf("TYPE_IS_NOT_CONVERTABLE_TO_FLOAT64:%T", v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceArrayInterfaceFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		err = errors.New("VALUE_CANT_BE_NIL")
		return nil, err
	case types.Array:
		r = v.([]any)
		break
	default:
		err = errors.New(fmt.Sprintf("TYPE_IS_NOT_CONVERTABLE_TO_ARRAY:%T", v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceStringFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		err = errors.New("VALUE_CANT_BE_NIL")
		return nil, err
	case int64:
		r = strconv.FormatInt(v.(int64), 10)
		break
	case float64:
		r = fmt.Sprintf("%f", v.(float64))
		break
	case string:
		r = v.(string)
		break
	case map[string]any:
		vs, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		r = string(vs)
		break
	case []uint8:
		r = string(v.([]byte))
		break
	case time.Time:
		vt := v.(time.Time)
		r = vt.Format(time.RFC3339)
		break
	case bool:
		vb := v.(bool)
		if vb {
			r = "TRUE"
		} else {
			r = "FALSE"
		}
		break
	default:
		err = errors.New(fmt.Sprintf("TYPE_IS_NOT_CONVERTABLE_TO_STRING:%T", v))
		return nil, err
	}
	return r, nil
}

func MustConvertToInterfaceStringFromAny(v any) (r any) {
	r, err := ConvertToInterfaceStringFromAny(v)
	if err != nil {
		panic(err)
	}
	return r
}
func ConvertToMapStringInterfaceFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		break
	case map[string]any:
		r = v
		break
	default:
		err := errors.New(fmt.Sprint("TYPE_IS_NOT_CONVERTABLE_TO_MAP[STRING]ANY:%T", v))
		return nil, err
	}
	return r, nil
}

func JSONToMapStringString(kv JSON) (r map[string]string) {
	r = map[string]string{}
	for k, v := range kv {
		switch v.(type) {
		case string:
			r[k] = v.(string)
		default:
			r[k] = fmt.Sprintf("%v", v)
		}
	}
	return r
}

func ShouldStrictJSONToMapStringString(kv JSON) (r map[string]string, err error) {
	r = map[string]string{}
	for k, v := range kv {
		switch v.(type) {
		case string:
			r[k] = v.(string)
		default:
			err = errors.Errorf("error convert JSON to Map[string]string")
			return nil, err
		}
	}
	return r, nil
}

func AnyToBytes(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	switch v := data.(type) {
	case int:
		err := binary.Write(buf, binary.BigEndian, int64(v))
		if err != nil {
			return nil, err
		}
	case int64:
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	case float64:
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	case string:
		err := binary.Write(buf, binary.BigEndian, []byte(v))
		if err != nil {
			return nil, err
		}
	case []byte:
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New(fmt.Sprintf("UNSUPPORTED_TYPE:%T", v))
	}
	return buf.Bytes(), nil
}

func BytesToInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0 // or handle the error as needed
	}
	return int64(binary.BigEndian.Uint64(b))
}

func AskForConfirmation(key1 string, key2 string) (err error) {
	reader := bufio.NewReader(os.Stdin)

	log.Log.Warnf("Input confirmation key 1?")
	userInputConfirmationKey1, err := reader.ReadString('\n')
	if err != nil {
		log.Log.Errorf(err, "Failed to input confirmation key 1")
		return errors.Wrap(err, "error occured")
	}
	userInputConfirmationKey1 = strings.TrimSpace(userInputConfirmationKey1)

	log.Log.Warnf("Input the input confirmation key 2 to confirm:")
	userInputConfirmationKey2, err := reader.ReadString('\n')
	if err != nil {
		log.Log.Errorf(err, "Failed to input confirmation key 2")
		return errors.Wrap(err, "error occured")
	}
	userInputConfirmationKey2 = strings.TrimSpace(userInputConfirmationKey2)

	if userInputConfirmationKey1 != key1 {
		err := log.Log.ErrorAndCreateErrorf("Confirmation key mismatch")
		return errors.Wrap(err, "error occured")
	}
	if userInputConfirmationKey2 != key2 {
		err := log.Log.ErrorAndCreateErrorf("Confirmation key mismatch")
		return errors.Wrap(err, "error occured")
	}

	return nil
}

// Diff returns the intersection and difference between two arrays
// Returns:
//   - included: values from first that exist in second
//   - missing: values from first that do NOT exist in second
func Diff[T comparable](first []T, second []T) (included, missing []T) {
	// RequestCreate a map of all values from second array
	valueMap := make(map[T]bool)
	for _, value := range second {
		valueMap[value] = true
	}

	// For each value in first array:
	// - if it exists in valueMap -> add to included
	// - if it doesn't exist in valueMap -> add to missing
	for _, value := range first {
		if valueMap[value] {
			included = append(included, value)
		} else {
			missing = append(missing, value)
		}
	}

	return included, missing
}

// DiffJsonFieldValues checks values existence between valuesToCheck and jsonData[fieldName]
// Returns:
//   - included: values from valuesToCheck that exist in jsonData[fieldName]
//   - missing: values from valuesToCheck that do NOT exist in jsonData[fieldName]
func DiffJsonFieldValues[T comparable](valuesToCheck []T, jsonData []map[string]any, fieldName string) (included, missing []T) {
	// RequestCreate a map of all values from jsonData[fieldName]
	valueMap := make(map[T]bool)
	for _, record := range jsonData {
		if value, ok := record[fieldName].(T); ok {
			valueMap[value] = true
		}
	}

	// For each value in valuesToCheck:
	// - if it exists in valueMap -> add to included
	// - if it doesn't exist in valueMap -> add to missing
	for _, value := range valuesToCheck {
		if valueMap[value] {
			included = append(included, value)
		} else {
			missing = append(missing, value)
		}
	}

	return included, missing
}

// K is the type for the map key (usually string)
// V is the type for the values we're comparing (must be comparable)
func FindCommonValues[K comparable, V comparable](arrays1, arrays2 []map[K]any, key K) []V {
	// RequestCreate maps to store unique values from each array
	values1 := make(map[V]bool)
	values2 := make(map[V]bool)

	// Collect values from first array
	for _, m := range arrays1 {
		if val, exists := m[key]; exists {
			if typedVal, ok := val.(V); ok {
				values1[typedVal] = true
			}
		}
	}

	// Collect values from second array
	for _, m := range arrays2 {
		if val, exists := m[key]; exists {
			if typedVal, ok := val.(V); ok {
				values2[typedVal] = true
			}
		}
	}

	// Find common values
	var common []V
	for val := range values1 {
		if values2[val] {
			common = append(common, val)
		}
	}

	return common
}

func FindCommonValuesInMapString[V comparable](arrays1, arrays2 []map[string]any, key string) []V {
	return FindCommonValues[string, V](arrays1, arrays2, key)
}

func StringsHasCommonItem(arr1, arr2 []string) bool {
	for _, str := range arr1 {
		if slices.Contains(arr2, str) {
			return true
		}
	}
	return false
}

func GetJSONFromV(v any) (r JSON, err error) {
	r, ok := v.(JSON)
	if !ok {
		rASBytes, ok := v.([]byte)
		if !ok {
			err = errors.Errorf("VALUE_IS_NOT_JSON:%v", v)
			return nil, err
		}
		r = JSON{}
		err = json.Unmarshal(rASBytes, &r)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func GetArrayFromV(v any) (r []any, err error) {
	if v == nil {
		return nil, nil
	}
	rASBytes, ok := v.([]byte)
	if !ok {
		err = errors.Errorf("VALUE_IS_NOT_ARRAY_BYTE:%v", v)
		return nil, err
	}
	r = []any{}
	err = json.Unmarshal(rASBytes, &r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func GetJSONFromKV(kv map[string]any, key string) (r JSON, err error) {
	if kv == nil {
		return nil, nil
	}
	r, ok := kv[key].(JSON)
	if !ok {
		var rASBytes []byte
		switch value := kv[key].(type) {
		case []byte:
			rASBytes = value
		case string:
			rASBytes = []byte(value) // Convert string to []byte
		default:
			err = errors.Errorf("KEY_%s_IS_NOT_JSON", key)
			return nil, err
		}
		r = JSON{}
		err = json.Unmarshal(rASBytes, &r)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func Int64SliceToStrings(nums []int64) []string {
	strs := make([]string, len(nums))
	for i, num := range nums {
		strs[i] = strconv.FormatInt(num, 10)
	}
	return strs
}

// GetMapValue safely retrieves and type-asserts a value from a map[string]any.
// Returns:
// - exists: True if the key exists in the map
// - value: The typed value if key exists and type assertion succeeds, nil otherwise
// - err: Error if type assertion fails for existing key
func GetMapValue[T any](m map[string]any, key string) (exist bool, value T, err error) {
	// Check if key exist
	rawValue, keyExist := m[key]
	if !keyExist {
		return false, value, nil
	}

	// If value is nil, return early
	if rawValue == nil {
		return true, value, nil
	}

	// Attempt type assertion
	typedValue, ok := rawValue.(T)
	if !ok {
		return true, value, errors.Errorf("value for key '%s' cannot be converted to requested type", key)
	}

	return true, typedValue, nil
}

func ExtractMapValue[T any](m *map[string]any, key string) (exists bool, value T, err error) {
	exists, value, err = GetMapValue[T](*m, key)
	if err != nil {
		return exists, value, err
	}
	if exists {
		delete(*m, key)
	}
	return exists, value, nil
}

func GetMapValueFromJSONs[T any](a []map[string]any, key string) (values []T, error error) {
	values = []T{}
	for _, m := range a {
		isExist, value, err := GetMapValue[T](m, key)
		if isExist {
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	return values, nil
}

// RemoveDuplicates removes duplicate values from a slice of any comparable type
func RemoveDuplicates[T comparable](slice []T) []T {
	// Create a map to track seen values
	seen := make(map[T]bool)
	result := make([]T, 0)

	// Iterate through the slice
	for _, value := range slice {
		// If the value hasn't been seen before, add it to result
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}

	return result
}

func GetBuildTime() string {
	// Try to get VCS timestamp from build info
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.time" {
				// Parse and format the time to ensure consistent output
				if t, err := time.Parse(time.RFC3339, setting.Value); err == nil {
					return t.Format(time.RFC3339)
				}
				return setting.Value
			}
		}
	}
	return ""
}

// ConvertToInt64 converts various types to int64
func ConvertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert string count to int64")
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert []byte count to int64")
		}
		return parsed, nil
	default:
		return 0, errors.Errorf("unexpected count value type: %T", value)
	}
}
