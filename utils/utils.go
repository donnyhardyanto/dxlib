package utils

import (
	"bufio"
	"bytes"
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

	"github.com/donnyhardyanto/dxlib/errors"
	"github.com/donnyhardyanto/dxlib/log"
)

// JSON is a type alias for map[string]any, representing a JSON object.
type JSON = map[string]any

// ArrayToJSON converts a slice of any type to a JSON string.
func ArrayToJSON[T any](arr []T) (string, error) {
	jsonBytes, err := json.Marshal(arr)
	if err != nil {
		return "", errors.Errorf("failed to marshal array: %+v", err)
	}
	return string(jsonBytes), nil
}

// StringsToJSON converts a slice of strings to a JSON string.
func StringsToJSON(arr []string) string {
	jsonBytes, _ := json.Marshal(arr)
	return string(jsonBytes)
}

// IntsToJSON converts a slice of ints to a JSON string.
func IntsToJSON(arr []int) string {
	jsonBytes, err := json.Marshal(arr)
	if err != nil {
		return "[]" // Return empty array in extremely unlikely error case
	}
	return string(jsonBytes)
}

// Int64sToJSON converts a slice of int64s to a JSON string.
func Int64sToJSON(arr []int64) string {
	jsonBytes, _ := json.Marshal(arr)
	return string(jsonBytes)
}

// Int64sToStrings converts a slice of int64s to a slice of strings.
func Int64sToStrings(arr []int64) []string {
	r := make([]string, len(arr))
	for i, v := range arr {
		r[i] = strconv.FormatInt(v, 10)
	}
	return r
}

// Float64sToJSON converts a slice of float64s to a JSON string.
func Float64sToJSON(arr []float64) string {
	jsonBytes, _ := json.Marshal(arr)
	return string(jsonBytes)
}

// StringToJSON converts a JSON string to a JSON object.
func StringToJSON(s string) (JSON, error) {
	v := JSON{}
	err := json.Unmarshal([]byte(s), &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// JSONToString converts a JSON object to a string.
func JSONToString(v JSON) (string, error) {
	s, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

// JSONToBytes converts a JSON object to a byte slice.
func JSONToBytes(v JSON) ([]byte, error) {
	s, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// TsIsContain checks if a slice of a comparable type contains a specific value.
func TsIsContain[T comparable](arr []T, v T) bool {
	for _, a := range arr {
		if a == v {
			return true
		}
	}
	return false
}

// Int64sIsContain checks if a slice of int64s contains a specific value.
func Int64sIsContain(arr []int64, v int64) bool {
	return TsIsContain[int64](arr, v)
}

// StringsIsContain checks if a slice of strings contains a specific value.
func StringsIsContain(arr []string, v string) bool {
	return TsIsContain[string](arr, v)
}

// GetAllMachineIP4s retrieves all non-loopback IPv4 addresses of the machine.
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

// GetAllActualBindingAddress returns a list of actual binding addresses based on a configured address.
// If the configured IP is not found on the machine, it returns all available IPs with the configured port.
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

// TCPIPPortCanConnect checks if a TCP connection can be established to a given IP and port.
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

// TCPAddressCanConnect checks if a TCP connection can be established to a given address.
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

// NowAsString returns the current UTC time as a string in RFC3339 format.
func NowAsString() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// IfFloatIsInt checks if a float64 has no fractional part.
func IfFloatIsInt(f float64) bool {
	fi := int64(f)
	if (f - float64(fi)) > 0 {
		return false
	}
	return true
}

// TypeAsString returns the type of a variable as a string.
func TypeAsString(v any) string {
	return fmt.Sprintf("%T", v)
}

// Int64ToString converts an int64 to a string.
func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

// GetValueFromNestedMap retrieves a value from a nested map using a dot-separated key.
// It traverses the map based on the keys provided in the dot-separated string.
// For example, given a map `{"a": {"b": 1}}` and a key `"a.b"`, it will return `1`.
// If any key in the path does not exist, it returns an error.
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

// SetValueInNestedMap sets a value in a nested map using a dot-separated key.
// It creates nested maps if they don't exist.
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

// StringIsInSlice checks if a string exists in a slice of strings.
func StringIsInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

// TsIsInSlice checks if all elements of a slice exist in another slice.
func TsIsInSlice[T comparable](v []T, aSlice []T) bool {
	for _, vi := range v {
		if !TsIsContain(aSlice, vi) {
			return false
		}
	}
	return true
}

// TimeSubToString returns the string representation of the duration between two time.Time objects.
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

// ConvertToInterfaceBoolFromAny converts a value of any type to a boolean interface.
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

// ConvertToInterfaceIntFromAny converts a value of any type to an integer interface.
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

// ConvertToInterfaceInt64FromAny converts a value of any type to an int64 interface.
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

// ConvertToInterfaceFloat64FromAny converts a value of any type to a float64 interface.
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

// ConvertToInterfaceArrayInterfaceFromAny converts a value of any type to a slice of interfaces.
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

// ConvertToInterfaceStringFromAny converts a value of any type to a string interface.
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

// MustConvertToInterfaceStringFromAny converts a value of any type to a string interface, panicking on error.
func MustConvertToInterfaceStringFromAny(v any) (r any) {
	r, err := ConvertToInterfaceStringFromAny(v)
	if err != nil {
		panic(err)
	}
	return r
}

// ConvertToMapStringInterfaceFromAny converts a value of any type to a map[string]any interface.
func ConvertToMapStringInterfaceFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		break
	case map[string]any:
		r = v
		break
	default:
		err := errors.Errorf("TYPE_IS_NOT_CONVERTABLE_TO_MAP[STRING]ANY:%T", v)
		return nil, err
	}
	return r, nil
}

// ConvertToArrayOfMapStringAnyFromAny converts a value of any type to []map[string]any.
// Handles both []map[string]any (direct) and []interface{} (from JSON unmarshal) cases.
func ConvertToArrayOfMapStringAnyFromAny(v any) ([]map[string]any, error) {
	switch val := v.(type) {
	case []map[string]any:
		return val, nil
	case []any:
		result := make([]map[string]any, len(val))
		for i, item := range val {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, errors.Errorf("ELEMENT_AT_INDEX_%d_IS_NOT_MAP_STRING_ANY:%T", i, item)
			}
			result[i] = m
		}
		return result, nil
	default:
		return nil, errors.Errorf("TYPE_IS_NOT_CONVERTABLE_TO_ARRAY_OF_MAP_STRING_ANY:%T", v)
	}
}

// JSONToMapStringString converts a JSON object to a map[string]string.
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

// MapStringStringToJSON converts a map[string]string to a JSON object.
func MapStringStringToJSON(kv map[string]string) (r JSON) {
	r = JSON{}
	for k, v := range kv {
		r[k] = v
	}
	return r
}

// ShouldStrictJSONToMapStringString strictly converts a JSON object to a map[string]string, returning an error if any value is not a string.
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

// AnyToBytes converts a value of a supported type to a byte slice.
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

// BytesToInt64 converts a byte slice to an int64.
func BytesToInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0 // or handle the error as needed
	}
	return int64(binary.BigEndian.Uint64(b))
}

// AskForConfirmation prompts the user for two confirmation keys and returns an error if they don't match the provided keys.
func AskForConfirmation(key1 string, key2 string) (err error) {
	reader := bufio.NewReader(os.Stdin)

	log.Log.Warnf("Input confirmation key 1?")
	userInputConfirmationKey1, err := reader.ReadString('\n')
	if err != nil {
		log.Log.Errorf(err, "Failed to input confirmation key 1")
		return errors.Wrap(err, "ERROR_IN_ASK_FOR_CONFIRMATION_KEY_1")
	}
	userInputConfirmationKey1 = strings.TrimSpace(userInputConfirmationKey1)

	log.Log.Warnf("Input the input confirmation key 2 to confirm:")
	userInputConfirmationKey2, err := reader.ReadString('\n')
	if err != nil {
		log.Log.Errorf(err, "Failed to input confirmation key 2")
		return errors.Wrap(err, "ERROR_IN_ASK_FOR_CONFIRMATION_KEY_2")
	}
	userInputConfirmationKey2 = strings.TrimSpace(userInputConfirmationKey2)

	if userInputConfirmationKey1 != key1 {
		err := log.Log.ErrorAndCreateErrorf("Confirmation key mismatch")
		return err
	}
	if userInputConfirmationKey2 != key2 {
		err := log.Log.ErrorAndCreateErrorf("Confirmation key mismatch")
		return err
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
// FindCommonValues finds common values in a specific key between two slices of maps.
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

// FindCommonValuesInMapString is a specialization of FindCommonValues for maps with string keys.
func FindCommonValuesInMapString[V comparable](arrays1, arrays2 []map[string]any, key string) []V {
	return FindCommonValues[string, V](arrays1, arrays2, key)
}

// StringsHasCommonItem checks if two string slices have any common items.
func StringsHasCommonItem(arr1, arr2 []string) bool {
	for _, str := range arr1 {
		if slices.Contains(arr2, str) {
			return true
		}
	}
	return false
}

// GetJSONFromV converts an `any` type to a JSON object.
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

// GetArrayFromV converts an `any` type to a slice of `any`.
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

// GetJSONFromKV retrieves a JSON object from a map by key.
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

// GetKVFromKV retrieves a nested map[string]any (KV) from a map by key.
// This is an alias to GetJSONFromKV since JSON is defined as map[string]any.
func GetKVFromKV(kv map[string]any, key string) (r map[string]any, err error) {
	return GetJSONFromKV(kv, key)
}

// GetVFromKV retrieves a value of a specific generic type T from a map[string]any.
// Error messages are structured for client-side parsing and localization.
func GetVFromKV[T any](kv map[string]any, key string) (r T, err error) {
	if kv == nil {
		// Error Code: KV_IS_NIL
		return r, errors.New("KV_IS_NIL")
	}

	// 1. Map Lookup
	v, ok := kv[key]
	if !ok {
		// Error Code: KEY_IS_NOT_EXIST
		return r, errors.Errorf("KEY_IS_NOT_EXIST:%s", key)
	}

	// 2. Type Assertion
	vAsT, ok := v.(T)
	if !ok {
		// Error Code: KEY_VALUE_IS_NOT_TYPE_T_BUT_X
		// %s: Key, %T: Expected Type (from r), %T: Actual Type (from v)
		return r, errors.Errorf("KEY_VALUE_IS_NOT_TYPE_T_BUT_X:%s:%T:%T", key, r, v)
	}

	return vAsT, nil
}

// GetStringFromKV retrieves a string value for the given key from a map[string]any.
// It relies on the generic GetVFromKV function to perform the key lookup and type assertion.
func GetStringFromKV(kv map[string]any, key string) (r string, err error) {
	return GetVFromKV[string](kv, key)
}

func GetStringFromMapStringString(kv map[string]string, key string) (r string, err error) {
	if kv == nil {
		// Error Code: KV_IS_NIL
		return r, errors.New("GetStringFromMapStringString:KV_IS_NIL")
	}

	v, ok := kv[key]
	if !ok {
		// Error Code: KEY_IS_NOT_EXIST
		return r, errors.Errorf("GetStringFromMapStringString:KEY_IS_NOT_EXIST:%s", key)
	}

	return v, nil
}

func GetStringFromMapStringStringDefault(kv map[string]string, key string, defaultValue string) (r string) {
	r, err := GetStringFromMapStringString(kv, key)
	if err != nil {
		return defaultValue
	}
	return r
}

// GetInt64FromKV retrieves an int64 value for the given key from a map[string]any.
// It relies on the generic GetVFromKV function for key lookup and type assertion.
func GetInt64FromKV(kv map[string]any, key string) (r int64, err error) {
	return GetVFromKV[int64](kv, key)
}

// GetIntFromKV retrieves an int value (default 32 or 64 bit) for the given key.
func GetIntFromKV(kv map[string]any, key string) (r int, err error) {
	return GetVFromKV[int](kv, key)
}

// GetUint64FromKV retrieves a uint64 value for the given key.
func GetUint64FromKV(kv map[string]any, key string) (r uint64, err error) {
	return GetVFromKV[uint64](kv, key)
}

// GetFloat64FromKV retrieves a float64 value for the given key.
func GetFloat64FromKV(kv map[string]any, key string) (r float64, err error) {
	return GetVFromKV[float64](kv, key)
}

// GetBoolFromKV retrieves a bool value for the given key.
func GetBoolFromKV(kv map[string]any, key string) (r bool, err error) {
	return GetVFromKV[bool](kv, key)
}

// GetBytesFromKV retrieves a []byte (byte slice) value for the given key.
func GetBytesFromKV(kv map[string]any, key string) (r []byte, err error) {
	return GetVFromKV[[]byte](kv, key)
}

// Int64SliceToStrings converts a slice of int64 to a slice of strings.
func Int64SliceToStrings(nums []int64) []string {
	strs := make([]string, len(nums))
	for i, num := range nums {
		strs[i] = strconv.FormatInt(num, 10)
	}
	return strs
}

func ConvertInt64FromKV(kv map[string]any, key string) (r int64, err error) {
	if kv == nil {
		// Error Code: KV_IS_NIL
		return r, errors.New("KV_IS_NIL")
	}

	// 1. Map Lookup
	v, ok := kv[key]
	if !ok {
		// Error Code: KEY_IS_NOT_EXIST
		return r, errors.Errorf("KEY_IS_NOT_EXIST:%s", key)
	}

	return ConvertToInt64(v)
}

// ConvertIntFromKV retrieves a value from map and converts it to int
func ConvertIntFromKV(kv map[string]any, key string) (r int, err error) {
	if kv == nil {
		// Error Code: KV_IS_NIL
		return r, errors.New("KV_IS_NIL")
	}

	// 1. Map Lookup
	v, ok := kv[key]
	if !ok {
		// Error Code: KEY_IS_NOT_EXIST
		return r, errors.Errorf("KEY_IS_NOT_EXIST:%s", key)
	}

	return ConvertToInt(v)
}

// ConvertFloat32FromKV retrieves a value from map and converts it to float32
func ConvertFloat32FromKV(kv map[string]any, key string) (r float32, err error) {
	if kv == nil {
		// Error Code: KV_IS_NIL
		return r, errors.New("KV_IS_NIL")
	}

	// 1. Map Lookup
	v, ok := kv[key]
	if !ok {
		// Error Code: KEY_IS_NOT_EXIST
		return r, errors.Errorf("KEY_IS_NOT_EXIST:%s", key)
	}

	return ConvertToFloat32(v)
}

// ConvertFloat64FromKV retrieves a value from map and converts it to float64
func ConvertFloat64FromKV(kv map[string]any, key string) (r float64, err error) {
	if kv == nil {
		// Error Code: KV_IS_NIL
		return r, errors.New("KV_IS_NIL")
	}

	// 1. Map Lookup
	v, ok := kv[key]
	if !ok {
		// Error Code: KEY_IS_NOT_EXIST
		return r, errors.Errorf("KEY_IS_NOT_EXIST:%s", key)
	}

	return ConvertToFloat64(v)
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

// ExtractMapValue retrieves a value from a map and deletes the key.
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

// GetMapValueFromJSONs extracts values for a given key from a slice of maps.
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

// GetBuildTime retrieves the VCS build time from the build info.
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

// ConvertToInt converts various types to int
func ConvertToInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case int32:
		return int(v), nil
	case float64:
		return int(v), nil
	case float32:
		return int(v), nil
	case string:
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert string to int")
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.Atoi(string(v))
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert []byte to int")
		}
		return parsed, nil
	default:
		return 0, errors.Errorf("unexpected value type for int conversion: %T", value)
	}
}

// ConvertToFloat32 converts various types to float32
func ConvertToFloat32(value interface{}) (float32, error) {
	switch v := value.(type) {
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	case int:
		return float32(v), nil
	case int64:
		return float32(v), nil
	case int32:
		return float32(v), nil
	case string:
		parsed, err := strconv.ParseFloat(v, 32)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert string to float32")
		}
		return float32(parsed), nil
	case []byte:
		parsed, err := strconv.ParseFloat(string(v), 32)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert []byte to float32")
		}
		return float32(parsed), nil
	default:
		return 0, errors.Errorf("unexpected value type for float32 conversion: %T", value)
	}
}

// ConvertToFloat64 converts various types to float64
func ConvertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert string to float64")
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, errors.Wrap(err, "failed to convert []byte to float64")
		}
		return parsed, nil
	default:
		return 0, errors.Errorf("unexpected value type for float64 conversion: %T", value)
	}
}

// ConvertToInt64FromKV gets value from map by key and converts it to int64
func ConvertToInt64FromKV(kv map[string]any, key string) (int64, error) {
	val, ok := kv[key]
	if !ok {
		return 0, errors.Errorf("key '%s' not found in map", key)
	}
	return ConvertToInt64(val)
}

// ConvertToIntFromKV gets value from map by key and converts it to int
func ConvertToIntFromKV(kv map[string]any, key string) (int, error) {
	val, ok := kv[key]
	if !ok {
		return 0, errors.Errorf("key '%s' not found in map", key)
	}
	return ConvertToInt(val)
}

// ConvertToFloat32FromKV gets value from map by key and converts it to float32
func ConvertToFloat32FromKV(kv map[string]any, key string) (float32, error) {
	val, ok := kv[key]
	if !ok {
		return 0, errors.Errorf("key '%s' not found in map", key)
	}
	return ConvertToFloat32(val)
}

// ConvertToFloat64FromKV gets value from map by key and converts it to float64
func ConvertToFloat64FromKV(kv map[string]any, key string) (float64, error) {
	val, ok := kv[key]
	if !ok {
		return 0, errors.Errorf("key '%s' not found in map", key)
	}
	return ConvertToFloat64(val)
}

// ConvertToStringFromKV gets value from map by key and converts it to string
func ConvertToStringFromKV(kv map[string]any, key string) (string, error) {
	val, ok := kv[key]
	if !ok {
		return "", errors.Errorf("key '%s' not found in map", key)
	}
	if val == nil {
		return "", nil
	}
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

// ConvertToBoolFromKV gets value from map by key and converts it to bool
func ConvertToBoolFromKV(kv map[string]any, key string) (bool, error) {
	val, ok := kv[key]
	if !ok {
		return false, errors.Errorf("key '%s' not found in map", key)
	}
	switch v := val.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case string:
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return false, errors.Wrap(err, "failed to convert string to bool")
		}
		return parsed, nil
	default:
		return false, errors.Errorf("unexpected value type for bool conversion: %T", val)
	}
}

func IsValuesMatch(a, b any) bool {
	// 1. If types are identical, direct comparison is safest
	if a == b {
		return true
	}

	// 2. Handle Numeric Cross-Comparison (int vs float)
	// We convert both to float64 to see if the numeric value is the same
	fa, okA := ConvertToFloat(a)
	fb, okB := ConvertToFloat(b)
	if okA && okB {
		return fa == fb
	}

	// 3. If they aren't both numbers and aren't identical, they don't match
	// This ensures "1" (string) != 1 (int)
	return false
}

func ConvertToFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case float64:
		return t, true
	case float32:
		return float64(t), true
	}
	return 0, false
}
