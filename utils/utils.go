package utils

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/donnyhardyanto/dxlib/log"
	"go/types"
	"math"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type JSON = map[string]any

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
func ArrayOfStringIsContains(arr []string, str string) bool {
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
			return nil, fmt.Errorf("key %s does not exist", k)
		}
		value, ok = valueMap[k]
		if !ok {
			return nil, fmt.Errorf("key %s does not exist", k)
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
		err := errors.New(fmt.Sprintf(`TYPE_IS_NOT_CONVERTABLE_TO_INT64:%T`, v))
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
			err := errors.New(fmt.Sprintf(`FLOAT_NUMBER_IS_NOT_INTEGER:%v`, v))
			return nil, err
		}
		r = int(f)
		break
	case float64:
		f := v.(float64)
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(fmt.Sprintf(`FLOAT_NUMBER_IS_NOT_INTEGER:%v`, v))
			return nil, err
		}
		r = int(f)
		break
	default:
		err := errors.New(fmt.Sprintf(`TYPE_IS_NOT_CONVERTABLE_TO_INT:%T`, v))
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
			err := errors.New(fmt.Sprintf(`FLOAT_NUMBER_IS_NOT_INTEGER:%v`, v))
			return nil, err
		}
		r = int64(f)
		break
	case float64:
		f := v.(float64)
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(fmt.Sprintf(`FLOAT_NUMBER_IS_NOT_INTEGER:%v`, v))
			return nil, err
		}
		r = int64(f)
		break
	default:
		err := errors.New(fmt.Sprintf(`TYPE_IS_NOT_CONVERTABLE_TO_INT64:%T`, v))
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
		err := errors.New(fmt.Sprintf(`TYPE_IS_NOT_CONVERTABLE_TO_FLOAT64:%T`, v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceArrayInterfaceFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		err = errors.New(`VALUE_CANT_BE_NIL`)
		return nil, err
	case types.Array:
		r = v.([]any)
		break
	default:
		err = errors.New(fmt.Sprintf(`TYPE_IS_NOT_CONVERTABLE_TO_ARRAY:%T`, v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceStringFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		err = errors.New(`VALUE_CANT_BE_NIL`)
		return nil, err
	case int64:
		r = strconv.FormatInt(v.(int64), 10)
		break
	case float64:
		r = fmt.Sprintf(`%f`, v.(float64))
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
		err = errors.New(fmt.Sprintf(`TYPE_IS_NOT_CONVERTABLE_TO_STRING:%T`, v))
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
		err := errors.New(fmt.Sprint(`TYPE_IS_NOT_CONVERTABLE_TO_MAP[STRING]ANY:%T`, v))
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
			err = fmt.Errorf("error convert JSON to Map[string]string")
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
		log.Log.Errorf("Failed to input confirmation key 1: %s", err.Error())
		return err
	}
	userInputConfirmationKey1 = strings.TrimSpace(userInputConfirmationKey1)

	log.Log.Warnf("Input the input confirmation key 2 to confirm:")
	userInputConfirmationKey2, err := reader.ReadString('\n')
	if err != nil {
		log.Log.Errorf("Failed to input confirmation key 2: %s", err.Error())
		return err
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

func StringArrayHasCommonItem(arr1, arr2 []string) bool {
	for _, str := range arr1 {
		if slices.Contains(arr2, str) {
			return true
		}
	}
	return false
}

func GetJSONFromKV(kv map[string]any, key string) (r JSON, err error) {
	r, ok := kv[key].(JSON)
	if !ok {
		rASBytes, ok := kv[key].([]byte)
		if !ok {
			err = fmt.Errorf("KEY_%S_IS_NOT_JSON", key)
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
