package utils

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"go/types"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

type JSON = map[string]any

func GetAllMachineIP4s() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}

	var ips []string
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP.String())
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
		fmt.Println("Failed to connect:", err)
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
		fmt.Println("Failed to connect:", err)
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
		fmt.Println("RandomData: rand.read error:", err)
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
		err := errors.New(`TypeIsNotConvertableToInt64:` + fmt.Sprint(v))
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
			err := errors.New(`TheFloatNumberIsNotInteger`)
			return nil, err
		}
		r = int(f)
		break
	case float64:
		f := v.(float64)
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(`TheFloatNumberIsNotInteger`)
			return nil, err
		}
		r = int(f)
		break
	default:
		err := errors.New(`TypeIsNotConvertableToInt:` + fmt.Sprint(v))
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
			err := errors.New(`TheFloatNumberIsNotInteger`)
			return nil, err
		}
		r = int64(f)
		break
	case float64:
		f := v.(float64)
		if (math.Ceil(f) - f) != 0 {
			err := errors.New(`TheFloatNumberIsNotInteger`)
			return nil, err
		}
		r = int64(f)
		break
	default:
		err := errors.New(`TypeIsNotConvertableToInt64:` + fmt.Sprint(v))
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
		err := errors.New(`TypeIsNotConvertableToFloat64` + fmt.Sprint(v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceArrayInterfaceFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		err = errors.New(`ValueCannotBeNil`)
		return nil, err
	case types.Array:
		r = v.([]any)
		break
	default:
		err = errors.New(`TypeIsNotConvertableToArray` + fmt.Sprint(v))
		return nil, err
	}
	return r, nil
}

func ConvertToInterfaceStringFromAny(v any) (r any, err error) {
	switch v.(type) {
	case types.Nil:
		r = nil
		err = errors.New(`ValueCannotBeNil`)
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
		err = errors.New(`TypeIsNotConvertableToString` + fmt.Sprint(v))
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
		err := errors.New(`TypeIsNotConvertableToMapStringInterface`)
		return nil, err
	}
	return r, nil
}

func JSONToMapStringString(kv JSON) (r map[string]string, err error) {
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
