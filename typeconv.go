package mqclient

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

//Coerce convert arg to expect type value
func Coerce(v interface{}, typ reflect.Type) (reflect.Value, error) {
	var err error
	if typ.Kind() == reflect.Ptr {
		return reflect.ValueOf(v), nil
	}
	switch typ.String() {
	case "string":
		v, err = CoerceString(v), nil
	case "int", "int16", "int32", "int64":
		v, err = CoerceInt64(v)
	case "uint", "uint16", "uint32", "uint64":
		v, err = CoerceUint64(v)
	case "float32", "float64":
		v, err = CoerceFloat64(v)
	case "bool":
		v, err = CoerceBool(v)
	case "time.Duration":
		v, err = CoerceDuration(v)
	case "net.Addr":
		v, err = CoerceAddr(v)
	default:
		v = nil
		err = fmt.Errorf("invalid type %s", typ.String())
		return reflect.ValueOf(v), err
	}
	return valueTypeCoerce(v, typ), nil
}

func valueTypeCoerce(v interface{}, typ reflect.Type) reflect.Value {
	val := reflect.ValueOf(v)
	if reflect.TypeOf(v) == typ {
		return val
	}
	tval := reflect.New(typ).Elem()
	switch typ.String() {
	case "int", "int16", "int32", "int64":
		tval.SetInt(val.Int())
	case "uint", "uint16", "uint32", "uint64":
		tval.SetUint(val.Uint())
	case "float32", "float64":
		tval.SetFloat(val.Float())
	default:
		tval.Set(val)
	}
	return tval
}

//CoerceString convert arg to string
func CoerceString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case int, int16, int32, int64, uint, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	}
	return fmt.Sprintf("%s", v)
}

//CoerceDuration convert arg to time.Duration
func CoerceDuration(v interface{}) (time.Duration, error) {
	switch v := v.(type) {
	case string:
		return time.ParseDuration(v)
	case int, int16, int32, int64:
		// treat like ms
		return time.Duration(reflect.ValueOf(v).Int()) * time.Millisecond, nil
	case uint, uint16, uint32, uint64:
		// treat like ms
		return time.Duration(reflect.ValueOf(v).Uint()) * time.Millisecond, nil
	case time.Duration:
		return v, nil
	}
	return 0, errors.New("invalid value type")
}

//CoerceAddr convert arg to net.Addr
func CoerceAddr(v interface{}) (net.Addr, error) {
	switch v := v.(type) {
	case string:
		return net.ResolveTCPAddr("tcp", v)
	case net.Addr:
		return v, nil
	}
	return nil, errors.New("invalid value type")
}

//CoerceBool convert arg to bool
func CoerceBool(v interface{}) (bool, error) {
	switch v := v.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int, int16, int32, int64:
		return reflect.ValueOf(v).Int() != 0, nil
	case uint, uint16, uint32, uint64:
		return reflect.ValueOf(v).Uint() != 0, nil
	}
	return false, errors.New("invalid value type")
}

//CoerceFloat64 convert arg to float64
func CoerceFloat64(v interface{}) (float64, error) {
	switch v := v.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case int, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int()), nil
	case uint, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Uint()), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	}
	return 0, errors.New("invalid value type")
}

//CoerceInt64 convert arg to int64
func CoerceInt64(v interface{}) (int64, error) {
	switch v := v.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case int, int16, int32, int64:
		return reflect.ValueOf(v).Int(), nil
	case uint, uint16, uint32, uint64:
		return int64(reflect.ValueOf(v).Uint()), nil
	}
	return 0, errors.New("invalid value type")
}

//CoerceUint64 convert arg to uint64
func CoerceUint64(v interface{}) (uint64, error) {
	switch v := v.(type) {
	case string:
		return strconv.ParseUint(v, 10, 64)
	case int, int16, int32, int64:
		return uint64(reflect.ValueOf(v).Int()), nil
	case uint, uint16, uint32, uint64:
		return reflect.ValueOf(v).Uint(), nil
	}
	return 0, errors.New("invalid value type")
}

//ValueCompare reflect value, return -1 if v1 < v2
func ValueCompare(v1 reflect.Value, v2 reflect.Value) (result int, err error) {
	switch v1.Type().String() {
	case "int", "int16", "int32", "int64":
		if v1.Int() > v2.Int() {
			result = 1
		} else if v1.Int() < v2.Int() {
			result = -1
		}
	case "uint", "uint16", "uint32", "uint64":
		if v1.Uint() > v2.Uint() {
			result = 1
		} else if v1.Uint() < v2.Uint() {
			result = -1
		}
	case "float32", "float64":
		if v1.Float() > v2.Float() {
			result = 1
		} else if v1.Float() < v2.Float() {
			result = -1
		}
	case "time.Duration":
		if v1.Interface().(time.Duration) > v2.Interface().(time.Duration) {
			result = 1
		} else if v1.Interface().(time.Duration) < v2.Interface().(time.Duration) {
			result = -1
		}
	default:
		result = -99
		err = errors.New("The values are not comparable")
	}
	return
}

//ResolveStruct initial new object and copy properties from map
func ResolveStruct(fields map[string]string, t reflect.Type) reflect.Value {
	header := reflect.New(t).Elem()
	for k, v := range fields {
		name := strings.ToUpper(k[:1]) + k[1:]
		if f := header.FieldByName(name); f.CanSet() {
			setFieldValue(f, v)
		}
	}
	return header
}

func setFieldValue(field reflect.Value, v string) {
	switch field.Type().String() {
	case "string":
		field.SetString(v)
	case "int", "int16", "int32", "int64":
		if i, err := strconv.Atoi(v); err == nil {
			field.SetInt(int64(i))
		}
	case "uint", "uint16", "uint32", "uint64":
		if i, err := strconv.Atoi(v); err == nil {
			field.SetUint(uint64(i))
		}
	case "float32":
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			field.SetFloat(float64(f))
		}
	case "float64":
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			field.SetFloat(float64(f))
		}
	case "bool":
		if b, err := strconv.ParseBool(v); err == nil {
			field.SetBool(b)
		}
	}
}
