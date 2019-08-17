package mqclient

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"
)

//Config of rocketmq client options
type Config struct {
	NamesrvAddr                      []string
	WsAddr                           string
	PollNameServerInterval           time.Duration `min:"100ms" default:"60s"`
	HeartbeatBrokerInterval          time.Duration `min:"100ms" default:"60s"`
	InstanceName                     string        `default:"DEFAULT"`
	RetryTimesWhenSendFailed         int           `min:"0" default:"3"`
	RetryAnotherBrokerWhenNotStoreOK bool          `default:"true"`
	SendMessageWithVIPChannel        bool
	CompressMsgBodyOverHowmuch       int
	ZipCompressLevel                 int
	ConnectTimeout                   time.Duration `min:"100ms" max:"5m" default:"2m"`
	ReadTimeout                      time.Duration `min:"100ms" max:"5m" default:"60s"`
	WriteTimeout                     time.Duration `min:"100ms" max:"5m" default:"1s"`
	LocalAddr                        net.Addr
	TLSConfig                        *tls.Config
	SendChanSize                     uint `default:"100"`
	RcvChanSize                      uint `default:"100"`
	initialized                      bool
}

//NewConfig returns default rocketmq client configuration
//This must be used to initialize Config structs.
func NewConfig() *Config {
	config := new(Config)
	config.setDefaults()
	config.initialized = true
	return config
}

func (c *Config) setDefaults() {
	val := reflect.ValueOf(c).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		defaultVal := field.Tag.Get("default")
		if !val.Field(i).CanSet() || defaultVal == "" {
			continue
		}
		value, err := Coerce(defaultVal, field.Type)
		if err == nil {
			val.Field(i).Set(value)
		}
	}
}

//Validate return error if set invalid values of options
func (c *Config) Validate() error {
	if !c.initialized {
		return errors.New("Config{} must be created with NewConfig()")
	}
	val := reflect.ValueOf(c).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		min := field.Tag.Get("min")
		max := field.Tag.Get("max")
		fieldVal := val.Field(i)

		if min != "" {
			minVal, _ := Coerce(fieldVal, field.Type)
			result, err := ValueCompare(fieldVal, minVal)
			if err != nil {
				return err
			} else if result == -1 {
				return fmt.Errorf("invalid config %s ! %v < %v", field.Name, fieldVal.Interface(), minVal.Interface())
			}
		}
		if max != "" {
			maxVal, _ := Coerce(fieldVal, field.Type)
			result, err := ValueCompare(fieldVal, maxVal)
			if err != nil {
				return err
			} else if result == 1 {
				return fmt.Errorf("invalid config %s ! %v > %v", field.Name, fieldVal.Interface(), maxVal.Interface())
			}
		}
	}
	return nil
}
