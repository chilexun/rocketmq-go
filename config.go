package mqclient

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"
)

//ClientConfig of rocketmq client options
type ClientConfig struct {
	NamesrvAddr             []string
	WsAddr                  string
	PollNameServerInterval  time.Duration `min:"100ms" default:"60s"`
	HeartbeatBrokerInterval time.Duration `min:"100ms" default:"60s"`
	InstanceName            string        `default:"DEFAULT"`
	ConnectTimeout          time.Duration `min:"100ms" max:"5m" default:"60s"`
	ReadTimeout             time.Duration `min:"100ms" max:"5m" default:"60s"`
	WriteTimeout            time.Duration `min:"100ms" max:"5m" default:"1s"`
	LocalAddr               net.Addr
	TLSConfig               *tls.Config
	SendChanSize            int `default:"100"`
	RcvChanSize             int `default:"100"`
	initialized             bool
}

//ProducerConfig of rocketmq options
type ProducerConfig struct {
	ClientConfig
	RetryTimesWhenSendFailed         int `min:"0" default:"2"`
	RetryAnotherBrokerWhenNotStoreOK bool
	SendMessageWithVIPChannel        bool `default:"true"`
	CompressMsgBodyOverHowmuch       int  `min:"512" default:"4094"`
	ZipCompressLevel                 int  `min:"0" default:"1"`
}

//ConsumerConfig of rocketmq options
type ConsumerConfig struct {
	ClientConfig
}

//NewProducerConfig return a new default producer configuration
// This must be used to initialize Config structs.
func NewProducerConfig() *ProducerConfig {
	config := &ProducerConfig{}
	setDefaults(config)
	setDefaults(&config.ClientConfig)
	config.initialized = true
	return config
}

//NewConsumerConfig return a new default consumer configuration
// This must be used to initialize Config structs.
func NewConsumerConfig() *ConsumerConfig {
	config := &ConsumerConfig{}
	setDefaults(config)
	setDefaults(&config.ClientConfig)
	config.initialized = true
	return config
}

func setDefaults(c interface{}) {
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

//Validate return error if set invalid values of producer options
func (c *ProducerConfig) Validate() error {
	if !c.initialized {
		return errors.New("Config{} must be created with NewConfig()")
	}
	err := validate(c)
	if err == nil {
		err = validate(c.ClientConfig)
	}
	return err
}

//Validate return error if set invalid values of consumer options
func (c *ConsumerConfig) Validate() error {
	if !c.initialized {
		return errors.New("Config{} must be created with NewConfig()")
	}
	err := validate(c)
	if err == nil {
		err = validate(c.ClientConfig)
	}
	return err
}

func validate(c interface{}) error {
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
