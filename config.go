package mqclient

import (
	"crypto/tls"
	"net"
	"time"
)

type Config struct {
	NamesrvAddr                      []string
	WsAddr                           string
	PollNameServerInterval           time.Duration
	HeartbeatBrokerInterval          time.Duration
	ProducerGroup                    string
	InstanceName                     string
	RetryTimesWhenSendFailed         int
	RetryAnotherBrokerWhenNotStoreOK bool
	SendMessageWithVIPChannel        bool
	CompressMsgBodyOverHowmuch       int
	ZipCompressLevel                 int
	ConnectTimeout                   time.Duration
	ReadTimeout                      time.Duration
	WriteTimeout                     time.Duration
	LocalAddr                        net.Addr
	TlsConfig                        *tls.Config
	SendChanSize                     int
	RcvChanSize                      int
}

func NewConfig() *Config {
	return nil
}

func (c *Config) Validate() error {
	return nil
}
