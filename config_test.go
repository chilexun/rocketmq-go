package mqclient

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	pConfig := NewProducerConfig()
	pConfig.ReadTimeout = time.Minute * 10
	pConfig.SerializeType = SerialTypeRocketMQ
	err := pConfig.Validate()
	if err != nil {
		t.Log("Validate Pass")
	} else {
		t.FailNow()
	}
}
