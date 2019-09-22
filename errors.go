package mqclient

import (
	"fmt"
)

//MQBrokerError represents the error from broker
type MQBrokerError struct {
	Code       ResponseCode
	ErrMessage string
}

func (e MQBrokerError) Error() string {
	return fmt.Sprintf("CODE:%d,DESC:%s", e.Code, e.ErrMessage)
}
