package mqclient

import (
	"testing"
	"time"
)

func TestInvokeSync(t *testing.T) {
	config := NewProducerConfig()
	rpcclient := NewRPCClient(&config.ClientConfig)
	resp, err := rpcclient.InvokeSync("127.0.0.1:3000", &request, time.Second)
	if err != nil {
		t.Fatal(err)
	} else {
		t.Logf("Response recieved:\n")
		t.Logf("Code: %d\n", resp.Code)
		t.Logf("Opaque: %d\n", resp.Opaque)
		t.Logf("Remark: %s\n", resp.Remark)
		t.Logf("Remark: %v\n", resp.ExtFields)
	}
}
