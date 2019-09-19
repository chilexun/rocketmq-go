package mqclient

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

var server atomic.Value
var tsURL string

type testHandler struct {
	count int32
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&h.count) > 1 {
		fmt.Fprintln(w, "127.0.0.1:3002")
	} else {
		fmt.Fprintln(w, "127.0.0.1:3001")
	}
	atomic.AddInt32(&h.count, 1)
}

func startWsServer() {
	ts := httptest.NewServer(&testHandler{})
	server.Store(ts)
	tsURL = ts.URL
}

func TestStart(t *testing.T) {
	go startWsServer()
	defer func() {
		ts := server.Load().(*httptest.Server)
		ts.Close()
	}()
	for tsURL == "" {
		time.Sleep(10 * time.Second)
	}
	config := &ClientConfig{
		WsAddr: tsURL,
	}
	client := GetClientInstance(config, "testInstance")
	client.Start()
	defer client.Shutdown()

	result := testFetchNameServer(client)
	if result {
		t.Logf("PASS\n")
	} else {
		t.FailNow()
	}
}

func testFetchNameServer(client *MQClient) bool {
	success := true
	for i := 0; i < 60; i++ {
		addrs := client.nameServAddrs
		if len(addrs) > 0 && addrs[0] == "127.0.0.1:3001" {
			success = true
			break
		}
		time.Sleep(time.Second)
	}
	if success {
		fmt.Println("Pass the 1st step test")
		time.Sleep(3 * time.Minute)
		for i := 0; i < 60; i++ {
			addrs := client.nameServAddrs
			if len(addrs) > 0 && addrs[0] == "127.0.0.1:3002" {
				return true
			}
			time.Sleep(time.Second)
		}
	}
	return false
}
