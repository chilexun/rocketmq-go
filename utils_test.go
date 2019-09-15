package mqclient

import (
	"testing"
)

func TestGetIPAddr(t *testing.T) {
	ipaddr := GetIPAddr()
	t.Logf("IP Addr : %s", ipaddr)
}

func TestCompress(t *testing.T) {
	rawData := []byte{22, 127, 33, 45, 127, 22, 32, 0, 28, 32, 32, 0}
	_, err := Compress(rawData, 5)
	if err == nil {
		t.Log("PASS")
	} else {
		t.Error("FAILED")
	}
}
