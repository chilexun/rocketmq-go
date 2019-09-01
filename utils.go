package mqclient

import (
	"bytes"
	"compress/flate"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

func GetIPAddr() string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		var internelIp string
		for _, addr := range addrs {
			ip := addr.String()
			if idx := strings.IndexByte(ip, ':'); idx > 0 {
				ip = ip[:idx]
			}
			if b, v4 := GetIPv4(ip); v4 && IsValidIp(b) {
				if !IsInternalAddr(b) {
					return ip
				} else {
					internelIp = ip
				}
			}
		}
		if len(internelIp) > 0 {
			return internelIp
		}
	}
	panic(err)
}

func GetIPv4(ipStr string) ([]byte, bool) {
	splited := strings.Split(ipStr, ".")
	if len(splited) != 4 {
		return nil, false
	}
	ip := make([]byte, 4)
	for i, str := range splited {
		b, err := strconv.Atoi(str)
		if err != nil {
			return nil, false
		}
		ip[i] = byte(b)
	}
	return ip, true
}

func IsValidIp(ip []byte) bool {
	if ip[0] >= 1 && ip[0] <= 126 {
		if ip[1] == 1 && ip[2] == 1 && ip[3] == 1 {
			return false
		}
		if ip[1] == 0 && ip[2] == 0 && ip[3] == 0 {
			return false
		}
		return true
	} else if ip[0] >= 128 && ip[0] <= 191 {
		if ip[2] == 1 && ip[3] == 1 {
			return false
		}
		if ip[2] == 0 && ip[3] == 0 {
			return false
		}
		return true
	} else if ip[0] >= 192 && ip[0] <= 223 {
		if ip[3] == 1 {
			return false
		}
		if ip[3] == 0 {
			return false
		}
		return true
	}
	return false
}

func IsInternalAddr(ip []byte) bool {
	if ip[0] == 10 {
		return true
	} else if ip[0] == 172 {
		if ip[1] >= 16 && ip[1] <= 31 {
			return true
		}
	} else if ip[0] == 192 {
		if ip[1] == 168 {
			return true
		}
	}
	return false
}

func Compress(body []byte, compressLevel int) ([]byte, error) {
	buf := new(bytes.Buffer)
	w, err := flate.NewWriter(buf, compressLevel)
	if err != nil {
		return nil, err
	}
	_, err = w.Write(body)
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type spinLock uint32

func (sl *spinLock) Lock() {
	for !atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1) {
		runtime.Gosched() //without this it locks up on GOMAXPROCS > 1
	}
}
func (sl *spinLock) Unlock() {
	atomic.StoreUint32((*uint32)(sl), 0)
}
func (sl *spinLock) TryLock() bool {
	return atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1)
}
func SpinLock() sync.Locker {
	var lock spinLock
	return &lock
}
