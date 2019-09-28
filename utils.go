package mqclient

import (
	"bytes"
	"compress/zlib"
	"net"
	"regexp"
	"runtime"
	"sync/atomic"
)

//ValidGroupName specifies the group name reg expression
var validGroupName = regexp.MustCompile(`^[%|a-zA-Z0-9_-]{1,255}$`)

//GetIPAddr returns ipv4 addrs and external addr is prefered
func GetIPAddr() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		var internelIP net.IP
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				ipv4 := ipnet.IP.To4()
				if ipv4 != nil && isValidIP(ipv4) {
					if !isInternalAddr(ipv4) {
						return ipv4
					}
					internelIP = ipv4
				}
			}
		}
		if internelIP != nil {
			return internelIP
		}
	}
	panic(err)
}

func isValidIP(ip []byte) bool {
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

func isInternalAddr(ip []byte) bool {
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

//Compress the body with specified level
func Compress(body []byte, compressLevel int) ([]byte, error) {
	buf := new(bytes.Buffer)
	w, err := zlib.NewWriterLevel(buf, compressLevel)
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

//SpinLock is a spin Locker implements
type SpinLock uint32

//Lock blocked util obtain
func (sl *SpinLock) Lock() {
	for !atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1) {
		runtime.Gosched() //without this it locks up on GOMAXPROCS > 1
	}
}

//Unlock release the lock
func (sl *SpinLock) Unlock() {
	atomic.StoreUint32((*uint32)(sl), 0)
}

//TryLock return true if lock obtain success
func (sl *SpinLock) TryLock() bool {
	return atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1)
}
