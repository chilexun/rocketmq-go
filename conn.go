package mqclient

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ConnEventListener interface {
	//OnMessage is invoked when received a response
	OnMessage(*Command)
	//OnError is invoked when send req error
	OnError(opaque int, err error)
	//OnIOError is invoked when tcp conn error
	OnIOError(*Conn, error)
}

//Conn represent a conn to nameserv/broker
type Conn struct {
	config      *Config
	conn        net.Conn
	addr        string
	respHandler ConnEventListener
	cmdChan     chan *Command
	respChan    chan *Command
	closeFlag   int32
	stopper     sync.Once
	wg          sync.WaitGroup
}

func NewConn(addr string, config *Config, respHandler ConnEventListener) *Conn {
	return &Conn{
		addr:        addr,
		config:      config,
		respHandler: respHandler,
		cmdChan:     make(chan *Command, config.SendChanSize),
		respChan:    make(chan *Command, config.RcvChanSize),
	}
}

func (c *Conn) Connect() error {
	dialer := &net.Dialer{
		LocalAddr: c.config.LocalAddr,
		Timeout:   c.config.ConnectTimeout,
	}

	var err error
	if c.config.TLSConfig != nil {
		c.conn, err = tls.DialWithDialer(dialer, "tcp", c.addr, c.config.TLSConfig)
	} else {
		c.conn, err = dialer.Dial("tcp", c.addr)
	}
	if err != nil {
		return err
	}

	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
	go c.handlerLoop()
	return nil
}

func (c *Conn) Close() error {
	c.stopper.Do(func() {
		atomic.StoreInt32(&c.closeFlag, 1)
		c.conn.Close()
		c.wg.Wait()
		close(c.cmdChan)
		close(c.respChan)
	})
	return nil
}

func (c *Conn) Read(p []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	return c.Read(p)
}

func (c *Conn) Write(bytes []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	return c.Write(bytes)
}

func (c *Conn) WriteCommand(cmd *Command) (err error) {
	if atomic.LoadInt32(&c.closeFlag) == 1 {
		return errors.New("Connect had been closed")
	}
	defer func() {
		if p := recover(); p != nil {
			err = errors.New("Connect had been closed")
		}
	}()
	c.cmdChan <- cmd
	return err
}

//    According RocketMQ protocol
//    [x][x][x][x][x][x][x][x]...
//    |  (int32) || (binary)
//    |  4-byte  || N-byte
//    ------------------------...
//        size       data
func (c *Conn) readLoop() {
	defer func() {
		c.wg.Done()
		c.Close()
	}()
	for atomic.LoadInt32(&c.closeFlag) == 0 {
		var msgSize int32
		var buf []byte
		// message size
		err := binary.Read(c, binary.BigEndian, &msgSize)
		if err == nil {
			// message binary data
			buf = make([]byte, msgSize)
			_, err = io.ReadFull(c, buf)
		}
		if err != nil {
			if err == io.EOF && atomic.LoadInt32(&c.closeFlag) == 1 {
				return
			}
			if !strings.Contains(err.Error(), "use of closed network connection") {
				//write err log
			}
			return
		}
		response, _ := Decode(buf)
		if response != nil {
			c.respChan <- response
		}
	}
}

func (c *Conn) writeLoop() {
	defer func() {
		c.wg.Done()
		c.Close()
	}()
	var cmd *Command
	for atomic.LoadInt32(&c.closeFlag) == 0 {
		cmd = <-c.cmdChan
		data, err := cmd.Encode()
		if err == nil {
			err = binary.Write(c, binary.BigEndian, int32(len(data)))
			if err == nil {
				_, err = c.Write(data)
			}
			if err != nil && atomic.LoadInt32(&c.closeFlag) != 1 {
				//write io err log
				return
			}
		} else {
			//write encode err log
		}
	}
}

func (c *Conn) handlerLoop() {
	var cmd *Command
	for {
		cmd = <-c.respChan
		if cmd != nil {
			c.respHandler.OnMessage(cmd)
		} else {
			return
		}
	}
}
