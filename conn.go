package mqclient

import (
	"bufio"
	"context"
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

//ConnEventListener will be callback by conn
type ConnEventListener interface {
	//OnMessage is invoked when received a response
	OnMessage(*Command)
	//OnError is invoked when send req error
	OnError(opaque int32, err error)
	//OnIOError is invoked when tcp conn error
	OnIOError(*Conn, error)
	//OnClosed is invoked when conn is closed
	OnClosed(*Conn)
}

type commandHolder struct {
	cmd *Command
	ctx context.Context
}

//Conn represents a conn to nameserv/broker
type Conn struct {
	config      *ClientConfig
	conn        net.Conn
	addr        string
	respHandler ConnEventListener
	cmdChan     chan commandHolder
	respChan    chan *Command
	closeFlag   int32
	stopper     sync.Once
	wg          sync.WaitGroup
}

//NewConn returns a connection and has not connected
func NewConn(addr string, config *ClientConfig, respHandler ConnEventListener) *Conn {
	return &Conn{
		addr:        addr,
		config:      config,
		respHandler: respHandler,
		cmdChan:     make(chan commandHolder, config.SendChanSize),
		respChan:    make(chan *Command, config.RcvChanSize),
	}
}

//Connect try to establish connection with server and r/w message
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

//Close the connection with server and stop to receive command
func (c *Conn) Close() (err error) {
	c.stopper.Do(func() {
		atomic.StoreInt32(&c.closeFlag, 1)
		err = c.conn.Close()
		close(c.cmdChan)
		c.wg.Wait()
		close(c.respChan)
		c.respHandler.OnClosed(c)
	})
	return
}

//IsActive tests if the connection is availible
func (c *Conn) IsActive() bool {
	return atomic.LoadInt32(&c.closeFlag) == 0
}

//GetAddr returns the remote ip address&port
func (c *Conn) GetAddr() string {
	return c.addr
}

func (c *Conn) Read(p []byte) (int, error) {
	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}
	return c.conn.Read(p)
}

func (c *Conn) Write(bytes []byte) (int, error) {
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}
	return c.conn.Write(bytes)
}

//WriteCommand encode the command and write to the connection
//the operation is async
func (c *Conn) WriteCommand(ctx context.Context, cmd *Command) (err error) {
	if atomic.LoadInt32(&c.closeFlag) == 1 {
		return errors.New("Connect had been closed")
	}
	defer func() {
		if p := recover(); p != nil {
			err = errors.New("Connect had been closed")
		}
	}()
	for {
		select {
		case c.cmdChan <- commandHolder{cmd, ctx}:
			return nil
		case <-ctx.Done():
			return errors.New("Request send buffer's full")
		}
	}
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
	reader := bufio.NewReader(c)
	for atomic.LoadInt32(&c.closeFlag) == 0 {
		var msgSize int32
		var buf []byte
		// message size
		err := binary.Read(reader, binary.BigEndian, &msgSize)
		if err == nil {
			// message binary data
			buf = make([]byte, msgSize)
			_, err = io.ReadFull(reader, buf)
		}
		if err != nil {
			if err == io.EOF && atomic.LoadInt32(&c.closeFlag) == 1 {
				//manual closed
			}
			if !strings.Contains(err.Error(), "use of closed network connection") {
				c.respHandler.OnIOError(c, err)
			}
			return
		}
		response, err := DecodeCommand(buf)
		if err == nil {
			c.respChan <- response
		}
	}
}

func (c *Conn) writeLoop() {
	defer func() {
		c.wg.Done()
		c.Close()
	}()
	var cmdHolder commandHolder
	for atomic.LoadInt32(&c.closeFlag) == 0 {
		cmdHolder = <-c.cmdChan
		if cmdHolder.cmd == nil {
			return //cmdChan is closed
		}
		err := cmdHolder.ctx.Err()
		if err != nil {
			logger.Warnf("Command discard since timeout, %d", cmdHolder.cmd.Opaque)
			continue
		}

		data, err := EncodeCommand(cmdHolder.cmd, SerializeType(c.config.SerializeType))
		if err == nil {
			_, err = c.Write(data)
			if err != nil && atomic.LoadInt32(&c.closeFlag) != 1 {
				logger.Warnf("Command discard since conn closed, %d", cmdHolder.cmd.Opaque)
				return
			}
		}
		if err != nil {
			c.respHandler.OnError(cmdHolder.cmd.Opaque, err)
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
