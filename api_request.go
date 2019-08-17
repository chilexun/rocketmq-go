package mqclient

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

type RPCClient interface {
	InvokeSync(addr string, request *Command, responseType reflect.Type, timeout time.Duration) (*Command, error)
	GetActiveConn(addr string) (*Conn, error)
	CloseAllConns()
}

type defaultRPCClient struct {
	config   *Config
	reqCache sync.Map
	connMap  sync.Map
	lock     sync.Mutex
}

type pendingResponse struct {
	cmd *Command
	err error
}

func NewRPCClient(config *Config) RPCClient {
	return &defaultRPCClient{config: config}
}

func (c *defaultRPCClient) InvokeSync(addr string, request *Command, responseType reflect.Type, timeout time.Duration) (*Command, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer func() {
		cancel()
	}()
	conn, err := c.GetActiveConn(addr)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, errors.New("connect remote timeout")
	default:
		break
	}

	respChan := make(chan *pendingResponse, 1)
	defer c.closeChan(request.Opaque)
	c.reqCache.Store(request.Opaque, respChan)
	conn.WriteCommand(request)

	select {
	case resp := <-respChan:
		if resp.err != nil {
			return nil, resp.err
		}
		if responseType != nil {
			resp.cmd.ResolveCustomHeader(responseType)
		}
		return resp.cmd, nil
	case <-ctx.Done():
		return nil, errors.New("timeout")
	}
}

func (c *defaultRPCClient) GetActiveConn(addr string) (*Conn, error) {
	return c.getOrCreateConn(addr, c.config, c)
}

func (c *defaultRPCClient) getOrCreateConn(addr string, config *Config, respHandler ConnEventListener) (*Conn, error) {
	conn, ok := c.connMap.Load(addr)
	if ok {
		return conn.(*Conn), nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	conn, ok = c.connMap.Load(addr)
	if ok {
		return conn.(*Conn), nil
	}
	newConn := NewConn(addr, config, respHandler)
	err := newConn.Connect()
	if err != nil {
		return nil, err
	}
	c.connMap.Store(addr, newConn)
	return newConn, nil
}

func (c *defaultRPCClient) CloseAllConns() {
	c.connMap.Range(func(k, v interface{}) bool {
		conn := v.(*Conn)
		c.connMap.Delete(conn.addr)
		conn.Close()
		return true
	})
}

func (c *defaultRPCClient) OnMessage(cmd *Command) {
	ch, ok := c.reqCache.Load(cmd.Opaque)
	if ok {
		defer recover()
		ch.(chan *pendingResponse) <- &pendingResponse{cmd, nil}
	}
}

func (c *defaultRPCClient) OnError(opaque int, err error) {
	ch, ok := c.reqCache.Load(opaque)
	if ok {
		defer recover()
		ch.(chan *pendingResponse) <- &pendingResponse{nil, err}
	}
}

func (c *defaultRPCClient) OnIOError(conn *Conn, err error) {

}

func (c *defaultRPCClient) closeChan(requestID int32) {
	v, ok := c.reqCache.Load(requestID)
	if ok {
		c.reqCache.Delete(requestID)
		close(v.(chan *Command))
	}
}
