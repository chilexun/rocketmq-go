package mqclient

import (
	"context"
	"errors"
	"sync"
	"time"
)

type RPCClient interface {
	InvokeSync(addr string, request *Command, timeout time.Duration) (*Command, error)
	GetActiveConn(addr string) (*Conn, error)
	CloseAllConns()
}

type defaultRPCClient struct {
	config   *ClientConfig
	reqCache sync.Map
	connMap  sync.Map
	lock     sync.Mutex
}

type pendingResponse struct {
	cmd *Command
	err error
}

func NewRPCClient(config *ClientConfig) RPCClient {
	return &defaultRPCClient{config: config}
}

func (c *defaultRPCClient) InvokeSync(addr string, request *Command, timeout time.Duration) (*Command, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer func() {
		cancel()
	}()
	conn, err := c.GetActiveConn(addr)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, errors.New("connect remote timeout")
	}

	respChan := make(chan *pendingResponse, 1)
	defer c.closeChan(request.Opaque)
	c.reqCache.Store(request.Opaque, respChan)
	err = conn.WriteCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.cmd, nil
	case <-ctx.Done():
		return nil, errors.New("Waiting remote response timeout")
	}
}

func (c *defaultRPCClient) GetActiveConn(addr string) (*Conn, error) {
	return c.getOrCreateConn(addr, c.config, c)
}

func (c *defaultRPCClient) getOrCreateConn(addr string, config *ClientConfig, respHandler ConnEventListener) (*Conn, error) {
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

func (c *defaultRPCClient) OnError(opaque int32, err error) {
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
