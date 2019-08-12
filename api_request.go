package mqclient

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

type RpcClient interface {
	InvokeSync(addr string, request *Command, responseType reflect.Type, timeout time.Duration) (*Command, error)
	GetActiveConn(addr string) (*Conn, error)
}

type defaultRpcClient struct {
	config   *Config
	reqCache sync.Map
}

type pendingResponse struct {
	cmd *Command
	err error
}

func NewRpcClient(config *Config) RpcClient {
	return &defaultRpcClient{config: config}
}

func (c *defaultRpcClient) InvokeSync(addr string, request *Command, responseType reflect.Type, timeout time.Duration) (*Command, error) {
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

func (c *defaultRpcClient) GetActiveConn(addr string) (*Conn, error) {
	return GetOrCreateConn(addr, c.config, c)
}

func (c *defaultRpcClient) onMessage(cmd *Command) {
	ch, ok := c.reqCache.Load(cmd.Opaque)
	if ok {
		defer recover()
		ch.(chan *pendingResponse) <- &pendingResponse{cmd, nil}
	}
}

func (c *defaultRpcClient) onError(opaque int, err error) {
	ch, ok := c.reqCache.Load(opaque)
	if ok {
		defer recover()
		ch.(chan *pendingResponse) <- &pendingResponse{nil, err}
	}
}

func (c *defaultRpcClient) closeChan(requestId int32) {
	v, ok := c.reqCache.Load(requestId)
	if ok {
		c.reqCache.Delete(requestId)
		close(v.(chan *Command))
	}
}
