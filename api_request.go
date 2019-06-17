package mqclient

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

type RpcClient interface {
	InvokeSync(addr string, request *Command, timeout time.Duration) (*Command, error)
	GetOrCreateActiveConn(addr string) bool
}

func NewRpcClient(config *Config) RpcClient {
	return nil
}

var reqCache sync.Map

func InvokeSync(addr string, config *Config, request *Command, responseType reflect.Type, timeout time.Duration) (*Command, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer func() {
		closeChan(request.Opaque)
		cancel()
	}()
	conn, err := GetOrCreateConn(addr, config, OnResponse)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, errors.New("connect remote timeout")
	default:
		break
	}

	respChan := make(chan *Command, 1)
	reqCache.Store(request.Opaque, respChan)
	conn.WriteCommand(request)

	select {
	case cmd := <-respChan:
		cmd.ResolveCustomHeader(responseType)
		return cmd, nil
	case <-ctx.Done():
		return nil, errors.New("timeout")
	}
}

func OnResponse(cmd *Command) {
	ch, ok := reqCache.Load(cmd.Opaque)
	if ok {
		defer recover()
		ch.(chan *Command) <- cmd
	}
}

func closeChan(requestId int32) {
	v, ok := reqCache.Load(requestId)
	if ok {
		reqCache.Delete(requestId)
		close(v.(chan *Command))
	}
}
