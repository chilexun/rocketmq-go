package mqclient

import (
	"container/list"
	"sync"
)

type processQueue struct {
	msgMap  map[int64]*MessageExt
	msgList list.List
	lock    sync.RWMutex
}

func newProcessQueue() *processQueue {
	return nil
}

func (q *processQueue) put(msgs []*MessageExt) {

}

func (q *processQueue) remove(msgs []*MessageExt) {

}

func (q *processQueue) expire() []*MessageExt {
	return nil
}
