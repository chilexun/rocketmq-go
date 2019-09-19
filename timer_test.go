package mqclient

import (
	"sync/atomic"
	"testing"
	"time"
)

var count int32

func accum() {
	atomic.AddInt32(&count, 1)
}

func TestScheduledJob(t *testing.T) {
	tw := NewTimeWheel(1*time.Second, 120)
	tw.Start()
	defer tw.Stop()
	tw.AddJob(2*time.Second, 5*time.Second, accum)
	time.Sleep(20 * time.Second)
	if atomic.LoadInt32(&count) > 3 {
		t.Logf("Current Value:%d \n", atomic.LoadInt32(&count))
	} else {
		t.Logf("Current Value:%d \n", atomic.LoadInt32(&count))
		t.FailNow()
	}
}
