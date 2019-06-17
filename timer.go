package mqclient

import (
	"container/list"
	"errors"
	"sync/atomic"
	"time"
)

// time wheel struct
type TimeWheel struct {
	interval   time.Duration
	ticker     *time.Ticker
	slots      []*list.List
	currentPos int
	slotNum    int
	jobChan    chan *job
	stopChan   chan bool
}

// Job callback function
type Runnable func()

// job struct
type job struct {
	delay    time.Duration
	circle   int
	interval time.Duration
	run      Runnable
	running  int32
}

// New create a empty time wheel
func NewTimeWheel(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:   interval,
		slots:      make([]*list.List, slotNum),
		currentPos: 0,
		slotNum:    slotNum,
		jobChan:    make(chan *job),
		stopChan:   make(chan bool),
	}

	tw.init()

	return tw
}

// Start the time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go func() {
		for {
			select {
			case <-tw.ticker.C:
				tw.execute()
			case job := <-tw.jobChan:
				tw.addJob(job)
			case <-tw.stopChan:
				tw.ticker.Stop()
				return
			}
		}
	}()
}

// Stop stop the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChan <- true
}

// AddTask add new task to the time wheel
func (tw *TimeWheel) AddJob(initialDelay time.Duration, interval time.Duration, runFunc Runnable) error {
	if initialDelay < 0 {
		return errors.New("illegal initail delay value, must not be lt 0")
	}
	if interval <= 0 {
		return errors.New("illegal interval value, must be gt 0")
	}

	tw.jobChan <- &job{delay: initialDelay, interval: interval, run: runFunc}
	return nil
}

// time wheel initialize
func (tw *TimeWheel) init() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

//execute the scheduled job
func (tw *TimeWheel) execute() {
	jobs := tw.slots[tw.currentPos]
	tw.scanAddRunJob(jobs)
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
}

func (tw *TimeWheel) addJob(job *job) {
	tw.addJobWithDelay(job.delay, job)
}

func (tw *TimeWheel) addJobWithDelay(delay time.Duration, job *job) {
	pos, circle := tw.getPositionAndCircle(delay)
	job.circle = circle
	tw.slots[pos].PushBack(job)
}

// scan job list and run the task
func (tw *TimeWheel) scanAddRunJob(l *list.List) {
	if l == nil {
		return
	}

	for item := l.Front(); item != nil; {
		job := item.Value.(*job)
		if job.circle > 0 {
			job.circle--
			item = item.Next()
			continue
		}
		if atomic.LoadInt32(&job.running) == 0 {
			atomic.StoreInt32(&job.running, 1)
			go tw.runJob(job)
		}
		next := item.Next()
		l.Remove(item)
		item = next
		tw.addJobWithDelay(job.interval, job)
	}
}

func (tw *TimeWheel) runJob(job *job) {
	job.run()
	atomic.StoreInt32(&job.running, 0)
}

// get the task position
func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum
	return
}
