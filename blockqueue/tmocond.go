package blockqueue

import (
	"sync"
	"time"
)

type TMOCond struct {
	L  sync.Locker
	ch chan bool
}

func NewTMOCond(l sync.Locker) *TMOCond {
	return &TMOCond{ch: make(chan bool, 1), L: l}
}

func (t *TMOCond) Wait() {
	t.L.Unlock()
	<-t.ch
	t.L.Lock()
}

func (t *TMOCond) WaitOrTimeout(tmo *time.Timer) bool {
	t.L.Unlock()
	var r bool
	select {
	case <-tmo.C:
		r = false
	case <-t.ch:
		r = true
	}
	t.L.Lock()
	return r
}

func (t *TMOCond) Signal() {
	t.signal()
}

func (t *TMOCond) Broadcast() {
	for {
		// Stop when we run out of waiters
		//
		if !t.signal() {
			return
		}
	}
}

func (t *TMOCond) signal() bool {
	select {
	case t.ch <- true:
		return true
	default:
		return false
	}
}
