package blockqueue

import (
	"sync"
	"time"
)

type EmptyQueueError struct{}

type FullQueueError struct{}

func (err *EmptyQueueError) Error() string {
	return "Queue is Empty"
}

func (err *FullQueueError) Error() string {
	return "Queue is Full"
}

var (
	Full  = &FullQueueError{}
	Empty = &EmptyQueueError{}
)

type QueueIF interface {
	Put(v interface{}) error
	Get() (interface{}, error)
	Size() int
}

type BlockQueue struct {
	queue    QueueIF
	mutex    *sync.RWMutex
	tt       sync.Mutex
	notEmpty *TMOCond
	notFull  *TMOCond
	maxSize  int
}

func NewBlockQueue(queue QueueIF, maxSize int) *BlockQueue {
	q := new(BlockQueue)
	q.queue = queue
	q.maxSize = maxSize
	q.mutex = &sync.RWMutex{}
	q.notEmpty = NewTMOCond(q.mutex)
	q.notFull = NewTMOCond(q.mutex)
	return q
}

func (q *BlockQueue) Size() int {
	q.mutex.RLock()
	size := q.queue.Size()
	q.mutex.RUnlock()
	return size
}

func (q *BlockQueue) IsEmpty() bool {
	q.mutex.RLock()
	isEmpty := (q.queue.Size() == 0)
	q.mutex.RUnlock()
	return isEmpty
}

func (q *BlockQueue) IsFull() bool {
	q.mutex.RLock()
	isFull := (q.maxSize > 0 && q.queue.Size() == q.maxSize)
	q.mutex.RUnlock()
	return isFull
}

func (q *BlockQueue) Get(block bool, timeout time.Duration) (interface{}, error) {
	q.notEmpty.L.Lock()
	defer q.notEmpty.L.Unlock()
	empty := false
	if !block {
		if q.queue.Size() == 0 {
			empty = true
		}
	} else if timeout == 0 {
		for q.queue.Size() == 0 {
			q.notEmpty.Wait()
		}
	} else {
		if q.queue.Size() == 0 {
			tmo := time.NewTimer(timeout)
		TIMEOUT:
			for q.queue.Size() == 0 {
				if !q.notEmpty.WaitOrTimeout(tmo) {
					empty = true
					break TIMEOUT
				}
			}
		}
	}
	if empty {
		return nil, Empty
	}
	v, err := q.queue.Get()
	if err != nil {
		return nil, err
	}
	q.notFull.Signal()
	return v, nil
}

func (q *BlockQueue) Put(v interface{}, block bool, timeout time.Duration) error {
	q.notFull.L.Lock()
	defer q.notFull.L.Unlock()
	full := false
	if q.maxSize > 0 {
		if !block {
			if q.queue.Size() == q.maxSize {
				full = true
			}
		} else if timeout == 0 {
			for q.queue.Size() == q.maxSize {
				q.notFull.Wait()
			}
		} else {
			if q.queue.Size() == q.maxSize {
				tmo := time.NewTimer(timeout)
			TIMEOUT:
				for q.queue.Size() == q.maxSize {
					if !q.notFull.WaitOrTimeout(tmo) {
						full = true
						break TIMEOUT
					}
				}
			}
		}
	}
	if full {
		return Full
	}
	err := q.queue.Put(v)
	if err != nil {
		return err
	}
	q.notEmpty.Signal()
	return nil
}
