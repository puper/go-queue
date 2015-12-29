package queue

import(
	"time"
	"container/list"
	"errors"
)

type EmptyQueueError struct{}

type FullQueueError struct{}

func (err *EmptyQueueError) Error() string {
	return "Queue is Empty"
}

func (err *FullQueueError) Error() string {
	return "Queue is Full"
}

type Queue struct {
	list list.List
	maxSize int
	enQueueTicketCh chan bool
	enQueueReqCh chan *Request
	deQueueTicketCh chan bool
	deQueueReqCh chan *Request
}

type Request struct {
	params interface{}
	resp chan interface{}
}

func NewRequest(params interface{}) *Request {
	return &Request{
		params: params,
		resp: make(chan interface{}),
	}
}

func NewQueue(maxSize int) *Queue {
	q := new(Queue)
	q.maxSize = maxSize
	q.enQueueTicketCh = make(chan bool, 1)
	q.deQueueTicketCh = make(chan bool, 1)
	q.enQueueReqCh = make(chan *Request, 1)
	q.deQueueReqCh = make(chan *Request, 1)
	q.deQueueTicketCh<-true
	go q.handleRequest()
	return q
}

func (q *Queue) Size() int {
	return q.list.Len()
}

func (q *Queue) handleRequest() {
	for {
		if q.Size() == 0 {
			<-q.enQueueTicketCh
			req := <-q.enQueueReqCh
			q.put(req.params)
			req.resp<-true
			<-q.deQueueTicketCh
		} else if q.Size() == 1 {
			select {
				case <-q.enQueueTicketCh:
					req := <-q.enQueueReqCh
					q.put(req.params)
					req.resp<-true
				case req := <-q.deQueueReqCh:
					req.resp <- q.get()
			}
		} else if q.maxSize > 0 && q.Size() == q.maxSize {
			<-q.deQueueTicketCh
			req := <-q.deQueueReqCh
			req.resp<-q.get()
			<-q.enQueueTicketCh
		} else if q.maxSize > 0 && q.Size() == q.maxSize - 1 {
			select {
				case req := <-q.enQueueReqCh:
					q.put(req.params)
					req.resp<-true
				case  <-q.deQueueTicketCh:
					req := <-q.deQueueReqCh
					req.resp <- q.get()
			}
		} else {
			select {
				case <-q.enQueueTicketCh:
					req := <-q.enQueueReqCh
					q.put(req.params)
					req.resp<-true
				case  <-q.deQueueTicketCh:
					req := <-q.deQueueReqCh
					req.resp <- q.get()
			}
		}
	}
}

func (q *Queue) EnQueue(v interface{}, block bool, timeout float64) error {
	req := NewRequest(v)
	if !block {
		select {
			case q.enQueueTicketCh<-true:
				q.enQueueReqCh<-req
				<-req.resp
				return nil
			default:
				return &FullQueueError{}
		}
	} else if timeout == float64(0) {
		q.enQueueTicketCh<-true
		q.enQueueReqCh<-req
		<-req.resp
		return nil
	} else if timeout < float64(0) {
		return errors.New("'timeout' must be a non-negative number")
	} else {
		select {
			case q.enQueueTicketCh<-true:
				q.enQueueReqCh<-req
				<-req.resp
				return nil
			case <-time.After(time.Duration(timeout) * time.Second):
				return &FullQueueError{}
		}
	}
}

func (q *Queue) DeQueue(block bool, timeout float64) (interface{}, error) {
	req := NewRequest(nil)
	if !block {
		select {
			case q.deQueueTicketCh<-true:
				q.deQueueReqCh<-req
				return <-req.resp, nil
			default:
				return nil, &EmptyQueueError{}
		}
	} else if timeout == float64(0) {
		q.deQueueTicketCh<-true
		q.deQueueReqCh<-req
		return <-req.resp, nil
	} else if timeout < float64(0) {
		return nil, errors.New("'timeout' must be a non-negative number")
	} else {
		select {
			case q.deQueueTicketCh<-true:
				q.deQueueReqCh<-req
				return <-req.resp, nil
			case <-time.After(time.Duration(timeout) * time.Second):
				return nil, &EmptyQueueError{}
		}
	}
}

func (q *Queue) put(v interface{}) {
	q.list.PushBack(v)
}

func (q *Queue) get() interface{} {
	e := q.list.Front()
	q.list.Remove(e)
	return e.Value
}