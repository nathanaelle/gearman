package gearman

import (
	"bytes"
	"context"
	"log"
	"sync"

	"github.com/nathanaelle/gearman/v2/protocol"
)

type (
	// MockServer is a fake local gearman server
	MockServer struct {
		lock     *sync.Mutex
		handlers map[string]Job
	}
)

var _ Worker = &MockServer{}
var _ Client = &MockServer{}

// NewMockServer create a MockServer
func NewMockServer() *MockServer {
	return &MockServer{
		lock:     &sync.Mutex{},
		handlers: make(map[string]Job),
	}
}

func (mc *MockServer) AddServers(...Conn) {

}

func (mc *MockServer) AddHandler(name string, job Job) Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	mc.handlers[name] = job

	return mc
}

func (mc *MockServer) DelHandler(name string) Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	delete(mc.handlers, name)

	return mc
}

func (mc *MockServer) DelAllHandlers() Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	mc.handlers = make(map[string]Job)

	return mc
}

func (mc *MockServer) GetHandler(name string) Job {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if job, ok := mc.handlers[name]; ok {
		return job
	}

	return FailJob
}

func (mc *MockServer) Receivers() (<-chan Message, context.Context) {
	return nil, nil
}

func (mc *MockServer) Close() error {
	return nil
}

func (mc *MockServer) Submit(req Task) Task {
	pkt := req.Packet()

	switch pkt.Cmd() {
	case protocol.SubmitJob:
		reply := make(chan protocol.Packet, 5)

		go runWorker(mc.GetHandler(string(pkt.At(0).Bytes())), bytes.NewReader(pkt.At(2).Bytes()), reply, TaskID{})

		go func() {
			for res := range reply {
				switch res.Cmd() {
				case protocol.WorkCompleteWorker:
					taskRes, _ := protocol.WorkComplete.Borrow(res)
					go req.Handle(taskRes)
					close(reply)
					break

				default:
					log.Fatalf("res unknown: %v %q", res.Cmd(), res.Payload())
				}
			}
		}()

	default:
		log.Fatalf("unknown: %v %q", pkt.Cmd(), pkt.Payload())
	}

	return req
}

func (mc *MockServer) assignTask(tid TaskID) {

}

func (mc *MockServer) getTask(TaskID) Task {
	return nil
}

func (mc *MockServer) extractTask(TaskID) Task {
	return nil
}

func (mc *MockServer) receivers() (<-chan Message, context.Context) {
	return nil, nil
}
