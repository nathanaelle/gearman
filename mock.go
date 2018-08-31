package gearman // import "github.com/nathanaelle/gearman"

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

// AddServers implements Client.AddServers and Worker.AddServers
func (mc *MockServer) AddServers(...Conn) {

}

// AddHandler implements Worker.AddHandler
func (mc *MockServer) AddHandler(name string, job Job) Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	mc.handlers[name] = job

	return mc
}

// DelHandler implements Worker.DelHandler
func (mc *MockServer) DelHandler(name string) Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	delete(mc.handlers, name)

	return mc
}

// DelAllHandlers implements Worker.DelAllHandlers
func (mc *MockServer) DelAllHandlers() Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	mc.handlers = make(map[string]Job)

	return mc
}

// GetHandler implements Worker.GetHandler
func (mc *MockServer) GetHandler(name string) Job {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if job, ok := mc.handlers[name]; ok {
		return job
	}

	return FailJob
}

// Receivers implements Client.Receivers and Worker.Receivers
func (mc *MockServer) Receivers() (<-chan Message, context.Context) {
	return nil, nil
}

// Close implements Client.Close and Worker.Close
func (mc *MockServer) Close() error {
	return nil
}

// Submit implements Client.Submit
func (mc *MockServer) Submit(req Task) Task {
	pkt := req.Packet()

	switch pkt.Cmd() {
	case protocol.SubmitJob:
		reply := mockPacketEmiter(req)

		go runWorker(mc.GetHandler(string(pkt.At(0).Bytes())), bytes.NewReader(pkt.At(2).Bytes()), reply, TaskID{})

	default:
		log.Fatalf("unknown: %v %q", pkt.Cmd(), pkt.Payload())
	}

	return req
}

// AssignTask implements Client.AssignTask
func (mc *MockServer) AssignTask(tid TaskID) {

}

// GetTask implements Client.GetTask
func (mc *MockServer) GetTask(TaskID) Task {
	return nil
}

// ExtractTask implements Client.ExtractTask
func (mc *MockServer) ExtractTask(TaskID) Task {
	return nil
}

func mockPacketEmiter(req Task) PacketEmiter {
	lock := sync.Mutex{}

	return newFuncPacketEmiter(func(res protocol.Packet, fpe PacketEmiter) {
		lock.Lock()
		defer lock.Unlock()

		switch res.Cmd() {
		case protocol.WorkCompleteWorker:
			taskRes, _ := protocol.WorkComplete.Borrow(res)
			go req.Handle(taskRes)
			return

		case protocol.WorkFailWorker:
			taskRes, _ := protocol.WorkFail.Borrow(res)
			go req.Handle(taskRes)
			return

		case protocol.WorkExceptionWorker:
			taskRes, _ := protocol.WorkException.Borrow(res)
			go req.Handle(taskRes)
			return

		case protocol.WorkDataWorker:
			taskRes, _ := protocol.WorkData.Borrow(res)
			go req.Handle(taskRes)
			return
		}
		log.Fatalf("res unknown: %v %q", res.Cmd(), res.Payload())
	})
}
