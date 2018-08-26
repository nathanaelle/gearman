// +build ignore

package gearman

import (
	"context"
	"sync"
)

type (
	MockServer struct {
		lock *sync.Mutex
		jobs map[string]Job
	}
)

var _ Worker = &MockServer{}
var _ Client = &MockServer{}

func NewMockServer() *MockServer {
	return &MockServer{
		lock: &sync.Mutex{},
		jobs: make(map[string]Job),
	}
}

func (mc *MockServer) AddServers(...Conn) {

}

func (mc *MockServer) AddHandler(name string, job Job) Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	mc.jobs[name] = job

	return mc
}

func (mc *MockServer) DelHandler(name string) Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	delete(mc.jobs, name)

	return mc
}

func (mc *MockServer) DelAllHandlers() Worker {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	mc.jobs = make(map[string]Job)

	return mc
}

func (mc *MockServer) GetHandler(name string) Job {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	return mc.jobs[name]
}

func (mc *MockServer) Receivers() (<-chan Message, context.Context) {
	return nil, nil
}

func (mc *MockServer) Close() error {
	return nil
}

func (mc *MockServer) Submit(Task) Task {

}

func (mc *MockServer) assignTask(tid TaskID) {

}

func (mc *MockServer) getTask(TaskID) Task {

}

func (mc *MockServer) extractTask(TaskID) Task {

}

func (mc *MockServer) receivers() (<-chan Message, context.Context) {
	return nil, nil
}
