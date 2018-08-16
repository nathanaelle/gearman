package gearman // import "github.com/nathanaelle/gearman"

import (
	"context"
	"log"
	"sync"
)

type (
	singleServer struct {
		pool
		configured bool
		jobs       map[string]Task
		mQueue     chan Message
		reqQueue   []Task
		climutex   *sync.Mutex
	}
)

// SingleServerClient creates a new (Client)[#Client]
func SingleServerClient(ctx context.Context, debug *log.Logger) Client {
	c := new(singleServer)
	c.mQueue = make(chan Message, 10)
	c.jobs = make(map[string]Task)
	c.climutex = new(sync.Mutex)
	c.pool.newPool(ctx, c.mQueue)

	go clientLoop(c, debug)

	return c
}

func (c *singleServer) receivers() (<-chan Message, context.Context) {
	return c.mQueue, c.ctx
}

func (c *singleServer) Close() error {
	return nil
}

//	Add a list of gearman server
func (c *singleServer) AddServers(servers ...Conn) Client {
	if c.configured || len(servers) == 0 {
		return c
	}

	if len(servers) > 1 {
		servers = servers[0:1]
	}

	c.configured = true

	for _, server := range servers {
		c.addServer(server)
	}
	return c
}

func (c *singleServer) Submit(req Task) Task {
	c.climutex.Lock()
	defer c.climutex.Unlock()

	c.reqQueue = append(c.reqQueue, req)

	for _, s := range c.listServers() {
		c.sendTo(s, req.Packet())
	}

	return req
}

func (c *singleServer) assignTask(tid TaskID) {
	c.climutex.Lock()
	defer c.climutex.Unlock()

	c.jobs[tid.String()] = c.reqQueue[0]
	c.reqQueue = c.reqQueue[1:]
}

func (c *singleServer) getTask(tid TaskID) Task {
	c.climutex.Lock()
	defer c.climutex.Unlock()

	if res, ok := c.jobs[tid.String()]; ok {
		return res
	}
	return NilTask
}

func (c *singleServer) extractTask(tid TaskID) Task {
	c.climutex.Lock()
	defer c.climutex.Unlock()

	sTID := tid.String()
	if res, ok := c.jobs[sTID]; ok {
		delete(c.jobs, sTID)
		return res
	}
	return NilTask
}
