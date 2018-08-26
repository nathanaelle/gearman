package gearman // import "github.com/nathanaelle/gearman"

import (
	"context"
	"log"
	"sync"
)

type (
	rrServer struct {
		ctx      context.Context
		debug    *log.Logger
		pool     []Client
		climutex *sync.Mutex
		idx      int
	}
)

// RoundRobinClient creates a new (Client)[#Client]
func RoundRobinClient(ctx context.Context, debug *log.Logger) Client {
	c := new(rrServer)
	c.debug = debug
	c.ctx = ctx
	c.climutex = new(sync.Mutex)
	c.idx = 0

	return c
}

func (c *rrServer) receivers() (<-chan Message, context.Context) {
	return nil, nil
}

func (c *rrServer) Close() error {
	c.climutex.Lock()
	defer c.climutex.Unlock()

	for _, server := range c.pool {
		server.Close()
	}
	return nil
}

//	Add a list of gearman server
func (c *rrServer) AddServers(servers ...Conn) {
	c.climutex.Lock()
	defer c.climutex.Unlock()

	for _, server := range servers {
		ssc := SingleServerClient(c.ctx, c.debug)
		ssc.AddServers(server)
		c.pool = append(c.pool, ssc)

	}
}

func (c *rrServer) Submit(req Task) Task {
	c.climutex.Lock()
	defer c.climutex.Unlock()

	cli := c.pool[c.idx]
	c.idx = (c.idx + 1) % (len(c.pool))

	return cli.Submit(req)
}

func (c *rrServer) assignTask(tid TaskID) {
}

func (c *rrServer) getTask(tid TaskID) Task {
	return nil
}

func (c *rrServer) extractTask(tid TaskID) Task {
	return nil
}
