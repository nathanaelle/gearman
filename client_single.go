package	gearman	// import "github.com/nathanaelle/gearman"

import	(
	"log"
)

type	(
	singleServer	struct {
		configured	bool
		pool
		jobs		map[string]Task
		m_queue		chan Message
		r_q		[]Task
	}
)


// create a new Client
// r_end is a channel to signal to the Client to end the process
func SingleServerClient(r_end <-chan struct{}, debug *log.Logger) Client {
	c		:= new(singleServer)
	c.m_queue	= make(chan Message,10)
	c.jobs		= make(map[string]Task)
	c.pool.new(c.m_queue, r_end)

	go client_loop(c,debug)

	return c
}


func (c *singleServer)Receivers() (<-chan Message,<-chan struct{}) {
	return	c.m_queue, c.r_end
}


func (c *singleServer)Close() error {
	return	nil
}


//	Add a list of gearman server
func (c *singleServer)AddServers(servers ...Conn) Client {
	if c.configured || len(servers) == 0 {
		return	c
	}

	if len(servers) > 1 {
		servers = servers[0:1]
	}

	c.configured = true

	for _,server := range servers {
		c.add_server(server)
	}
	return	c
}


func (c *singleServer)Submit(req Task) Task {
	c.r_q	= append(c.r_q, req)

	for _,s := range c.list_servers() {
		c.send_to(s, req.Packet())
	}

	return	req
}


func (c *singleServer)AssignTask(tid TaskID) {
	c.jobs[tid.String()]	= c.r_q[0]
	c.r_q			= c.r_q[1:]
}


func (c *singleServer)GetTask(tid TaskID) Task {
	if res,ok := c.jobs[tid.String()]; ok {
		return	res
	}
	return	NilTask
}


func (c *singleServer)ExtractTask(tid TaskID) Task {
	s_tid := tid.String()
	if res,ok := c.jobs[s_tid]; ok {
		delete(c.jobs, s_tid)
		return	res
	}
	return	NilTask
}
