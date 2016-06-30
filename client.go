package	gearman

import	(
	"log"
)

type	(
	Client	interface {
		AddServers(...Conn)	Client
		Submit(Task)		Task
		MessageQueue()		<-chan message
		AssignTask(tid TaskID)
		GetTask(TaskID)		Task
		ExtractTask(TaskID)	Task
		EndSignal()		<-chan struct{}
	}

	singleServer	struct {
		configured	bool
		pool
		jobs		map[TaskID]Task
		m_queue		chan message
		r_q		[]Task
	}
)


// create a new Client
// r_end is a channel to signal to the Client to end the process
func SingleServerClient(r_end <-chan struct{}, debug *log.Logger) Client {
	c		:= new(singleServer)
	c.m_queue	= make(chan message,10)
	c.jobs		= make(map[TaskID]Task)
	c.pool.new(c.m_queue, r_end)

	go client_loop(c,debug)

	return c
}


func (c *singleServer)MessageQueue() <-chan message {
	return c.m_queue
}

func (c *singleServer)EndSignal() <-chan struct{} {
	return c.r_end
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
	c.jobs[tid]	= c.r_q[0]
	c.r_q		= c.r_q[1:]
}


func (c *singleServer)GetTask(tid TaskID) Task {
	if res,ok := c.jobs[tid]; ok {
		return	res
	}
	return	NilTask
}


func (c *singleServer)ExtractTask(tid TaskID) Task {
	if res,ok := c.jobs[tid]; ok {
		delete(c.jobs, tid)
		return	res
	}
	return	NilTask
}

func	client_loop(c Client,dbg *log.Logger) {
	mq	:= c.MessageQueue()
	end	:= c.EndSignal()

	for	{
		select	{
		case	msg := <-mq:
			debug(dbg, "CLI\t%s\n",msg.pkt)
			switch	msg.pkt.Cmd() {
			case	NOOP:

			case	ECHO_RES:
				debug(dbg, "CLI\tECHO [%s]\n",string(msg.pkt.At(0)))

			case	ERROR:
				debug(dbg, "CLI\tERR [%s] [%s]\n",msg.pkt.At(0),string(msg.pkt.At(1)))

			case	JOB_CREATED:
				tid,err	:= slice2TaskID(msg.pkt.At(0))
				if err != nil {
					panic(err)
				}
				c.AssignTask(tid)


			case	WORK_DATA, WORK_WARNING, WORK_STATUS:
				tid,err	:= slice2TaskID(msg.pkt.At(0))
				if err != nil {
					panic(err)
				}

				c.GetTask(tid).Handle(msg.pkt)

			case	WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION:
				tid,err	:= slice2TaskID(msg.pkt.At(0))
				if err != nil {
					panic(err)
				}

				c.ExtractTask(tid).Handle(msg.pkt)

			case	STATUS_RES:
				panic("status_res not wrote")

			case	OPTION_RES:
				panic("option_res not wrote")

			default:
				debug(dbg, "CLI\t%s\n", msg.pkt)
			}

		case	<-end:
			return
		}
	}
}
