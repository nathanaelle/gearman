// +build ignore

package	gearman


import	(
	"log"
)


type	Client	struct {
	pool
	m_queue		chan message
	running_jobs	map[[]byte]chan
}


// create a new Client
// r_end is a channel to signal to the Client to end the process
func NewClient(r_end <-chan bool, debug *log.Logger)*Client{
	c		:= new(Client)
	c.m_queue	= make(chan message,10)
	c.running_jobs	= make(map[[]byte]string)
	c.pool.new(c.m_queue, r_end)

	go c.loop(debug)

	return c
}


//	Add a list of gearman server
//	the gearman
func (c *Client)AddServers(servers ...string) (*Client) {
	for _,server := range servers {
		c.add_server(server)
	}
	return c
}


func (c *Client)loop(debug *log.Logger) {
	for {
		select {
		case msg := <- w.m_queue:
			debug(dbg, "CMD=[%2x] SIZE=[%d]\n",msg.pkt.Header.Cmd,uint64(msg.pkt.Header.Size))
			switch msg.pkt.Header.Cmd {
			case	NOOP:
				w.send(msg.reply,GRAB_JOB,[][]byte{})

			case	ECHO_RES:
				debug(dbg, "EKO=[%s]\n",string(msg.pkt.payload[0]))
				w.send(msg.reply,GRAB_JOB,[][]byte{})

			case	ERROR:
				debug(dbg, "ERR=[%s] [%s]\n",msg.pkt.payload[0],string(msg.pkt.payload[1]))
				w.send(msg.reply,GRAB_JOB,[][]byte{})

			case	JOB_CREATED:

			case	WORK_DATA, WORK_WARNING, WORK_STATUS, WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION:

			case	STATUS_RES:

			case	OPTION_RES:

			default:
				debug(dbg, "CMD=[%d] !!=[%#v]\n",msg.pkt.Header.Cmd,msg.pkt.payload)
				w.send(msg.reply,GRAB_JOB,[][]byte{})

			}

		case <- w.r_end:
			return
		}
	}
}
