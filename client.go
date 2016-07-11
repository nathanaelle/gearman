package	gearman

import	(
	"log"
)

type	(
	Client	interface {
		AddServers(...Conn)	Client
		Submit(Task)		Task
		AssignTask(tid TaskID)
		GetTask(TaskID)		Task
		ExtractTask(TaskID)	Task
		Receivers() 		(<-chan Message,<-chan struct{})
		Close()			error
	}
)


func	client_loop(c Client, dbg *log.Logger) {
	m_q,end	:= c.Receivers()

	for	{
		select	{
		case	msg := <-m_q:
			debug(dbg, "CLI\t%s\n",msg.Pkt)
			switch	msg.Pkt.Cmd() {
			case	NOOP:

			case	ECHO_RES:
				debug(dbg, "CLI\tECHO [%s]\n",string(msg.Pkt.At(0)))

			case	ERROR:
				debug(dbg, "CLI\tERR [%s] [%s]\n",msg.Pkt.At(0),string(msg.Pkt.At(1)))

			case	JOB_CREATED:
				tid,err	:= slice2TaskID(msg.Pkt.At(0))
				if err != nil {
					panic(err)
				}
				c.AssignTask(tid)


			case	WORK_DATA, WORK_WARNING, WORK_STATUS:
				tid,err	:= slice2TaskID(msg.Pkt.At(0))
				if err != nil {
					panic(err)
				}

				c.GetTask(tid).Handle(msg.Pkt)

			case	WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION:
				tid,err	:= slice2TaskID(msg.Pkt.At(0))
				if err != nil {
					panic(err)
				}

				c.ExtractTask(tid).Handle(msg.Pkt)

			case	STATUS_RES:
				panic("status_res not wrote")

			case	OPTION_RES:
				panic("option_res not wrote")

			default:
				debug(dbg, "CLI\t%s\n", msg.Pkt)
			}

		case	<-end:
			return
		}
	}
}
