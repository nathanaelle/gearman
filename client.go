package	gearman // import "github.com/nathanaelle/gearman"

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
	var tid TaskID
	var err	error

	m_q,end	:= c.Receivers()

	for	{
		select	{
		case	msg := <-m_q:
			debug(dbg, "CLI\t%s\n",msg.Pkt)
			switch	msg.Pkt.Cmd() {
			case	NOOP:

			case	ECHO_RES:
				debug(dbg, "CLI\tECHO [%s]\n", string(msg.Pkt.At(0).Bytes()) )

			case	ERROR:
				debug(dbg, "CLI\tERR [%s] [%s]\n",msg.Pkt.At(0).Bytes(),string(msg.Pkt.At(1).Bytes()))

			case	JOB_CREATED:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					panic(err)
				}
				c.AssignTask(tid)


			case	WORK_DATA, WORK_WARNING, WORK_STATUS:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					panic(err)
				}

				c.GetTask(tid).Handle(msg.Pkt)

			case	WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
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
