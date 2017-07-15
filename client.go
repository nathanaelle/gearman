package	gearman // import "github.com/nathanaelle/gearman"

import	(
	"log"
	"context"
)

type	(
	Client	interface {
		AddServers(...Conn)	Client
		Submit(Task)		Task
		Close()			error

		assignTask(tid TaskID)
		getTask(TaskID)		Task
		extractTask(TaskID)	Task
		receivers() 		(<-chan Message, context.Context)
	}
)


func	client_loop(c Client, dbg *log.Logger) {
	var tid TaskID
	var err	error

	m_q,ctx	:= c.receivers()

	for	{
		select	{
		case	msg, done := <-m_q:
			if msg.Pkt == nil {
				if done {
					return
				}
				debug(dbg, "CLI CORRUPTED MESSAGE \t%#v\n", msg)
				continue
			}

			debug(dbg, "CLI\t%s\n",msg.Pkt)
			switch	msg.Pkt.Cmd() {
			case	NOOP:

			case	ECHO_RES:
				debug(dbg, "CLI\tECHO [%s]\n", string(msg.Pkt.At(0).Bytes()) )

			case	ERROR:
				debug(dbg, "CLI\tERR [%s] [%s]\n",msg.Pkt.At(0).Bytes(),string(msg.Pkt.At(1).Bytes()))

			case	JOB_CREATED:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					debug(dbg, "CLI\tJOB_CREATED TID [%s] err : %v\n", string(msg.Pkt.At(0).Bytes()), err )
					panic(err)
				}
				c.assignTask(tid)


			case	WORK_DATA, WORK_WARNING, WORK_STATUS:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					debug(dbg, "CLI\t%s TID [%s] err : %v\n", msg.Pkt.Cmd(), string(msg.Pkt.At(0).Bytes()), err )
					panic(err)
				}

				c.getTask(tid).Handle(msg.Pkt)

			case	WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					debug(dbg, "CLI\t%s TID [%s] err : %v\n", msg.Pkt.Cmd(), string(msg.Pkt.At(0).Bytes()), err )
					panic(err)
				}

				c.extractTask(tid).Handle(msg.Pkt)

			case	STATUS_RES:
				panic("status_res not wrote")

			case	OPTION_RES:
				panic("option_res not wrote")

			default:
				debug(dbg, "CLI\t%s\n", msg.Pkt)
			}

		case	<-ctx.Done():
			return
		}
	}
}
