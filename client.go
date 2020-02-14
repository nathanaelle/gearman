package gearman // import "github.com/nathanaelle/gearman/v2"

import (
	"context"
	"log"

	"github.com/nathanaelle/gearman/v2/protocol"
)

type (
	// Client define the exposed interface of a gearman Client
	Client interface {
		AddServers(...Conn)
		Submit(Task) Task
		Close() error

		AssignTask(TaskID)
		GetTask(TaskID) Task
		ExtractTask(TaskID) Task
		Receivers() (<-chan Message, context.Context)
	}
)

func clientLoop(c Client, dbg *log.Logger) {
	var tid TaskID
	var err error

	mQueue, ctx := c.Receivers()

	for {
		select {
		case msg, done := <-mQueue:
			if msg.Pkt == nil {
				if done {
					return
				}
				debug(dbg, "CLI CORRUPTED MESSAGE \t%#v\n", msg)
				continue
			}

			debug(dbg, "CLI\t%s\n", msg.Pkt)
			switch msg.Pkt.Cmd() {
			case protocol.Noop:

			case protocol.EchoRes:
				debug(dbg, "CLI\tECHO [%s]\n", string(msg.Pkt.At(0).Bytes()))

			case protocol.Error:
				debug(dbg, "CLI\tERR [%s] [%s]\n", msg.Pkt.At(0).Bytes(), string(msg.Pkt.At(1).Bytes()))

			case protocol.JobCreated:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					debug(dbg, "CLI\tprotocol.JobCreated TID [%s] err : %v\n", string(msg.Pkt.At(0).Bytes()), err)
					panic(err)
				}
				c.AssignTask(tid)

			case protocol.WorkData, protocol.WorkWarning, protocol.WorkStatus:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					debug(dbg, "CLI\t%s TID [%s] err : %v\n", msg.Pkt.Cmd(), string(msg.Pkt.At(0).Bytes()), err)
					panic(err)
				}

				c.GetTask(tid).Handle(msg.Pkt)

			case protocol.WorkComplete, protocol.WorkFail, protocol.WorkException:
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					debug(dbg, "CLI\t%s TID [%s] err : %v\n", msg.Pkt.Cmd(), string(msg.Pkt.At(0).Bytes()), err)
					panic(err)
				}

				c.ExtractTask(tid).Handle(msg.Pkt)

			case protocol.StatusRes:
				panic("status_res not wrote")

			case protocol.OptionRes:
				panic("option_res not wrote")

			default:
				debug(dbg, "CLI\t%s\n", msg.Pkt)
			}

		case <-ctx.Done():
			return
		}
	}
}
