package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"

	"github.com/nathanaelle/gearman/v2/protocol"
)

type (
	// Worker define the exposed interface of gearman worker
	Worker interface {
		AddServers(...Conn)
		AddHandler(string, Job) Worker
		DelHandler(string) Worker
		DelAllHandlers() Worker
		GetHandler(string) Job
		Receivers() (<-chan Message, context.Context)
		Close() error
	}

	worker struct {
		pool
		handlers  map[string]Job
		wMsgQueue <-chan Message
	}

	workWriter func([]byte) (int, error)
)

func (f workWriter) Write(p []byte) (int, error) {
	return f(p)
}

// NewWorker instanciate a Worker
func NewWorker(ctx context.Context, debug *log.Logger) Worker {
	q := make(chan Message, 100)
	w := new(worker)
	w.wMsgQueue = q
	w.handlers = make(map[string]Job)
	w.pool.newPool(ctx, q)

	go workerLoop(w, debug)

	return w
}

// AddServers add a pool of servers to a Worker
func (w *worker) AddServers(servers ...Conn) {
	for _, server := range servers {
		w.addServer(server)
	}
}

//	Add a Job to a generic Worker
func (w *worker) AddHandler(name string, f Job) Worker {
	w.Lock()
	w.handlers[name] = f
	w.Unlock()

	w.addHandler(name)
	return w
}

//	Del a Job from a generic Worker
func (w *worker) DelHandler(name string) Worker {
	w.Lock()
	delete(w.handlers, name)
	w.Unlock()

	w.delHandler(name)
	return w
}

//	Del all Job from a generic Worker
func (w *worker) DelAllHandlers() Worker {
	w.Lock()
	w.handlers = make(map[string]Job)
	w.Unlock()

	w.delAllHandlers()

	return w
}

func (w *worker) Close() error {
	return nil
}

func (w *worker) Receivers() (<-chan Message, context.Context) {
	return w.wMsgQueue, w.ctx
}

func (w *worker) GetHandler(name string) Job {
	w.Lock()
	defer w.Unlock()
	if job, ok := w.handlers[name]; ok {
		return job
	}

	return FailJob
}

func isolatedServe(job Job, req io.Reader, res, data io.Writer) (status bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			status = false
			if rErr, ok := r.(error); ok {
				err = rErr
				return
			}
			if rStr, ok := r.(string); ok {
				err = errors.New(rStr)
				return
			}
			panic(r)
		}
	}()

	status, err = job.Serve(req, res, data, make(chan int))
	return
}

func workData(reply chan<- protocol.Packet, tid TaskID) io.Writer {
	return workWriter(func(p []byte) (n int, err error) {
		reply <- protocol.BuildPacket(protocol.WorkDataWorker, tid, protocol.Opacify(p))
		return len(p), nil
	})
}

func workComplete(reply chan<- protocol.Packet, tid TaskID) io.Writer {
	return workWriter(func(p []byte) (n int, err error) {
		reply <- protocol.BuildPacket(protocol.WorkCompleteWorker, tid, protocol.Opacify(p))
		return len(p), nil
	})
}

func runWorker(job Job, input io.Reader, reply chan<- protocol.Packet, tid TaskID) {
	res := new(bytes.Buffer)
	status, err := isolatedServe(job, input, res, workData(reply, tid))

	switch {
	case err == nil && status:
		reply <- protocol.BuildPacket(protocol.WorkCompleteWorker, tid, protocol.Opacify(res.Bytes()))

	case err == nil && !status:
		reply <- protocol.BuildPacket(protocol.WorkFailWorker, tid)

	case err != nil:
		reply <- protocol.BuildPacket(protocol.WorkExceptionWorker, tid, protocol.Opacify([]byte(err.Error())))
	}
}

func workerLoop(w Worker, dbg *log.Logger) {
	var tid TaskID
	var err error

	msgQueue, end := w.Receivers()

	for {
		select {
		case msg := <-msgQueue:
			switch msg.Pkt.Cmd() {
			case protocol.NoJob:
				msg.Server.CounterAdd(-1)

			case protocol.Noop:
				msg.Server.CounterAdd(2)
				msg.Reply <- protocol.PktGrabJob
				msg.Reply <- protocol.PktGrabJobUniq
				continue

			case protocol.EchoRes:
				debug(dbg, "WRKR\tECHO\t[%v]\n", msg.Pkt.At(0))

			case protocol.JobAssign:
				msg.Server.CounterAdd(-1)
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					panic(err)
				}
				go runWorker(w.GetHandler(string(msg.Pkt.At(1).Bytes())), bytes.NewReader(msg.Pkt.At(2).Bytes()), msg.Reply, tid)

			case protocol.JobAssignUniq:
				msg.Server.CounterAdd(-1)
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					panic(err)
				}
				go runWorker(w.GetHandler(string(msg.Pkt.At(1).Bytes())), bytes.NewReader(msg.Pkt.At(2).Bytes()), msg.Reply, tid)

			case protocol.Error:
				debug(dbg, "WRKR\tERR\t[%s] [%s]\n", msg.Pkt.At(0).Bytes(), string(msg.Pkt.At(1).Bytes()))

			default:
				debug(dbg, "WRKR\t%s\n", msg.Pkt)
			}

			if msg.Server.IsZeroCounter() {
				msg.Reply <- protocol.PktPreSleep
			}

		case <-end.Done():
			return
		}
	}
}
