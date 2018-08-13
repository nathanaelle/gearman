package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
)

type (
	Worker interface {
		AddServers(...Conn) Worker
		AddHandler(string, Job) Worker
		DelHandler(string) Worker
		DelAllHandlers() Worker
		GetHandler(string) Job
		Receivers() (<-chan Message, context.Context)
		Close() error
	}

	worker struct {
		pool
		handlers map[string]Job
		m_queue  <-chan Message
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
	w.m_queue = q
	w.handlers = make(map[string]Job)
	w.pool.new(q, ctx)

	go workerLoop(w, debug)

	return w
}

// AddServers add a pool of servers to a Worker
func (w *worker) AddServers(servers ...Conn) Worker {
	for _, server := range servers {
		w.addServer(server)
	}
	return w
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
	return w.m_queue, w.ctx
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

func workData(reply chan<- Packet, tid TaskID) io.Writer {
	return workWriter(func(p []byte) (n int, err error) {
		reply <- BuildPacket(WORK_DATA_WRK, tid, Opacify(p))
		return len(p), nil
	})
}

func workComplete(reply chan<- Packet, tid TaskID) io.Writer {
	return workWriter(func(p []byte) (n int, err error) {
		reply <- BuildPacket(WORK_COMPLETE_WRK, tid, Opacify(p))
		return len(p), nil
	})
}

func run(job Job, input io.Reader, reply chan<- Packet, tid TaskID) {
	res := new(bytes.Buffer)
	status, err := isolatedServe(job, input, res, workData(reply, tid))

	switch {
	case err == nil && status:
		reply <- BuildPacket(WORK_COMPLETE_WRK, tid, Opacify(res.Bytes()))

	case err == nil && !status:
		reply <- BuildPacket(WORK_FAIL_WRK, tid)

	case err != nil:
		reply <- BuildPacket(WORK_EXCEPTION_WRK, tid, Opacify([]byte(err.Error())))
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
			case NO_JOB:
				msg.Server.CounterAdd(-1)

			case NOOP:
				msg.Server.CounterAdd(2)
				msg.Reply <- grabJob
				msg.Reply <- grabJobUniq
				continue

			case ECHO_RES:
				debug(dbg, "WRKR\tECHO\t[%v]\n", msg.Pkt.At(0))

			case JOB_ASSIGN:
				msg.Server.CounterAdd(-1)
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					panic(err)
				}
				go run(w.GetHandler(string(msg.Pkt.At(1).Bytes())), bytes.NewReader(msg.Pkt.At(2).Bytes()), msg.Reply, tid)

			case JOB_ASSIGN_UNIQ:
				msg.Server.CounterAdd(-1)
				if err = tid.Cast(msg.Pkt.At(0)); err != nil {
					panic(err)
				}
				go run(w.GetHandler(string(msg.Pkt.At(1).Bytes())), bytes.NewReader(msg.Pkt.At(2).Bytes()), msg.Reply, tid)

			case ERROR:
				debug(dbg, "WRKR\tERR\t[%s] [%s]\n", msg.Pkt.At(0).Bytes(), string(msg.Pkt.At(1).Bytes()))
			default:
				debug(dbg, "WRKR\t%s\n", msg.Pkt)
			}

			if msg.Server.IsZeroCounter() {
				msg.Reply <- preSleep
			}

		case <-end.Done():
			return
		}
	}
}
