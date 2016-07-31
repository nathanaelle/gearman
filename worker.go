package	gearman

import	(
	"io"
	"log"
	"bytes"
	"errors"
)

type	(

	Worker	interface {
		AddServers(...Conn) Worker
		AddHandler(string, Job) Worker
		GetHandler(string) Job
		Receivers() (<-chan Message,<-chan struct{})
		Close() error
	}


	worker	struct {
		pool
		handlers	map[string]Job
		m_queue		<-chan Message
	}

	work_writer func([]byte) (int, error)

)


func (f work_writer) Write(p []byte) (int, error) {
	return f(p)
}


// create a new Worker
// r_end is a channel to signal to the Worker to end the process
func NewWorker(r_end <-chan struct{}, debug *log.Logger) Worker{
	q		:= make(chan Message,100)
	w		:= new(worker)
	w.m_queue	= q
	w.handlers	= make(map[string]Job)
	w.pool.new(q, r_end)

	go worker_loop(w, debug)

	return w
}


//	Add a list of gearman server
//	the gearman
func (w *worker)AddServers(servers ...Conn) Worker {
	for _,server := range servers {
		w.add_server(server)
	}
	return w
}


//	Add a Job to a generic Worker
func (w *worker)AddHandler(name string,f Job) Worker {
	w.Lock()
	w.handlers[name] = f
	w.Unlock()

	w.add_handler(name)
	return w
}


func (w *worker)Close() error {
	return	nil
}

func (w *worker)Receivers() (<-chan Message,<-chan struct{}) {
	return	w.m_queue,w.r_end
}



func (w *worker)GetHandler(name string) Job {
	w.Lock()
	defer	w.Unlock()
	if job,  ok := w.handlers[name]; ok {
		return job
	}

	return	FailJob
}


func isolated_Serve(job Job, req io.Reader, res, data io.Writer) (status bool, err error) {
	defer func(){
		if r := recover(); r != nil {
			status	= false
			if e_r, ok := r.(error); ok {
				err = e_r
				return
			}
			if e_r, ok := r.(string); ok {
				err = errors.New(e_r)
				return
			}
			panic(r)
		}
	}()

	status, err = job.Serve(req, res, data, make(chan int))
	return
}


func work_data(reply chan<- Packet, tid TaskID) io.Writer {
	return work_writer(func(p []byte) (n int, err error){
		reply <- BuildPacket(WORK_DATA_WRK, tid, Opacify(p))
		return len(p),nil
	})
}


func work_complete(reply chan<- Packet, tid TaskID)  io.Writer {
	return work_writer(func(p []byte) (n int, err error){
		reply <- BuildPacket(WORK_COMPLETE_WRK, tid, Opacify(p))
		return len(p),nil
	})
}



func run(job Job, input io.Reader, reply chan<- Packet, tid TaskID) {
	res	:= new(bytes.Buffer)
	status, err := isolated_Serve( job, input, res, work_data(reply, tid) )

	switch	{
	case	err == nil && status:
		reply <- BuildPacket(WORK_COMPLETE_WRK, tid, Opacify(res.Bytes()))

	case	err == nil && !status:
		reply <- BuildPacket(WORK_FAIL_WRK, tid)

	case	err != nil:
		reply <- BuildPacket(WORK_EXCEPTION_WRK, tid, Opacify([]byte(err.Error())))
	}
}


func	worker_loop(w Worker,dbg *log.Logger) {
	var tid TaskID
	var err error

	m_q,end	:= w.Receivers()

	for	{
		select	{
		case	msg := <- m_q:
			switch	msg.Pkt.Cmd() {
			case	NO_JOB:
				msg.Server.CounterAdd(-1)

			case	NOOP:
				msg.Server.CounterAdd(2)
				msg.Reply <- grab_job
				msg.Reply <- grab_job_uniq
				continue

			case	ECHO_RES:
				debug(dbg, "WRKR\tECHO\t[%v]\n", msg.Pkt.At(0))

			case	JOB_ASSIGN:
				msg.Server.CounterAdd(-1)
				if err = msg.Pkt.At(0).Cast(&tid); err != nil {
					panic(err)
				}
				go run(w.GetHandler(string(msg.Pkt.At(1).Bytes())), bytes.NewReader(msg.Pkt.At(2).Bytes()), msg.Reply, tid)

			case	JOB_ASSIGN_UNIQ:
				msg.Server.CounterAdd(-1)
				if err = msg.Pkt.At(0).Cast(&tid); err != nil {
					panic(err)
				}
				go run(w.GetHandler(string(msg.Pkt.At(1).Bytes())), bytes.NewReader(msg.Pkt.At(2).Bytes()), msg.Reply, tid)

			case	ERROR:
				debug(dbg, "WRKR\tERR\t[%s] [%s]\n", msg.Pkt.At(0).Bytes(), string(msg.Pkt.At(1).Bytes()))
			default:
				debug(dbg, "WRKR\t%s\n", msg.Pkt)
			}

			if msg.Server.IsZeroCounter() {
				msg.Reply <- pre_sleep
			}

		case	<- end:
			return
		}
	}
}
