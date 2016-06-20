package	gearman

import	(
	"io"
	"log"
	"bytes"
	"errors"
)

type	Worker	struct {
	pool
	handlers	map[string]Job
	m_queue		<-chan message
}

// create a new Worker
// r_end is a channel to signal to the Worker to end the process
func NewWorker(r_end <-chan bool, debug *log.Logger)*Worker{
	q		:= make(chan message,10)
	w		:= new(Worker)
	w.m_queue	= q
	w.handlers	= make(map[string]Job)
	w.pool.new(q, r_end)

	go w.loop(debug)

	return w
}


//	Add a list of gearman server
//	the gearman
func (w *Worker)AddServers(servers ...Conn) (*Worker) {
	for _,server := range servers {
		w.add_server(server)
	}
	return w
}


//	Add a Job to a generic Worker
func (w *Worker)AddHandler(name string,f Job) (*Worker) {
	w.Lock()
	w.handlers[name] = f
	w.Unlock()

	w.add_handler(name)
	return w
}


func (w *Worker)get_handler(name string) Job {
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


func (w *Worker)run(msg message) {
	name 	:= string(msg.pkt.At(1))

	res	:= new(bytes.Buffer)

	status, err := isolated_Serve( w.get_handler(name), bytes.NewReader(msg.pkt.At(2)), res, msg.work_data() )
	switch {
	case	err == nil && status:
		msg.reply(WORK_COMPLETE, res.Bytes() )

	case	err == nil && !status:
		msg.reply(WORK_FAIL)

	case	err != nil:
		msg.reply(WORK_EXCEPTION, []byte(err.Error()) )
	}
}


func (w *Worker)loop(dbg *log.Logger) {
	for {
		select {
		case	msg := <- w.m_queue:
			debug(dbg, "\t%s\n", msg.pkt)
			switch msg.pkt.Cmd() {
			case	NOOP:
				msg.pkt_reply(grab_job)

			case	NO_JOB:
				msg.pkt_reply(pre_sleep)

			case	JOB_ASSIGN:
				go w.run(msg)
				msg.pkt_reply(grab_job)

			case	ECHO_RES:
				debug(dbg, "EKO\t[%s]\n",string(msg.pkt.At(0)))
				msg.pkt_reply(grab_job)

			case	ERROR:
				debug(dbg, "ERR\t[%s] [%s]\n",msg.pkt.At(0),string(msg.pkt.At(1)))
				msg.pkt_reply(grab_job)

			default:
				debug(dbg, "!!\tCMD=[%d]\n",msg.pkt.Cmd())
				msg.pkt_reply(grab_job)
			}

		case <- w.r_end:
			return
		}
	}
}
