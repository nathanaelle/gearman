package	gearman

import	(
	"io"
	"sync"
	"bytes"
	"errors"
)



type	(
	TaskID	[64]byte

	Task	interface {
		Handle(p Packet)
		Value() ([]byte,error)
		Reader() (io.Reader,error)
		Packet() Packet
	}

	task	struct {
		packet		Packet
		solved		*sync.WaitGroup
		payload		bytes.Buffer
		err		error
	}

	nullTask struct{}
)


var	NilTask	Task	= &nullTask{}


func	slice2TaskID(d []byte) (tid TaskID, err error) {
	if len(d) > 64 {
		err = errors.New("tid too long")
		return
	}

	copy(tid[:], d)
	return
}




func NewTask(cmd string, payload []byte) Task {
	r := &task {
		packet:	packet(SUBMIT_JOB, []byte(cmd), []byte{},payload),
		solved:	new(sync.WaitGroup),
	}

	r.solved.Add(1)
	return	r
}


func (r *task) Packet() Packet {
	return r.packet
}


func (r *task) Handle(p Packet) {
	switch p.Cmd() {
	case	WORK_COMPLETE:
		r.payload.Write(p.At(1))
		r.solved.Done()

	case	WORK_FAIL:
		r.err = unknownError
		r.solved.Done()

	case	WORK_EXCEPTION:
		r.err = &ExceptionError { p.At(1) }
		r.solved.Done()
	}
}


func (r *task) Value() ([]byte,error) {
	r.solved.Wait()

	return r.payload.Bytes(), r.err
}


func (r *task) Reader() (io.Reader,error) {
	r.solved.Wait()

	return bytes.NewReader( r.payload.Bytes() ), r.err
}


func (_ *nullTask) Handle(_ Packet) {
}


func (_ *nullTask) Value() ([]byte,error) {
	return []byte{},nil
}

func (_ *nullTask) Packet() Packet {
	return empty_echo_packet
}


func (_ *nullTask) Reader() (io.Reader,error) {
	return bytes.NewReader( []byte{} ), nil
}
