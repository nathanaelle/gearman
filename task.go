package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"context"
	"io"
)

type (
	// Task describe a requested task by a Client
	Task interface {
		Handle(p Packet)
		Value() ([]byte, error)
		Reader() (io.Reader, error)
		Packet() Packet
		Done() <-chan struct{}
	}

	task struct {
		packet  Packet
		ctx     context.Context
		cancel  context.CancelFunc
		payload bytes.Buffer
		err     error
		statNum int
		statDen int
	}

	nullTask struct{}

	echoTask struct {
		packet  Packet
		ctx     context.Context
		cancel  context.CancelFunc
		payload bytes.Buffer
		err     error
	}
)

// NilTask is a task with no task
var NilTask Task = &nullTask{}

// NewTask create a Task from a command string and a payload
func NewTask(cmd string, payload []byte) Task {
	r := &task{
		packet: BuildPacket(SUBMIT_JOB, Opacify([]byte(cmd)), Opacify([]byte{}), Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

// NewTaskLow create a low priority Task from a command string and a payload
func NewTaskLow(cmd string, payload []byte) Task {
	r := &task{
		packet: BuildPacket(SUBMIT_JOB_LOW, Opacify([]byte(cmd)), Opacify([]byte{}), Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

// NewTaskHigh create a hig priority Task from a command string and a payload
func NewTaskHigh(cmd string, payload []byte) Task {
	r := &task{
		packet: BuildPacket(SUBMIT_JOB_HIGH, Opacify([]byte(cmd)), Opacify([]byte{}), Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

func (r *task) Done() <-chan struct{} {
	return r.ctx.Done()
}

func (r *task) Packet() Packet {
	return r.packet
}

func (r *task) Handle(p Packet) {
	switch p.Cmd() {
	case WORK_COMPLETE:
		r.payload.Write(p.At(1).Bytes())
		r.cancel()

	case WORK_FAIL:
		r.err = ErrUnknown
		r.cancel()

	case WORK_EXCEPTION:
		r.err = &ExceptionError{p.At(1).Bytes()}
		r.cancel()

	case WORK_DATA:
		r.payload.Write(p.At(1).Bytes())

	case WORK_STATUS:
		// TODO

	case WORK_WARNING:
		// TODO

	}
}

func (r *task) Value() ([]byte, error) {
	<-r.ctx.Done()

	return r.payload.Bytes(), r.err
}

func (r *task) Reader() (io.Reader, error) {
	<-r.ctx.Done()

	return bytes.NewReader(r.payload.Bytes()), r.err
}

func (nt *nullTask) Handle(_ Packet) {
}

func (nt *nullTask) Value() ([]byte, error) {
	return []byte{}, nil
}

func (nt *nullTask) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (nt *nullTask) Packet() Packet {
	return emptyEchoPacket
}

func (nt *nullTask) Reader() (io.Reader, error) {
	return bytes.NewReader([]byte{}), nil
}

// EchoTask returns a Task for an Echo Request
func EchoTask(payload []byte) Task {
	r := &echoTask{
		packet: BuildPacket(ECHO_REQ, Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

func (r *echoTask) Done() <-chan struct{} {
	return r.ctx.Done()
}

func (r *echoTask) Handle(p Packet) {
	switch p.Cmd() {
	case ECHO_RES:
		r.payload.Write(p.At(0).Bytes())
		r.cancel()

	default:
		r.err = ErrUnknown
		r.cancel()
	}
}

func (r *echoTask) Value() ([]byte, error) {
	<-r.ctx.Done()

	return r.payload.Bytes(), r.err
}

func (r *echoTask) Packet() Packet {
	return r.packet
}

func (r *echoTask) Reader() (io.Reader, error) {
	<-r.ctx.Done()

	return bytes.NewReader(r.payload.Bytes()), r.err
}
