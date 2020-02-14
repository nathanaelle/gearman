package gearman // import "github.com/nathanaelle/gearman/v2"

import (
	"bytes"
	"context"
	"io"

	"github.com/nathanaelle/gearman/v2/protocol"
)

type (
	// Task describe a requested task by a Client
	Task interface {
		Handle(p protocol.Packet)
		Value() ([]byte, error)
		Reader() (io.Reader, error)
		Packet() protocol.Packet
		Done() <-chan struct{}
	}

	task struct {
		packet  protocol.Packet
		ctx     context.Context
		cancel  context.CancelFunc
		payload bytes.Buffer
		err     error
		statNum int
		statDen int
	}

	nullTask struct{}

	echoTask struct {
		packet  protocol.Packet
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
		packet: protocol.BuildPacket(protocol.SubmitJob, protocol.Opacify([]byte(cmd)), protocol.Opacify([]byte{}), protocol.Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

// NewTaskLow create a low priority Task from a command string and a payload
func NewTaskLow(cmd string, payload []byte) Task {
	r := &task{
		packet: protocol.BuildPacket(protocol.SubmitJobLow, protocol.Opacify([]byte(cmd)), protocol.Opacify([]byte{}), protocol.Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

// NewTaskHigh create a hig priority Task from a command string and a payload
func NewTaskHigh(cmd string, payload []byte) Task {
	r := &task{
		packet: protocol.BuildPacket(protocol.SubmitJobHigh, protocol.Opacify([]byte(cmd)), protocol.Opacify([]byte{}), protocol.Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

func (r *task) Done() <-chan struct{} {
	return r.ctx.Done()
}

func (r *task) Packet() protocol.Packet {
	return r.packet
}

func (r *task) Handle(p protocol.Packet) {
	switch p.Cmd() {
	case protocol.WorkComplete:
		r.payload.Write(p.At(1).Bytes())
		r.cancel()

	case protocol.WorkFail:
		r.err = ErrWorkFail
		r.cancel()

	case protocol.WorkException:
		r.err = &ExceptionError{p.At(1).Bytes()}
		r.cancel()

	case protocol.WorkData:
		r.payload.Write(p.At(1).Bytes())

	case protocol.WorkStatus:
		// TODO

	case protocol.WorkWarning:
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

func (nt *nullTask) Handle(_ protocol.Packet) {
}

func (nt *nullTask) Value() ([]byte, error) {
	return []byte{}, nil
}

func (nt *nullTask) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (nt *nullTask) Packet() protocol.Packet {
	return protocol.PktEmptyEchoPacket
}

func (nt *nullTask) Reader() (io.Reader, error) {
	return bytes.NewReader([]byte{}), nil
}

// EchoTask returns a Task for an Echo Request
func EchoTask(payload []byte) Task {
	r := &echoTask{
		packet: protocol.BuildPacket(protocol.EchoReq, protocol.Opacify(payload)),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

func (r *echoTask) Done() <-chan struct{} {
	return r.ctx.Done()
}

func (r *echoTask) Handle(p protocol.Packet) {
	switch p.Cmd() {
	case protocol.EchoRes:
		r.payload.Write(p.At(0).Bytes())
		r.cancel()

	default:
		r.err = &IncoherentError{protocol.EchoRes, p.Cmd()}
		r.cancel()
	}
}

func (r *echoTask) Value() ([]byte, error) {
	<-r.ctx.Done()

	return r.payload.Bytes(), r.err
}

func (r *echoTask) Packet() protocol.Packet {
	return r.packet
}

func (r *echoTask) Reader() (io.Reader, error) {
	<-r.ctx.Done()

	return bytes.NewReader(r.payload.Bytes()), r.err
}
