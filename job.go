package gearman // import "github.com/nathanaelle/gearman"

import (
	"errors"
	"io"
)

type (
	// Job describe a job exposed by Worker
	Job interface {
		Serve(payload io.Reader, reply, data io.Writer, progress chan<- int) (success bool, err error)
	}

	// JobHandler is a light version of a Job
	JobHandler func(payload io.Reader, reply io.Writer) (err error)

	// LongJobHandler is the heavy version (and maybe long in time) of a Job
	LongJobHandler func(payload io.Reader, reply, data io.Writer, progress chan<- int) (success bool, err error)
)

// FailJob is a Failed Job
var FailJob Job = JobHandler(func(payload io.Reader, reply io.Writer) (err error) {
	return errors.New("job doesn't exist")
})

// Serve is the the implementation of Job.Serve
func (jh JobHandler) Serve(payload io.Reader, reply, data io.Writer, progress chan<- int) (success bool, err error) {
	close(progress)

	err = jh(payload, reply)
	if err == nil {
		return true, nil
	}
	return false, err
}

// Serve is the the implementation of Job.Serve
func (jh LongJobHandler) Serve(payload io.Reader, reply io.Writer, data io.Writer, progress chan<- int) (success bool, err error) {
	defer close(progress)

	return jh(payload, reply, data, progress)
}
