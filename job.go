package	gearman // import "github.com/nathanaelle/gearman"

import	(
	"io"
	"errors"
)


type	(
	Job	interface {
		// this describe a Job
		Serve(payload io.Reader, reply, data io.Writer, progress chan<- int) (success bool, err error)
	}

	JobHandler	func(payload io.Reader,reply io.Writer) (err error)
)


var FailJob JobHandler = func(payload io.Reader,reply io.Writer) (err error) {
	return errors.New("job doesn't exist")
}


func (jh JobHandler) Serve(payload io.Reader, reply, data io.Writer, progress chan<- int) (success bool, err error) {
	close(progress)

	err = jh(payload,reply)
	if err == nil {
		return true, nil
	}
	return false, err
}



type	LongJobHandler	func(payload io.Reader, reply, data io.Writer, progress chan<- int) (success bool, err error)

func (jh LongJobHandler) Serve(payload io.Reader, reply io.Writer, data io.Writer, progress chan<- int) (success bool, err error) {
	defer close(progress)

	return jh(payload,reply,data,progress)
}
