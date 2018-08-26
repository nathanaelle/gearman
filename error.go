package gearman // import "github.com/nathanaelle/gearman"

import (
	"errors"
	"fmt"

	"github.com/nathanaelle/gearman/v2/protocol"
)

type (
	// ExceptionError is returned when a worker returns an Exception Packet
	ExceptionError struct {
		Payload []byte
	}

	// IncoherentError is returned when some packet is expected but another arrived
	IncoherentError struct {
		Expected protocol.Command
		Got      protocol.Command
	}
)

var (
	// ErrWorkFail is returned when a worker returns a Fail Packet
	ErrWorkFail error = errors.New("Work Failed")
)

func (e *ExceptionError) Error() string {
	return fmt.Sprintf("Error [%x]", e.Payload)
}

func (e *IncoherentError) Error() string {
	return fmt.Sprintf("Protocol Command – Expected [%v] Got [%v]", e.Expected, e.Got)
}
