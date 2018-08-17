package gearman // import "github.com/nathanaelle/gearman"

import (
	"errors"
	"fmt"
)

type (
	ExceptionError struct {
		Payload []byte
	}
)

var (
	ErrUnknown      error = errors.New("Unspecified Error happens")
	ErrTextProtocol error = errors.New("Text Protocol is unsupported")
)

func (e *ExceptionError) Error() string {
	return fmt.Sprintf("Error [%x]", e.Payload)
}
