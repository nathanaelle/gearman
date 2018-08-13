package gearman // import "github.com/nathanaelle/gearman"

import (
	"errors"
	"fmt"
)

type (
	ExceptionError struct {
		Payload []byte
	}

	UndefinedPacketError struct {
		Cmd Command
	}

	PayloadLenError struct {
		Cmd         Command
		ExpectedLen int
		GivenLen    int
	}

	BorrowError struct {
		Cmd    Command
		Packet Packet
	}
)

var (
	ErrUnknown              error = errors.New("Unspecified Error happens")
	ErrBuffTooSmall         error = errors.New("Buffer is Too Small")
	ErrPayloadInEmptyPacket error = errors.New("Found payload in expected empty packet")
	ErrTextProtocol         error = errors.New("Text Protocol is unsupported")
	ErrCastOpaqueAsOpaque   error = errors.New("Can't cast Opaque as Opaque")
)

func (e *ExceptionError) Error() string {
	return fmt.Sprintf("Error [%x]", e.Payload)
}

func (e *PayloadLenError) Error() string {
	return fmt.Sprintf("[%d] items Required for [%v] payload but got [%v] items", e.ExpectedLen, e.Cmd, e.GivenLen)
}

func (e *UndefinedPacketError) Error() string {
	return fmt.Sprintf("%v is undefined", e.Cmd)
}

func (e *BorrowError) Error() string {
	return fmt.Sprintf("%v can't borrow from %v", e.Cmd, e.Packet)
}
