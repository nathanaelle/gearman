package protocol

import (
	"errors"
	"fmt"
)

type (
	UndefinedPacketError struct {
		Cmd Command
	}

	BorrowError struct {
		Cmd    Command
		Packet Packet
	}

	PayloadLenError struct {
		Cmd         Command
		ExpectedLen int
		GivenLen    int
	}
)

var (
	ErrBuffTooSmall         error = errors.New("Buffer is Too Small")
	ErrPayloadInEmptyPacket error = errors.New("Found payload in expected empty packet")
	ErrCastOpaqueAsOpaque   error = errors.New("Can't cast Opaque as Opaque")
)

func (e *UndefinedPacketError) Error() string {
	return fmt.Sprintf("%v is undefined", e.Cmd)
}

func (e *BorrowError) Error() string {
	return fmt.Sprintf("%v can't borrow from %v", e.Cmd, e.Packet)
}

func (e *PayloadLenError) Error() string {
	return fmt.Sprintf("[%d] items Required for [%v] payload but got [%v] items", e.ExpectedLen, e.Cmd, e.GivenLen)
}
