package protocol // import "github.com/nathanaelle/gearman/v2/protocol"

import (
	"errors"
	"fmt"
)

type (
	// UndefinedPacketError describe the presence of a packet that is undecodable because the command is unknown
	UndefinedPacketError struct {
		Cmd Command
	}

	// BorrowError a Command try to borrow a packet that is incompatible
	BorrowError struct {
		Cmd    Command
		Packet Packet
	}

	// PayloadLenError describe an inconsistency in a packet between the expected number of arguments and the given one
	PayloadLenError struct {
		Cmd         Command
		ExpectedLen int
		GivenLen    int
	}
)

var (
	// ErrBuffTooSmall …
	ErrBuffTooSmall error = errors.New("Buffer is Too Small")

	// ErrPayloadInEmptyPacket is produced when a Packet is expected to be empty and found to have arguments
	ErrPayloadInEmptyPacket error = errors.New("Found payload in expected empty packet")

	// ErrCastOpaqueAsOpaque the error is produced when an already Opaque data is casted to become Opaque
	ErrCastOpaqueAsOpaque error = errors.New("Can't cast Opaque as Opaque")
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
