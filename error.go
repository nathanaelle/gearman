package	gearman // import "github.com/nathanaelle/gearman"

import	(
	"fmt"
	"errors"
)


type	(
	ExceptionError struct {
		Payload	[]byte
	}

	UndefinedPacketError struct {
		Cmd	Command
	}


	PayloadLenError struct {
		Cmd		Command
		ExpectedLen	int
		GivenLen	int
	}

)

var	(
	unknownError			error	= errors.New("Unspecified Error happens")
	BuffTooSmallError		error	= errors.New("Buffer is Too Small")
	PayloadInEmptyPacketError	error	= errors.New("Found payload in expected empty packet")
	TextProtocolError		error	= errors.New("Text Protocol is unsupported")
)


func (e *ExceptionError)Error() string {
	return	fmt.Sprintf("Error [%x]", e.Payload)
}

func (e *PayloadLenError)Error() string {
	return	fmt.Sprintf("[%d] items Required for [%v] payload but got [%v] items", e.ExpectedLen, e.Cmd, e.GivenLen)
}

func (e *UndefinedPacketError)Error() string {
	return	fmt.Sprintf("%v is undefined", e.Cmd)
}
