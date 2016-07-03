package	gearman

import	(
	"fmt"
	"errors"
)


type	(
	ExceptionError struct {
		Payload	[]byte
	}

	RESQRequiredError struct {
		Cmd		Command
		Given		Hello
		Expected	Hello
	}

	RESRequiredError struct {
		Cmd	Command
		Hello	Hello
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
)

func (e *ExceptionError)Error() string {
	return	fmt.Sprintf("Error [%x]", e.Payload)
}

func (e *RESQRequiredError)Error() string {
	return	fmt.Sprintf("[%v] Required for [%v] but got [%v]", e.Expected, e.Cmd, e.Given)
}

func (e *PayloadLenError)Error() string {
	return	fmt.Sprintf("[%d] itemps Required for [%v] payload but got [%v] items", e.ExpectedLen, e.Cmd, e.GivenLen)
}
