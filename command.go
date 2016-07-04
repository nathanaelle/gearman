package	gearman

import	(
	"fmt"
)

type	(
	Hello	uint32
	Command	uint32
)

const	(
	REQ	Hello	= 0x00524551
	RES	Hello	= 0x00524553
)

const	(
	HELLO_COMMAND_NONE	int	= 1<<iota
	HELLO_COMMAND_RES
	HELLO_COMMAND_REQ
)

const	(
	CAN_DO			Command	= 1	// REQ    Worker
	CANT_DO			Command	= 2	// REQ    Worker
	RESET_ABILITIES		Command	= 3	// REQ    Worker
	PRE_SLEEP		Command	= 4	// REQ    Worker
	NOOP			Command	= 6	// RES    Worker
	SUBMIT_JOB		Command	= 7	// REQ    Client
	JOB_CREATED		Command	= 8	// RES    Client
	GRAB_JOB		Command	= 9	// REQ    Worker
	NO_JOB			Command	= 10	// RES    Worker
	JOB_ASSIGN		Command	= 11	// RES    Worker
	WORK_STATUS		Command	= 12	// REQ    Worker		RES    Client
	WORK_COMPLETE		Command	= 13	// REQ    Worker		RES    Client
	WORK_FAIL		Command	= 14	// REQ    Worker		RES    Client
	GET_STATUS		Command	= 15	// REQ    Client
	ECHO_REQ		Command	= 16	// REQ    Client/Worker
	ECHO_RES		Command	= 17	// RES    Client/Worker
	SUBMIT_JOB_BG		Command	= 18	// REQ    Client
	ERROR			Command	= 19	// RES    Client/Worker
	STATUS_RES		Command	= 20	// RES    Client
	SUBMIT_JOB_HIGH		Command	= 21	// REQ    Client
	SET_CLIENT_ID		Command	= 22	// REQ    Worker
	CAN_DO_TIMEOUT		Command	= 23	// REQ    Worker
	ALL_YOURS		Command	= 24	// REQ    Worker
	WORK_EXCEPTION		Command	= 25	// REQ    Worker		RES    Client
	OPTION_REQ		Command	= 26	// REQ    Client/Worker
	OPTION_RES		Command	= 27	// RES    Client/Worker
	WORK_DATA		Command	= 28	// REQ    Worker		RES    Client
	WORK_WARNING		Command	= 29	// REQ    Worker		RES    Client
	GRAB_JOB_UNIQ		Command	= 30	// REQ    Worker
	JOB_ASSIGN_UNIQ		Command	= 31	// RES    Worker
	SUBMIT_JOB_HIGH_BG	Command	= 32	// REQ    Client
	SUBMIT_JOB_LOW		Command	= 33	// REQ    Client
	SUBMIT_JOB_LOW_BG	Command	= 34	// REQ    Client
	SUBMIT_JOB_SCHED	Command	= 35	// REQ    Client
	SUBMIT_JOB_EPOCH	Command	= 36	// REQ    Client
)


var lenCommand	map[Command]int = map[Command]int{
	CAN_DO:			1,
	CANT_DO:		1,
	SUBMIT_JOB:		3,
	JOB_CREATED:		1,
	JOB_ASSIGN:		3,
	WORK_STATUS:		3,
	WORK_COMPLETE:		2,
	WORK_FAIL:		1,
	GET_STATUS:		1,
	ECHO_REQ:		1,
	ECHO_RES:		1,
	SUBMIT_JOB_BG:		3,
	ERROR:			2,
	STATUS_RES:		5,
	SUBMIT_JOB_HIGH:	3,
	SET_CLIENT_ID:		1,
	CAN_DO_TIMEOUT:		2,
	WORK_EXCEPTION:		2,
	OPTION_REQ:		1,
	OPTION_RES:		1,
	WORK_DATA:		2,
	WORK_WARNING:		2,
	JOB_ASSIGN_UNIQ:	4,
	SUBMIT_JOB_HIGH_BG:	3,
	SUBMIT_JOB_LOW:		3,
	SUBMIT_JOB_LOW_BG:	3,
	SUBMIT_JOB_SCHED:	8,
	SUBMIT_JOB_EPOCH:	4,
}

var helloCommand	map[Command]int = map[Command]int{
	CAN_DO:			HELLO_COMMAND_REQ,
	CANT_DO:		HELLO_COMMAND_REQ,
	RESET_ABILITIES:	HELLO_COMMAND_REQ,
	PRE_SLEEP:		HELLO_COMMAND_REQ,
	NOOP:			HELLO_COMMAND_RES,
	SUBMIT_JOB:		HELLO_COMMAND_REQ,
	JOB_CREATED:		HELLO_COMMAND_RES,
	GRAB_JOB:		HELLO_COMMAND_REQ,
	NO_JOB:			HELLO_COMMAND_RES,
	JOB_ASSIGN:		HELLO_COMMAND_RES,
	WORK_STATUS:		HELLO_COMMAND_REQ|HELLO_COMMAND_RES,
	WORK_COMPLETE:		HELLO_COMMAND_REQ|HELLO_COMMAND_RES,
	WORK_FAIL:		HELLO_COMMAND_REQ|HELLO_COMMAND_RES,
	GET_STATUS:		HELLO_COMMAND_REQ,
	ECHO_REQ:		HELLO_COMMAND_REQ,
	ECHO_RES:		HELLO_COMMAND_RES,
	SUBMIT_JOB_BG:		HELLO_COMMAND_REQ,
	ERROR:			HELLO_COMMAND_RES,
	STATUS_RES:		HELLO_COMMAND_RES,
	SUBMIT_JOB_HIGH:	HELLO_COMMAND_REQ,
	SET_CLIENT_ID:		HELLO_COMMAND_REQ,
	CAN_DO_TIMEOUT:		HELLO_COMMAND_REQ,
	ALL_YOURS:		HELLO_COMMAND_REQ,
	WORK_EXCEPTION:		HELLO_COMMAND_REQ|HELLO_COMMAND_RES,
	OPTION_REQ:		HELLO_COMMAND_REQ,
	OPTION_RES:		HELLO_COMMAND_RES,
	WORK_DATA:		HELLO_COMMAND_REQ|HELLO_COMMAND_RES,
	WORK_WARNING:		HELLO_COMMAND_REQ|HELLO_COMMAND_RES,
	GRAB_JOB_UNIQ:		HELLO_COMMAND_REQ,
	JOB_ASSIGN_UNIQ:	HELLO_COMMAND_RES,
	SUBMIT_JOB_HIGH_BG:	HELLO_COMMAND_REQ,
	SUBMIT_JOB_LOW:		HELLO_COMMAND_REQ,
	SUBMIT_JOB_LOW_BG:	HELLO_COMMAND_REQ,
	SUBMIT_JOB_SCHED:	HELLO_COMMAND_REQ,
	SUBMIT_JOB_EPOCH:	HELLO_COMMAND_REQ,

}



func (h Hello)String() string {
	switch	h {
	case	REQ:	return	"REQ"
	case	RES:	return	"RES"
	default:	return	fmt.Sprintf( "WTF[%08x]", uint32(h) )
	}
}


func (c Command)PayloadLen() int {
	expected_len, ok := lenCommand[c]
	if !ok {
		return	0
	}
	return	expected_len
}

func (cmd Command)MatchHello(hello Hello) error {
	expected_match, ok := helloCommand[cmd]
	if !ok {
		return	&RESQRequiredError{cmd, hello, 0}
	}

	exp_req	:= (expected_match&HELLO_COMMAND_REQ) == HELLO_COMMAND_REQ
	exp_res	:= (expected_match&HELLO_COMMAND_RES) == HELLO_COMMAND_RES
	is_req	:= (hello == REQ)
	is_res	:= (hello == RES)

	switch	{
	case	is_req:
		if exp_req {
			return	nil
		}
		return	&RESQRequiredError{cmd, hello, RES}

	case	is_res:
		if exp_res {
			return	nil
		}
		return	&RESQRequiredError{cmd, hello, REQ}
	}
	return	&RESQRequiredError{cmd, hello, 0}
}


func (c Command)String() string {
	switch	c {
	case	CAN_DO:			return	"CAN_DO"
	case	CANT_DO:		return	"CANT_DO"
	case	RESET_ABILITIES:	return	"RESET_ABILITIES"
	case	PRE_SLEEP:		return	"PRE_SLEEP"
	case	NOOP:			return	"NOOP"
	case	SUBMIT_JOB:		return	"SUBMIT_JOB"
	case	JOB_CREATED:		return	"JOB_CREATED"
	case	GRAB_JOB:		return	"GRAB_JOB"
	case	NO_JOB:			return	"NO_JOB"
	case	JOB_ASSIGN:		return	"JOB_ASSIGN"
	case	WORK_STATUS:		return	"WORK_STATUS"
	case	WORK_COMPLETE:		return	"WORK_COMPLETE"
	case	WORK_FAIL:		return	"WORK_FAIL"
	case	GET_STATUS:		return	"GET_STATUS"
	case	ECHO_REQ:		return	"ECHO_REQ"
	case	ECHO_RES:		return	"ECHO_RES"
	case	SUBMIT_JOB_BG:		return	"SUBMIT_JOB_BG"
	case	ERROR:			return	"ERROR"
	case	STATUS_RES:		return	"STATUS_RES"
	case	SUBMIT_JOB_HIGH:	return	"SUBMIT_JOB_HIGH"
	case	SET_CLIENT_ID:		return	"SET_CLIENT_ID"
	case	CAN_DO_TIMEOUT:		return	"CAN_DO_TIMEOUT"
	case	ALL_YOURS:		return	"ALL_YOURS"
	case	WORK_EXCEPTION:		return	"WORK_EXCEPTION"
	case	OPTION_REQ:		return	"OPTION_REQ"
	case	OPTION_RES:		return	"OPTION_RES"
	case	WORK_DATA:		return	"WORK_DATA"
	case	WORK_WARNING:		return	"WORK_WARNING"
	case	GRAB_JOB_UNIQ:		return	"GRAB_JOB_UNIQ"
	case	JOB_ASSIGN_UNIQ:	return	"JOB_ASSIGN_UNIQ"
	case	SUBMIT_JOB_HIGH_BG:	return	"SUBMIT_JOB_HIGH_BG"
	case	SUBMIT_JOB_LOW:		return	"SUBMIT_JOB_LOW"
	case	SUBMIT_JOB_LOW_BG:	return	"SUBMIT_JOB_LOW_BG"
	case	SUBMIT_JOB_SCHED:	return	"SUBMIT_JOB_SCHED"
	case	SUBMIT_JOB_EPOCH:	return	"SUBMIT_JOB_EPOCH"
	default:			return	fmt.Sprintf("CMD[%08x]", uint32(c))
	}
}


func (cmd Command)Unmarshal(hello Hello, payload []byte) (Packet,error) {
	if err := cmd.MatchHello(hello); err != nil {
		return	nil, err
	}

	switch	cmd.PayloadLen() {
	case	0:
		return	newPkt0size(cmd, hello, len(payload) )

	case	1:
		return	newPkt1len(cmd, hello, payload)

	default:
		return	newPktnlen(cmd, hello, payload)
	}
}
