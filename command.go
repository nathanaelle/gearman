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
	_			Command = iota
	CAN_DO			//	1	REQ    Worker
	CANT_DO			//	2	REQ    Worker
	RESET_ABILITIES		//	3	REQ    Worker
	PRE_SLEEP		//	4	REQ    Worker
	_			//	5
	NOOP			//	6	RES    Worker
	SUBMIT_JOB		//	7	REQ    Client
	JOB_CREATED		//	8	RES    Client
	GRAB_JOB		//	9	REQ    Worker
	NO_JOB			//	10	RES    Worker
	JOB_ASSIGN		//	11	RES    Worker
	WORK_STATUS		//	12	REQ    Worker		RES    Client
	WORK_COMPLETE		//	13	REQ    Worker		RES    Client
	WORK_FAIL		//	14	REQ    Worker		RES    Client
	GET_STATUS		//	15	REQ    Client
	ECHO_REQ		//	16	REQ    Client/Worker
	ECHO_RES		//	17	RES    Client/Worker
	SUBMIT_JOB_BG		//	18	REQ    Client
	ERROR			//	19	RES    Client/Worker
	STATUS_RES		//	20	RES    Client
	SUBMIT_JOB_HIGH		//	21	REQ    Client
	SET_CLIENT_ID		//	22	REQ    Worker
	CAN_DO_TIMEOUT		//	23	REQ    Worker
	ALL_YOURS		//	24	REQ    Worker
	WORK_EXCEPTION		//	25	REQ    Worker		RES    Client
	OPTION_REQ		//	26	REQ    Client/Worker
	OPTION_RES		//	27	RES    Client/Worker
	WORK_DATA		//	28	REQ    Worker		RES    Client
	WORK_WARNING		//	29	REQ    Worker		RES    Client
	GRAB_JOB_UNIQ		//	30	REQ    Worker
	JOB_ASSIGN_UNIQ		//	31	RES    Worker
	SUBMIT_JOB_HIGH_BG	//	32	REQ    Client
	SUBMIT_JOB_LOW		//	33	REQ    Client
	SUBMIT_JOB_LOW_BG	//	34	REQ    Client
	SUBMIT_JOB_SCHED	//	35	REQ    Client
	SUBMIT_JOB_EPOCH	//	36	REQ    Client

	OK			Command = iota+1000
	ADMIN_WORKERS
	ADMIN_STATUS
	ADMIN_MAX_QUEUE
	ADMIN_SHUTDOWN
	ADMIN_VERSION
	ADMIN_WORKERS_LIST
	ADMIN_STATUS_LIST
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
	OK:			1,
	ADMIN_SHUTDOWN:		1,
	ADMIN_MAX_QUEUE:	2,
	ADMIN_WORKERS_LIST:	-1,
	ADMIN_STATUS_LIST:	-1,
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
	ADMIN_WORKERS:		HELLO_COMMAND_REQ,
	ADMIN_STATUS:		HELLO_COMMAND_REQ,
	ADMIN_MAX_QUEUE:	HELLO_COMMAND_REQ,
	ADMIN_SHUTDOWN:		HELLO_COMMAND_REQ,
	ADMIN_VERSION:		HELLO_COMMAND_REQ,
	OK:			HELLO_COMMAND_RES,
	ADMIN_WORKERS_LIST:	HELLO_COMMAND_RES,
	ADMIN_STATUS_LIST:	HELLO_COMMAND_RES,
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
	case	OK:			return	"OK"
	case	ADMIN_WORKERS:		return	"ADMIN_WORKERS"
	case	ADMIN_STATUS:		return	"ADMIN_STATUS"
	case	ADMIN_MAX_QUEUE:	return	"ADMIN_MAX_QUEUE"
	case	ADMIN_SHUTDOWN:		return	"ADMIN_SHUTDOWN"
	case	ADMIN_VERSION:		return	"ADMIN_VERSION"
	case	ADMIN_WORKERS_LIST:	return	"ADMIN_WORKERS_LIST"
	case	ADMIN_STATUS_LIST:	return	"ADMIN_STATUS_LIST"

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
