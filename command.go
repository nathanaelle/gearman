package	gearman

import	(
	"fmt"
)

type	(
	Command	uint64
)

const	(
	CAN_DO			Command = 0x0052455100000001	// 1	REQ    Worker
	CANT_DO			Command = 0x0052455100000002	// 2	REQ    Worker
	RESET_ABILITIES		Command = 0x0052455100000003	// 3	REQ    Worker
	PRE_SLEEP		Command = 0x0052455100000004	// 4	REQ    Worker
	NOOP			Command = 0x0052455300000006	// 6	RES    Worker
	SUBMIT_JOB		Command = 0x0052455100000007	// 7	REQ    Client
	JOB_CREATED		Command = 0x0052455300000008	// 8	RES    Client
	GRAB_JOB		Command = 0x0052455100000009	// 9	REQ    Worker
	NO_JOB			Command = 0x005245530000000a	// 10	RES    Worker
	JOB_ASSIGN		Command = 0x005245530000000b	// 11	RES    Worker
	WORK_STATUS_WRK		Command = 0x005245510000000c	// 12	REQ    Worker
	WORK_STATUS		Command = 0x005245530000000c	// 12	RES    Client
	WORK_COMPLETE_WRK	Command = 0x005245510000000d	// 13	REQ    Worker
	WORK_COMPLETE		Command = 0x005245530000000d	// 13	RES    Client
	WORK_FAIL_WRK		Command = 0x005245510000000e	// 14	REQ    Worker
	WORK_FAIL		Command = 0x005245530000000e	// 14	RES    Client
	GET_STATUS		Command = 0x005245510000000f	// 15	REQ    Client
	ECHO_REQ		Command = 0x0052455100000010	// 16	REQ    Client/Worker
	ECHO_RES		Command = 0x0052455300000011	// 17	RES    Client/Worker
	SUBMIT_JOB_BG		Command = 0x0052455100000012	// 18	REQ    Client
	ERROR			Command = 0x0052455300000013	// 19	RES    Client/Worker
	STATUS_RES		Command = 0x0052455300000014	// 20	RES    Client
	SUBMIT_JOB_HIGH		Command = 0x0052455100000015	// 21	REQ    Client
	SET_CLIENT_ID		Command = 0x0052455100000016	// 22	REQ    Worker
	CAN_DO_TIMEOUT		Command = 0x0052455100000017	// 23	REQ    Worker
	ALL_YOURS		Command = 0x0052455100000018	// 24	REQ    Worker
	WORK_EXCEPTION_WRK	Command = 0x0052455100000019	// 25	REQ    Worker
	WORK_EXCEPTION		Command = 0x0052455300000019	// 25	RES    Client
	OPTION_REQ		Command = 0x005245510000001a	// 26	REQ    Client/Worker
	OPTION_RES		Command = 0x005245530000001b	// 27	RES    Client/Worker
	WORK_DATA_WRK		Command = 0x005245510000001c	// 28	REQ    Worker
	WORK_DATA		Command = 0x005245530000001c	// 28	RES    Client
	WORK_WARNING_WRK	Command = 0x005245510000001d	// 29	REQ    Worker
	WORK_WARNING		Command = 0x005245530000001d	// 29	RES    Client
	GRAB_JOB_UNIQ		Command = 0x005245510000001e	// 30	REQ    Worker
	JOB_ASSIGN_UNIQ		Command = 0x005245530000001f	// 31	RES    Worker
	SUBMIT_JOB_HIGH_BG	Command = 0x0052455100000020	// 32	REQ    Client
	SUBMIT_JOB_LOW		Command = 0x0052455100000021	// 33	REQ    Client
	SUBMIT_JOB_LOW_BG	Command = 0x0052455100000022	// 34	REQ    Client
	SUBMIT_JOB_SCHED	Command = 0x0052455100000023	// 35	REQ    Client
	SUBMIT_JOB_EPOCH	Command = 0x0052455100000024	// 36	REQ    Client

	OK			Command = 0x0052455300000040
	ADMIN_WORKERS		Command = 0x0052455100000041
	ADMIN_STATUS		Command = 0x0052455100000042
	ADMIN_MAX_QUEUE		Command = 0x0052455100000043
	ADMIN_SHUTDOWN		Command = 0x0052455100000044
	ADMIN_VERSION		Command = 0x0052455100000045
	ADMIN_WORKERS_LIST	Command = 0x0052455300000046
	ADMIN_STATUS_LIST	Command = 0x0052455300000047
)


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
	case	WORK_STATUS_WRK:	return	"WORK_STATUS_WRK"
	case	WORK_STATUS:		return	"WORK_STATUS"
	case	WORK_COMPLETE_WRK:	return	"WORK_COMPLETE_WRK"
	case	WORK_COMPLETE:		return	"WORK_COMPLETE"
	case	WORK_FAIL_WRK:		return	"WORK_FAIL_WRK"
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
	case	WORK_EXCEPTION_WRK:	return	"WORK_EXCEPTION_WRK"
	case	WORK_EXCEPTION:		return	"WORK_EXCEPTION"
	case	OPTION_REQ:		return	"OPTION_REQ"
	case	OPTION_RES:		return	"OPTION_RES"
	case	WORK_DATA_WRK:		return	"WORK_DATA_WRK"
	case	WORK_DATA:		return	"WORK_DATA"
	case	WORK_WARNING_WRK:	return	"WORK_WARNING_WRK"
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

	default:			return	fmt.Sprintf("HELLO[%08x] CMD[%08x]", uint32(c>>32), uint32(c))
	}
}


func (cmd Command)Unmarshal(payload []byte) (Packet,error) {
	switch	cmd {
	case	RESET_ABILITIES,PRE_SLEEP,NOOP,GRAB_JOB,NO_JOB,ALL_YOURS,GRAB_JOB_UNIQ,ADMIN_WORKERS,ADMIN_STATUS,ADMIN_VERSION:
		return	newPkt0size(cmd, len(payload) )

	case	JOB_CREATED,CAN_DO,CANT_DO,GET_STATUS,SET_CLIENT_ID,OK,ADMIN_SHUTDOWN,
		WORK_FAIL_WRK,WORK_FAIL,
		ECHO_REQ,ECHO_RES,
		OPTION_REQ,OPTION_RES:
		return	newPkt1len(cmd, payload)

	case	ERROR,CAN_DO_TIMEOUT,ADMIN_MAX_QUEUE,
		WORK_COMPLETE_WRK,WORK_COMPLETE,
		WORK_EXCEPTION_WRK,WORK_EXCEPTION,
		WORK_DATA_WRK,WORK_DATA,
		WORK_WARNING_WRK,WORK_WARNING:
		return	newPktnlen(cmd, payload, 2)

	case	SUBMIT_JOB,JOB_ASSIGN,
		WORK_STATUS_WRK,WORK_STATUS,
		SUBMIT_JOB_HIGH,SUBMIT_JOB_LOW,
		SUBMIT_JOB_BG,SUBMIT_JOB_HIGH_BG,SUBMIT_JOB_LOW_BG:
		return	newPktnlen(cmd, payload, 3)

	case	JOB_ASSIGN_UNIQ,SUBMIT_JOB_EPOCH:
		return	newPktnlen(cmd, payload, 4)

	case	STATUS_RES:
		return	newPktnlen(cmd, payload, 5)

	case	SUBMIT_JOB_SCHED:
		return	newPktnlen(cmd, payload, 8)

	case	ADMIN_WORKERS_LIST,ADMIN_STATUS_LIST:
		return	newPktnlen(cmd, payload, -1)
	}

	return	nil, &UndefinedPacketError{ cmd }
}
