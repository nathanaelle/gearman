package protocol // import "github.com/nathanaelle/gearman/v2/protocol"

import (
	"fmt"
)

type (
	// Command is a low level command for the gearman protocol
	Command uint64
)

// List of available Command
const (
	CanDo                     Command = 0x0052455100000001 // 1	REQ    Worker
	CantDo                    Command = 0x0052455100000002 // 2	REQ    Worker
	ResetAbilities            Command = 0x0052455100000003 // 3	REQ    Worker
	PreSleep                  Command = 0x0052455100000004 // 4	REQ    Worker
	Noop                      Command = 0x0052455300000006 // 6	RES    Worker
	SubmitJob                 Command = 0x0052455100000007 // 7	REQ    Client
	JobCreated                Command = 0x0052455300000008 // 8	RES    Client
	GrabJob                   Command = 0x0052455100000009 // 9	REQ    Worker
	NoJob                     Command = 0x005245530000000a // 10	RES    Worker
	JobAssign                 Command = 0x005245530000000b // 11	RES    Worker
	WorkStatusWorker          Command = 0x005245510000000c // 12	REQ    Worker
	WorkStatus                Command = 0x005245530000000c // 12	RES    Client
	WorkCompleteWorker        Command = 0x005245510000000d // 13	REQ    Worker
	WorkComplete              Command = 0x005245530000000d // 13	RES    Client
	WorkFailWorker            Command = 0x005245510000000e // 14	REQ    Worker
	WorkFail                  Command = 0x005245530000000e // 14	RES    Client
	GetStatus                 Command = 0x005245510000000f // 15	REQ    Client
	EchoReq                   Command = 0x0052455100000010 // 16	REQ    Client/Worker
	EchoRes                   Command = 0x0052455300000011 // 17	RES    Client/Worker
	SubmitJobBackground       Command = 0x0052455100000012 // 18	REQ    Client
	Error                     Command = 0x0052455300000013 // 19	RES    Client/Worker
	StatusRes                 Command = 0x0052455300000014 // 20	RES    Client
	SubmitJobHigh             Command = 0x0052455100000015 // 21	REQ    Client
	SetClientID               Command = 0x0052455100000016 // 22	REQ    Worker
	CanDoTimeout              Command = 0x0052455100000017 // 23	REQ    Worker
	AllYours                  Command = 0x0052455100000018 // 24	REQ    Worker
	WorkExceptionWorker       Command = 0x0052455100000019 // 25	REQ    Worker
	WorkException             Command = 0x0052455300000019 // 25	RES    Client
	OptionReq                 Command = 0x005245510000001a // 26	REQ    Client/Worker
	OptionRes                 Command = 0x005245530000001b // 27	RES    Client/Worker
	WorkDataWorker            Command = 0x005245510000001c // 28	REQ    Worker
	WorkData                  Command = 0x005245530000001c // 28	RES    Client
	WorkWarningWorker         Command = 0x005245510000001d // 29	REQ    Worker
	WorkWarning               Command = 0x005245530000001d // 29	RES    Client
	GrabJobUniq               Command = 0x005245510000001e // 30	REQ    Worker
	JobAssignUniq             Command = 0x005245530000001f // 31	RES    Worker
	SubmitJobHighBackground   Command = 0x0052455100000020 // 32	REQ    Client
	SubmitJobLow              Command = 0x0052455100000021 // 33	REQ    Client
	SubmitJobLowBackground    Command = 0x0052455100000022 // 34	REQ    Client
	SubmitJobSched            Command = 0x0052455100000023 // 35	REQ    Client
	SubmitJobEpoch            Command = 0x0052455100000024 // 36	REQ    Client
	SubmitReduceJob           Command = 0x0052455100000025 // 37	REQ    Client
	SubmitReduceJobBackground Command = 0x0052455100000026 // 38	REQ    Client
	GrabJobAll                Command = 0x0052455100000027 // 39	REQ    Worker
	JobAssignAll              Command = 0x0052455300000028 // 40	RES    Worker
	GetStatusUniq             Command = 0x0052455100000029 // 41	REQ    Client
	StatusResUniq             Command = 0x005245530000002a // 42	RES    Client

	OK                 Command = 0x0052455300000050
	ADMIN_WORKERS      Command = 0x0052455100000051
	ADMIN_Status       Command = 0x0052455100000052
	ADMIN_MAX_QUEUE    Command = 0x0052455100000053
	ADMIN_SHUTDOWN     Command = 0x0052455100000054
	ADMIN_VERSION      Command = 0x0052455100000055
	ADMIN_WORKERS_LIST Command = 0x0052455300000056
	ADMIN_Status_LIST  Command = 0x0052455300000057
	CAPABILITY         Command = 0x0052455100000058
	CAPABILITY_LIST    Command = 0x0052455300000059
)

func (cmd Command) String() string {
	switch cmd {
	case CanDo:
		return "CanDo"
	case CantDo:
		return "CantDo"
	case ResetAbilities:
		return "ResetAbilities"
	case PreSleep:
		return "PreSleep"
	case Noop:
		return "Noop"
	case SubmitJob:
		return "SubmitJob"
	case JobCreated:
		return "JobCreated"
	case GrabJob:
		return "GrabJob"
	case NoJob:
		return "NoJob"
	case JobAssign:
		return "JobAssign"
	case WorkStatusWorker:
		return "WorkStatusWorker"
	case WorkStatus:
		return "WorkStatus"
	case WorkCompleteWorker:
		return "WorkCompleteWorker"
	case WorkComplete:
		return "WorkComplete"
	case WorkFailWorker:
		return "WorkFailWorker"
	case WorkFail:
		return "WorkFail"
	case GetStatus:
		return "GetStatus"
	case EchoReq:
		return "EchoReq"
	case EchoRes:
		return "EchoRes"
	case SubmitJobBackground:
		return "SubmitJobBackground"
	case Error:
		return "Error"
	case StatusRes:
		return "StatusRes"
	case SubmitJobHigh:
		return "SubmitJobHigh"
	case SetClientID:
		return "SetClientID"
	case CanDoTimeout:
		return "CanDoTimeout"
	case AllYours:
		return "AllYours"
	case WorkExceptionWorker:
		return "WorkExceptionWorker"
	case WorkException:
		return "WorkException"
	case OptionReq:
		return "OptionReq"
	case OptionRes:
		return "OptionRes"
	case WorkDataWorker:
		return "WorkDataWorker"
	case WorkData:
		return "WorkData"
	case WorkWarningWorker:
		return "WorkWarningWorker"
	case WorkWarning:
		return "WorkWarning"
	case GrabJobUniq:
		return "GrabJobUniq"
	case JobAssignUniq:
		return "JobAssignUniq"
	case SubmitJobHighBackground:
		return "SubmitJobHighBackground"
	case SubmitJobLow:
		return "SubmitJobLow"
	case SubmitJobLowBackground:
		return "SubmitJobLowBackground"
	case SubmitJobSched:
		return "SubmitJobSched"
	case SubmitJobEpoch:
		return "SubmitJobEpoch"
	case SubmitReduceJob:
		return "SubmitReduceJob"
	case SubmitReduceJobBackground:
		return "SubmitReduceJobBackground"
	case GrabJobAll:
		return "GrabJobAll"
	case JobAssignAll:
		return "JobAssignAll"
	case GetStatusUniq:
		return "GetStatusUniq"
	case StatusResUniq:
		return "StatusResUniq"

	case OK:
		return "OK"
	case ADMIN_WORKERS:
		return "ADMIN_WORKERS"
	case ADMIN_Status:
		return "ADMIN_Status"
	case ADMIN_MAX_QUEUE:
		return "ADMIN_MAX_QUEUE"
	case ADMIN_SHUTDOWN:
		return "ADMIN_SHUTDOWN"
	case ADMIN_VERSION:
		return "ADMIN_VERSION"
	case ADMIN_WORKERS_LIST:
		return "ADMIN_WORKERS_LIST"
	case ADMIN_Status_LIST:
		return "ADMIN_Status_LIST"

	default:
		return fmt.Sprintf("HELLO[%08x] CMD[%08x]", uint32(cmd>>32), uint32(cmd))
	}
}

// Borrow borrows from a packet Payload to create a new Packet
func (cmd Command) Borrow(p Packet) (Packet, error) {
	switch {
	case cmd == EchoRes && p.Cmd() == EchoReq:
		return newPkt1len(cmd, p.Payload())
	case cmd == WorkComplete && p.Cmd() == WorkCompleteWorker:
		return newPktnlen(cmd, p.Payload(), 2)
	case cmd == WorkException && p.Cmd() == WorkExceptionWorker:
		return newPktnlen(cmd, p.Payload(), 2)
	case cmd == WorkData && p.Cmd() == WorkDataWorker:
		return newPktnlen(cmd, p.Payload(), 2)
	case cmd == WorkWarning && p.Cmd() == WorkWarningWorker:
		return newPktnlen(cmd, p.Payload(), 2)
	case cmd == WorkStatus && p.Cmd() == WorkStatusWorker:
		return newPktnlen(cmd, p.Payload(), 3)
	}
	return nil, &BorrowError{cmd, p}
}

// Unmarshal decodes a payload to a Packet
func (cmd Command) Unmarshal(payload []byte) (Packet, error) {
	switch cmd {
	case ResetAbilities, PreSleep, Noop, AllYours,
		GrabJob, NoJob, GrabJobUniq, GrabJobAll,
		ADMIN_WORKERS, ADMIN_Status, ADMIN_VERSION:
		return newPkt0size(cmd, len(payload))

	case JobCreated, CanDo, CantDo, SetClientID, OK, ADMIN_SHUTDOWN,
		WorkFailWorker, WorkFail,
		EchoReq, EchoRes,
		OptionReq, OptionRes,
		GetStatus, GetStatusUniq:
		return newPkt1len(cmd, payload)

	case Error, CanDoTimeout, ADMIN_MAX_QUEUE,
		WorkCompleteWorker, WorkComplete,
		WorkExceptionWorker, WorkException,
		WorkDataWorker, WorkData,
		WorkWarningWorker, WorkWarning:
		return newPktnlen(cmd, payload, 2)

	case SubmitJob, JobAssign,
		WorkStatusWorker, WorkStatus,
		SubmitJobHigh, SubmitJobLow,
		SubmitJobBackground, SubmitJobHighBackground, SubmitJobLowBackground:
		return newPktnlen(cmd, payload, 3)

	case JobAssignUniq, SubmitJobEpoch, SubmitReduceJob, SubmitReduceJobBackground:
		return newPktnlen(cmd, payload, 4)

	case StatusRes, JobAssignAll:
		return newPktnlen(cmd, payload, 5)

	case StatusResUniq:
		return newPktnlen(cmd, payload, 6)

	case SubmitJobSched:
		return newPktnlen(cmd, payload, 8)

	case ADMIN_WORKERS_LIST, ADMIN_Status_LIST:
		return newPktnlen(cmd, payload, -1)
	}

	return nil, &UndefinedPacketError{cmd}
}
