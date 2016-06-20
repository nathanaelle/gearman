package	gearman

import	(
	"fmt"
	"bytes"
	"errors"
)

type	(
	command	uint32

	Packet	interface {
		Marshal()	[]byte
		Encode([]byte)	(int,error)
		//	return the command in the packet
		Cmd()		command
		//	return the size in bytes of the payload
		Size()		uint32
		//	return the number of element in the payload
		Len()		int
		At(int)		[]byte
		String()	string
	}

	//	packet with no payload
	pkt0size struct {
		hello	uint32
		cmd	command
	}

	//	packet with only one payload
	pkt1len struct {
		hello	uint32
		cmd	command
		size	uint32
		raw	[]byte
	}

	//	packet with arbitrary payload
	pktcommon struct {
		pkt1len
		idx	[]int
	}
)


var	(
	BuffTooSmallError		error	= errors.New("Buffer is Too Small")
	PayloadInEmptyPacketError	error	= errors.New("Found payload in expected empty packet")

	internal_echo_packet	Packet = &pkt1len { RES, ECHO_RES, 13, []byte{'i','n','t','e','r','n','a','l',' ','e','c','h','o'} }
	reset_abilities		Packet = &pkt0size { REQ, RESET_ABILITIES }
	noop			Packet = &pkt0size { RES, NOOP }
	no_job			Packet = &pkt0size { RES, NO_JOB }
	grab_job		Packet = &pkt0size { REQ, GRAB_JOB }
	pre_sleep		Packet = &pkt0size { REQ, PRE_SLEEP }

)


const	REQ	uint32 = 0x00524551
const	RES	uint32 = 0x00524553


const	(
	_	command	= iota
	CAN_DO			//  1	REQ    Worker
	CANT_DO			//  2	REQ    Worker
	RESET_ABILITIES		//  3	REQ    Worker
	PRE_SLEEP		//  4	REQ    Worker
	_			//  5	-	-
	NOOP			//  6	RES    Worker
	SUBMIT_JOB		//  7	REQ    Client
	JOB_CREATED		//  8	RES    Client
	GRAB_JOB		//  9	REQ    Worker
	NO_JOB			// 10	RES    Worker
	JOB_ASSIGN		// 11	RES    Worker
	WORK_STATUS		// 12	REQ    Worker		RES    Client
	WORK_COMPLETE		// 13	REQ    Worker		RES    Client
	WORK_FAIL		// 14	REQ    Worker		RES    Client
	GET_STATUS		// 15	REQ    Client
	ECHO_REQ		// 16	REQ    Client/Worker
	ECHO_RES		// 17	RES    Client/Worker
	SUBMIT_JOB_BG		// 18	REQ    Client
	ERROR			// 19	RES    Client/Worker
	STATUS_RES		// 20	RES    Client
	SUBMIT_JOB_HIGH		// 21	REQ    Client
	SET_CLIENT_ID		// 22	REQ    Worker
	CAN_DO_TIMEOUT		// 23	REQ    Worker
	ALL_YOURS		// 24	REQ    Worker
	WORK_EXCEPTION		// 25	REQ    Worker		RES    Client
	OPTION_REQ		// 26	REQ    Client/Worker
	OPTION_RES		// 27	RES    Client/Worker
	WORK_DATA		// 28	REQ    Worker		RES    Client
	WORK_WARNING		// 29	REQ    Worker		RES    Client
	GRAB_JOB_UNIQ		// 30	REQ    Worker
	JOB_ASSIGN_UNIQ		// 31	RES    Worker
	SUBMIT_JOB_HIGH_BG	// 32	REQ    Client
	SUBMIT_JOB_LOW		// 33	REQ    Client
	SUBMIT_JOB_LOW_BG	// 34	REQ    Client
	SUBMIT_JOB_SCHED	// 35	REQ    Client
	SUBMIT_JOB_EPOCH	// 36	REQ    Client
)



func	req_packet(c command, data ...[]byte) Packet {
	p	:= new(pktcommon)
	p.hello	=  REQ
	p.cmd	=  c
	p.raw	=  bytes.Join(data, []byte{ 0 } )
	p.size	=  uint32(len(p.raw))
	p.index()

	return	p
}


func	res_packet(c command, data ...[]byte) Packet {
	p	:= new(pktcommon)
	p.hello	=  RES
	p.cmd	=  c
	p.raw	=  bytes.Join(data, []byte{ 0 } )
	p.size	=  uint32(len(p.raw))
	p.index()

	return	p
}


func can_do(h string) Packet {
	pl := []byte(h)
	return	&pkt1len{ REQ, CAN_DO, uint32(len(pl)), pl }
}


func cant_do(h string) Packet {
	pl := []byte(h)
	return	&pkt1len{ REQ, CANT_DO, uint32(len(pl)), pl }
}




//	return the size in bytes of a packet with no payload
func (pl pkt0size)Size() uint32 {
	return	0
}


//	return the command in the packet
func (pl pkt0size)Cmd() command {
	return	pl.cmd
}


//	return the number of element in the payload
func (pl pkt0size)Len() int {
	return	0
}


func	(pl pkt0size)At(i int) []byte {
	return	[]byte{}
}


func (pl pkt0size)String() string {
	switch pl.hello {
	case	REQ:
		return	fmt.Sprintf("REQ CMD=%2d SIZE=0 PLSIZE=0", int(pl.cmd))
	case	RES:
		return	fmt.Sprintf("RES CMD=%2d SIZE=0 PLSIZE=0", int(pl.cmd))
	default:
		return	fmt.Sprintf("WTF CMD=%2d SIZE=0 PLSIZE=0", int(pl.cmd))
	}
}

func (pl pkt0size)Marshal() []byte {
	buff	:= make([]byte,12)
	pl.Encode(buff)
	return	buff
}


func (pl pkt0size)Encode(buff []byte) (int,error) {
	if len(buff) < 12 {
		return	0, BuffTooSmallError
	}
	uint322be(buff[0:4], pl.hello)
	uint322be(buff[4:8], uint32(pl.cmd))
	uint322be(buff[8:12], 0)

	return 12,nil
}



//	return the size in bytes of the payload
func (pl pkt1len)Size() uint32 {
	return	pl.size
}


//	return the command in the packet
func (pl pkt1len)Cmd() command {
	return	pl.cmd
}


//	return the number of element in the payload
func (pl pkt1len)Len() int {
	return	1
}


func	(pl pkt1len)At(i int) []byte {
	switch i {
	case	0:
		return	pl.raw

	default:
		return	[]byte{}
	}
}


func (pl pkt1len)String() string {
	switch pl.hello {
	case	REQ:
		return	fmt.Sprintf("REQ CMD=%2d SIZE=%d PLSIZE=1", int(pl.cmd), pl.size)
	case	RES:
		return	fmt.Sprintf("RES CMD=%2d SIZE=%d PLSIZE=1", int(pl.cmd), pl.size)
	default:
		return	fmt.Sprintf("WTF CMD=%2d SIZE=%d PLSIZE=1", int(pl.cmd), pl.size)
	}
}

func (pl pkt1len)Marshal() []byte {
	buff	:= make([]byte,pl.size+12)
	pl.Encode(buff)
	return	buff
}


func (pl pkt1len)Encode(buff []byte) (int,error) {
	if len(buff) < int(pl.size+12) {
		return	0, BuffTooSmallError
	}
	uint322be(buff[0:4], pl.hello)
	uint322be(buff[4:8], uint32(pl.cmd))
	uint322be(buff[8:12], pl.size)
	copy(buff[12:],pl.raw)

	return int(pl.size+12),nil
}





//	generic packet with arbitrary payload
func	(pl *pktcommon)index() error {
	pl.idx	= []int{0}
	begin	:= 0
	for i,c := range pl.raw {
		if c == 0 {
			pl.idx = append(pl.idx, i)
			begin = i+1
		}
	}

	if begin < len(pl.raw) {
		pl.idx = append(pl.idx, len(pl.raw))
	}

	return	nil
}

//	return the number of element in the payload
func (pl pktcommon)Len() int {
	if len(pl.idx) < 2 {
		return 0
	}

	return	len(pl.idx)-1
}


func	(pl pktcommon)At(i int) []byte {
	switch {
	case	i < 0:
		return	[]byte{}

	case	i+1 >= len(pl.idx):
		return	[]byte{}

	case	i == 0:
		return	pl.raw[pl.idx[0]:pl.idx[1]]

	default:
		return	pl.raw[pl.idx[i]+1:pl.idx[i+1]]
	}
}


func (pl pktcommon)String() string {
	switch pl.hello {
	case	REQ:
		return	fmt.Sprintf("REQ CMD=%2d SIZE=%d PLSIZE=%d", int(pl.cmd), pl.size, pl.Len())
	case	RES:
		return	fmt.Sprintf("RES CMD=%2d SIZE=%d PLSIZE=%d", int(pl.cmd), pl.size, pl.Len())
	default:
		return	fmt.Sprintf("WTF CMD=%2d SIZE=%d PLSIZE=%d", int(pl.cmd), pl.size, pl.Len())
	}
}
