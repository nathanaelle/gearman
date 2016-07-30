package	gearman

import	(
	"fmt"
)

type	(
	//	Gearman Packet
	Packet	interface {
		Marshal()	[]byte
		Encode([]byte)	(int,error)
		//	return the command in the packet
		Cmd()		Command
		//	return the size in bytes of the payload
		Size()		uint32
		//	return the number of element in the payload
		Len()		int
		//	return the payload at the index i
		At(int)		[]byte

		//	return the Raw Payload
		Payload()	[]byte

		//	implements Stringer interface
		String()	string
	}

	//	packet with no payload
	pkt0size struct {
		cmd	Command
	}

	//	packet with only one payload
	pkt1len struct {
		cmd	Command
		size	uint32
		raw	[]byte
	}

	//	packet with arbitrary payload
	pktcommon struct {
		cmd	Command
		size	uint32
		raw	[]byte
		idx	[]int
	}
)


var	(
	internal_echo_packet	Packet = &pkt1len { ECHO_RES, 13, []byte{'i','n','t','e','r','n','a','l',' ','e','c','h','o'} }
	empty_echo_packet	Packet = &pkt1len { ECHO_REQ, 0, []byte{} }
	reset_abilities		Packet = &pkt0size { RESET_ABILITIES }
	noop			Packet = &pkt0size { NOOP }
	no_job			Packet = &pkt0size { NO_JOB }
	grab_job		Packet = &pkt0size { GRAB_JOB }
	grab_job_uniq		Packet = &pkt0size { GRAB_JOB_UNIQ }
	pre_sleep		Packet = &pkt0size { PRE_SLEEP }
)


func newPkt0size(cmd Command, size int) (Packet,error) {
	if size != 0 {
		return	nil, PayloadInEmptyPacketError
	}
	return	&pkt0size{ cmd },nil
}

//	return the size in bytes of a packet with no payload
func (pl pkt0size)Size() uint32 {
	return	0
}

//	return the command in the packet
func (pl pkt0size)Cmd() Command {
	return	pl.cmd
}

//	return the number of element in the payload
func (_ pkt0size)Len() int {
	return	0
}


func (_ pkt0size)Payload() []byte {
	return	[]byte{}
}


//	return the payload at the index i
func	(_ pkt0size)At(_ int) []byte {
	return	[]byte{}
}

//	implements Stringer interface
func (pl pkt0size)String() string {
	return	fmt.Sprintf("%v SIZE=0 PLSIZE=0", pl.cmd)
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
	uint642be(buff[0:8], uint64(pl.cmd))
	uint322be(buff[8:12], 0)

	return 12,nil
}


//	create a new packet
func newPkt1len(cmd Command, payload []byte) (Packet,error) {
	return	&pkt1len{ cmd, uint32(len(payload)), payload[:] }, nil
}

//	return the size in bytes of the payload
func (pl pkt1len)Size() uint32 {
	return	pl.size
}

//	return the command in the packet
func (pl pkt1len)Cmd() Command {
	return	pl.cmd
}

//	return the number of element in the payload
func (pl pkt1len)Len() int {
	return	1
}

func (pl pkt1len)Payload() []byte {
	return	pl.raw
}


//	return the payload at the index i
func	(pl pkt1len)At(i int) []byte {
	switch i {
	case	0:	return	pl.raw
	default:	return	[]byte{}
	}
}

//	implements Stringer interface
func (pl pkt1len)String() string {
	return	fmt.Sprintf("%v SIZE=%2d PLSIZE=1", pl.cmd, pl.size)
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
	uint642be(buff[0:8], uint64(pl.cmd))
	uint322be(buff[8:12], pl.size)
	copy(buff[12:],pl.raw)

	return int(pl.size+12),nil
}


//	generic packet with arbitrary payload
func newPktnlen(cmd Command, payload []byte, expected_len int) (Packet,error) {
	len_payload	:= len(payload)

	// indexing the payload
	// the idea is storing the begin and end of each slice of the payload
	// think like this 0pa0y0lo0ad0 as 3 inner zeros and 2 outter zeros
	// the loop count the inner zeros
	l := 2
	for _,c := range payload {
		if c == 0 {
			l++
		}
	}

	if expected_len != l-1 {
		return nil, &PayloadLenError{ cmd, expected_len, l-2 }
	}

	pkt	:= &pktcommon{ cmd, uint32(len_payload), payload, make([]int, l) }
	idx	:= 1
	for i,c := range payload {
		if c == 0 {
			pkt.idx[idx] =i
			idx++
		}
	}
	pkt.idx[0]	= 0
	pkt.idx[l-1]	= len_payload

	return pkt,nil
}

//	return the size in bytes of the payload
func (pl pktcommon)Size() uint32 {
	return	pl.size
}

//	return the command in the packet
func (pl pktcommon)Cmd() Command {
	return	pl.cmd
}

func (pl pktcommon)Payload() []byte {
	return	pl.raw
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
	case	i < 0:			return	[]byte{}
	case	i+1 >= len(pl.idx):	return	[]byte{}
	case	i == 0:			return	pl.raw[pl.idx[0]:pl.idx[1]]
	default:			return	pl.raw[pl.idx[i]+1:pl.idx[i+1]]
	}
}


func (pl pktcommon)String() string {
	return	fmt.Sprintf("%v SIZE=%d PLSIZE=%d", pl.cmd, pl.size, pl.Len())
}


func (pl pktcommon)Marshal() []byte {
	buff	:= make([]byte,pl.size+12)
	pl.Encode(buff)
	return	buff
}


func (pl pktcommon)Encode(buff []byte) (int,error) {
	if len(buff) < int(pl.size+12) {
		return	0, BuffTooSmallError
	}
	uint642be(buff[0:8], uint64(pl.cmd))
	uint322be(buff[8:12], pl.size)
	copy(buff[12:],pl.raw)

	return int(pl.size+12),nil
}
