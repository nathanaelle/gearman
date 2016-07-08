package	gearman

import	(
	"fmt"
	"bytes"
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
		//	implements Stringer interface
		String()	string
	}

	//	packet with no payload
	pkt0size struct {
		hello	Hello
		cmd	Command
	}

	//	packet with only one payload
	pkt1len struct {
		hello	Hello
		cmd	Command
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
	internal_echo_packet	Packet = &pkt1len { RES, ECHO_RES, 13, []byte{'i','n','t','e','r','n','a','l',' ','e','c','h','o'} }
	empty_echo_packet	Packet = &pkt1len { REQ, ECHO_REQ, 0, []byte{} }
	reset_abilities		Packet = &pkt0size { REQ, RESET_ABILITIES }
	noop			Packet = &pkt0size { RES, NOOP }
	no_job			Packet = &pkt0size { RES, NO_JOB }
	grab_job		Packet = &pkt0size { REQ, GRAB_JOB }
	grab_job_uniq		Packet = &pkt0size { REQ, GRAB_JOB_UNIQ }
	pre_sleep		Packet = &pkt0size { REQ, PRE_SLEEP }
)



func	req_packet(c Command, data ...[]byte) Packet {
	p,err	:= newPktnlen(c, REQ, bytes.Join(data, []byte{ 0 } ))
	if err != nil {
		panic(fmt.Sprintf("%v got %v", c, err))
	}
	return	p
}


func	res_packet(c Command, data ...[]byte) Packet {
	p,err	:= newPktnlen(c, RES, bytes.Join(data, []byte{ 0 } ))
	if err != nil {
		panic(fmt.Sprintf("%v got %v", c, err))
	}
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



func newPkt0size(cmd Command, given_hello Hello, size int) (Packet,error) {
	if size != 0 {
		return	nil, PayloadInEmptyPacketError
	}
	return	&pkt0size{ given_hello, cmd },nil
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
func (pl pkt0size)Len() int {
	return	0
}

//	return the payload at the index i
func	(pl pkt0size)At(i int) []byte {
	return	[]byte{}
}

//	implements Stringer interface
func (pl pkt0size)String() string {
	return	fmt.Sprintf("%v %v SIZE=0 PLSIZE=0", pl.hello, pl.cmd)
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
	uint322be(buff[0:4], uint32(pl.hello))
	uint322be(buff[4:8], uint32(pl.cmd))
	uint322be(buff[8:12], 0)

	return 12,nil
}


func newPkt1len(cmd Command, given_hello Hello, payload []byte) (Packet,error) {
	return	&pkt1len{ given_hello, cmd, uint32(len(payload)), payload }, nil
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

//	return the payload at the index i
func	(pl pkt1len)At(i int) []byte {
	switch i {
	case	0:	return	pl.raw
	default:	return	[]byte{}
	}
}

//	implements Stringer interface
func (pl pkt1len)String() string {
	return	fmt.Sprintf("%v %v SIZE=%2d PLSIZE=1", pl.hello, pl.cmd, pl.size)
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
	uint322be(buff[0:4], uint32(pl.hello))
	uint322be(buff[4:8], uint32(pl.cmd))
	uint322be(buff[8:12], pl.size)
	copy(buff[12:],pl.raw)

	return int(pl.size+12),nil
}


//	generic packet with arbitrary payload
func newPktnlen(cmd Command, hello Hello, payload []byte) (Packet,error) {
	expected_len	:= cmd.PayloadLen()
	if expected_len == 0 {
		return	newPkt0size(cmd, hello, len(payload))
	}

	pkt := pkt1len{ hello, cmd, uint32(len(payload)), payload }
	if expected_len == 1 {
		return &pkt, nil
	}

	// indexing the payload
	// the idea is storing the begin and end of each slice of the payload
	// think like this 0pa0y0lo0ad0 as 3 inner zeros and 2 outter zeros
	// the loop count the inner zeros
	l := 2
	for _,c := range pkt.raw {
		if c == 0 {
			l++
		}
	}

	if expected_len != l-1 {
		return nil, &PayloadLenError{ cmd, expected_len, l-2 }
	}


	begin	:= 0
	index	:= make([]int, 0, l)
	index	=  append(index, 0)
	for i,c := range pkt.raw {
		if c == 0 {
			index = append(index, i)
			begin = i+1
		}
	}

	if begin < len(pkt.raw) {
		index = append(index, len(pkt.raw))
	}

	return &pktcommon{ pkt, index },nil
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
	return	fmt.Sprintf("%v %v SIZE=%d PLSIZE=%d", pl.hello, pl.cmd, pl.size, pl.Len())
}
