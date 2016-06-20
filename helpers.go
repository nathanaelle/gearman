package	gearman

import	(
	"io"
	"log"
	"crypto/rand"
	"encoding/base64"
)


//	unmarshal bigendian encoded uint32 to uint32
func	be2uint32(b []byte) uint32 {
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}


//	marshal uint32 to bigendian encoded uint32
func uint322be(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}


func debug(dbg *log.Logger, msg string, args ...interface{}) {
	if dbg == nil {
		return
	}
	dbg.Printf(msg, args...)
}


func rand_id() (string,error) {
	var	raw	[24]byte

	_,err	:= rand.Read(raw[:])
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}



func ReadPacket(c io.Reader) (Packet,error) {
	var	header	[12]byte

	if _, err := c.Read(header[:]); err != nil {
		return nil,err
	}
	hello	:= be2uint32(header[0:4])
	cmd	:= command(be2uint32(header[4:8]))
	size	:= be2uint32(header[8:12])

	switch cmd {
	case	PRE_SLEEP, NOOP, GRAB_JOB, RESET_ABILITIES, GRAB_JOB_UNIQ, ALL_YOURS, NO_JOB:
		if size != 0 {
			return	nil, PayloadInEmptyPacketError
		}
		return	&pkt0size{ hello, cmd },nil

	case	ECHO_RES, ECHO_REQ, GET_STATUS, JOB_CREATED, CAN_DO, CANT_DO, WORK_FAIL, SET_CLIENT_ID:
		p	:= &pkt1len{ hello, cmd, size, make([]byte, size) }
		size	:= uint32(0)
		for size < p.size {
			t_s, err := c.Read(p.raw[size:])
			if err != nil {
				return nil,err
			}
			size += uint32(t_s)
		}
		return	p,nil

	default:
		p	:= &pktcommon{ pkt1len{ hello, cmd, size, make([]byte, size) }, []int{} }
		size	:= uint32(0)

		for size < p.size {
			t_s, err := c.Read(p.raw[size:])
			if err != nil {
				return nil,err
			}
			size += uint32(t_s)
		}

		p.index()

		return	p,nil
	}

}


func WritePacket(c io.Writer, p Packet) (error) {
	size	:= 0
	raw 	:= p.Marshal()

	for size < len(raw) {
		t_s, err := c.Write(raw[size:])
		if err != nil {
			return err
		}
		size += t_s
	}

	return nil
}
