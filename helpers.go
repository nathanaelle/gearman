package	gearman

import	(
	"io"
	"log"
	"net"
	"fmt"
	"bytes"
	"crypto/rand"
	"encoding/base64"
)


//	unmarshal bigendian encoded uint32 to uint32
func	be2uint32(b []byte) uint32 {
	return	uint32(b[3])     | uint32(b[2])<<8 |
		uint32(b[1])<<16 | uint32(b[0])<<24
}


//	marshal uint32 to bigendian encoded uint32
func uint322be(b []byte, v uint32) {
	b[0] = byte(v>>24)
	b[1] = byte(v>>16)
	b[2] = byte(v>>8 )
	b[3] = byte(v    )
}


//	unmarshal bigendian encoded uint64 to uint64
func	be2uint64(b []byte) uint64 {
	return	uint64(b[7])     | uint64(b[6])<<8  |
		uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 |
		uint64(b[1])<<48 | uint64(b[0])<<56
}


//	marshal uint64 to bigendian encoded uint64
func uint642be(b []byte, v uint64) {
	b[0] = byte(v>>56)
	b[1] = byte(v>>48)
	b[2] = byte(v>>40)
	b[3] = byte(v>>32)
	b[4] = byte(v>>24)
	b[5] = byte(v>>16)
	b[6] = byte(v>>8 )
	b[7] = byte(v    )
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



func is_eof(err error) bool {
	return err == io.EOF
}

func is_timeout(err error) bool {
	switch	t_err := err.(type) {
	case net.Error:
		return	t_err.Timeout()
	}
	return false
}



func ReadPacket(c io.Reader) (Packet,error) {
	var	header	[12]byte
	var	payload	[]byte

	if _, err := c.Read(header[:]); err != nil {
		return nil,err
	}
	if header[0] != 0 {
		return nil,TextProtocolError
	}

	//h	:= Hello(be2uint32(header[0:4]))
	cmd	:= Command(be2uint64(header[0:8]))
	size	:= be2uint32(header[8:12])

	if size > 0 {
		t_size	:= uint32(0)
		payload	=  make([]byte, size)
		for t_size < size {
			t_s, err := c.Read(payload[t_size:])
			if err != nil {
				return nil,err
			}
			t_size += uint32(t_s)
		}
	}

	return	cmd.Unmarshal(payload[:])
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


func	BuildPacket(c Command, data ...[]byte) (p Packet) {
	var err error

	switch	len(data) {
	case	0:
		p,err	= c.Unmarshal([]byte{})
	case	1:
		p,err	= c.Unmarshal(data[0])
	default:
		p,err	= c.Unmarshal(bytes.Join(data, []byte{ 0 } ))
	}
	if err != nil {
		panic(fmt.Sprintf("%v got %v", c, err))
	}

	return	p
}
