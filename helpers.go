package	gearman

import	(
	"io"
	"log"
	"net"
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



func is_eof(err error) bool {
	return err == io.EOF
}

func is_timeout(err error) bool {
	switch	t_err := err.(type) {
	case net.Error:
		return	t_err.Timeout() && t_err.Temporary()
	}
	return false
}



func ReadPacket(c io.Reader) (Packet,error) {
	var	header	[12]byte
	var	payload	[]byte

	if _, err := c.Read(header[:]); err != nil {
		return nil,err
	}
	h	:= Hello(be2uint32(header[0:4]))
	cmd	:= Command(be2uint32(header[4:8]))
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

	return	cmd.Unmarshal(h, payload)
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
