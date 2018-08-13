package gearman // import "github.com/nathanaelle/gearman"

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
)

// unmarshal bigendian encoded uint32 to uint32
func be2uint32(b []byte) uint32 {
	return uint32(b[3]) | uint32(b[2])<<8 |
		uint32(b[1])<<16 | uint32(b[0])<<24
}

// marshal uint32 to bigendian encoded uint32
func uint322be(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

// unmarshal bigendian encoded uint64 to uint64
func be2uint64(b []byte) uint64 {
	return uint64(b[7]) | uint64(b[6])<<8 |
		uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 |
		uint64(b[1])<<48 | uint64(b[0])<<56
}

// marshal uint64 to bigendian encoded uint64
func uint642be(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func debug(dbg *log.Logger, msg string, args ...interface{}) {
	if dbg == nil {
		return
	}
	dbg.Printf(msg, args...)
}

func randID() (string, error) {
	var raw [24]byte

	_, err := rand.Read(raw[:])
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func isEOF(err error) bool {
	return err == io.EOF
}

func isTimeout(err error) bool {
	switch tErr := err.(type) {
	case net.Error:
		return tErr.Timeout()
	}
	return false
}

// BuildPacket create a Packet from a Command and a list of data
func BuildPacket(c Command, data ...MarshalerGearman) Packet {
	switch len(data) {
	case 0:
		p, err := c.Unmarshal([]byte{})
		if err != nil {
			panic(fmt.Sprintf("%v : %v", c, err))
		}
		return p

	case 1:
		b, err := data[0].MarshalGearman()
		if err != nil {
			panic(fmt.Sprintf("%v got %v", c, err))
		}

		p, err := c.Unmarshal(b)
		if err != nil {
			panic(fmt.Sprintf("%v : %v", c, err))
		}

		return p
	}

	l := len(data) - 1
	for _, d := range data {
		l += d.Len()
	}

	ret := make([]byte, l)
	l = 0
	for _, d := range data {
		b, err := d.MarshalGearman()
		if err != nil {
			panic(fmt.Sprintf("%v got %v", c, err))
		}
		copy(ret[l:], b[:])
		l += d.Len() + 1
	}

	p, err := c.Unmarshal(ret)
	if err != nil {
		panic(fmt.Sprintf("%v : %v", c, err))
	}

	return p
}
