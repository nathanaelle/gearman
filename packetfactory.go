package gearman // import "github.com/nathanaelle/gearman"

import (
	"io"
)

type (
	// PacketFactory produce Packet
	PacketFactory interface {
		Packet() (Packet, error)
	}

	packetFactory struct {
		c    io.Reader
		size int
		b    []byte
	}
)

const (
	minSize int = 1 << 16
)

// NewPacketFactory create a PacketFactory from a io.Reader
func NewPacketFactory(c io.Reader, size int) PacketFactory {
	if size < minSize {
		size = minSize
	}

	return &packetFactory{
		c:    c,
		size: size,
		b:    make([]byte, 0, size),
	}
}

func (pf *packetFactory) readHint(expected int) (err error) {
	if len(pf.b) > expected {
		return nil
	}

	if cap(pf.b) < expected {
		newSize := expected
		if newSize < (pf.size + len(pf.b)) {
			newSize = pf.size + len(pf.b)
		}

		old := pf.b
		pf.b = make([]byte, len(old), newSize)
		copy(pf.b[0:len(old)], old[0:len(old)])
	}

	var n int

	for len(pf.b) < expected {
		n, err = pf.c.Read(pf.b[len(pf.b):cap(pf.b)])
		pf.b = pf.b[0 : len(pf.b)+n]
		if err != nil {
			return err
		}
	}
	return
}

func (pf *packetFactory) Packet() (Packet, error) {
	if err := pf.readHint(12); err != nil {
		return nil, err
	}

	cmd := Command(be2uint64(pf.b[0:8]))
	size := be2uint32(pf.b[8:12])

	pf.b = pf.b[12:]

	if size == 0 {
		return cmd.Unmarshal(nil)
	}

	if err := pf.readHint(int(size)); err != nil {
		return nil, err
	}
	payload := pf.b[:int(size)]
	pf.b = pf.b[int(size):]

	return cmd.Unmarshal(payload)
}
