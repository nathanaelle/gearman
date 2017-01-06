package	gearman // import "github.com/nathanaelle/gearman"

import	(
	"io"
)


type	(
	PacketFactory interface {
		Packet()	(Packet,error)
	}

	packetFactory struct {
		c	io.Reader
		size	int
		b	[]byte
	}
)

const	(
	THRESHOLD	int	= 1<<13
	MINSIZE		int	= 1<<16
)


func NewPacketFactory(c io.Reader, size int) PacketFactory {
	if size < MINSIZE {
		size = MINSIZE
	}

	return	&packetFactory {
		c:	c,
		size:	size,
		b:	make([]byte,0,size),
	}
}

func (pf *packetFactory)read_hint(expected int) (err error) {
	if len(pf.b) > expected {
		return	nil
	}

	if (cap(pf.b)-len(pf.b)) < expected {
		if expected < (pf.size+len(pf.b)) {
			expected = pf.size+len(pf.b)
		}

		old	:= pf.b
		pf.b	= make([]byte, 0, expected)
		copy(pf.b[0:len(old)], old[:])
	}

	n	:= 0
	dn	:= 0
	for n < expected {
		dn, err = pf.c.Read(pf.b[len(pf.b):cap(pf.b)])
		n	+= dn
	}
	return
}



func (pf *packetFactory)Packet() (Packet, error) {
	if err := pf.read_hint(12); err != nil {
		return	nil, err
	}

	cmd	:= Command(be2uint64(pf.b[0:8]))
	size	:= be2uint32(pf.b[8:12])

	pf.b	= pf.b[12:]

	if size == 0 {
		return	cmd.Unmarshal(nil)
	}

	if err := pf.read_hint(int(size)); err != nil {
		return	nil, err
	}
	payload	:= pf.b[:int(size)]
	pf.b = pf.b[int(size):]

	return	cmd.Unmarshal(payload)
}
