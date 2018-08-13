package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"io"
	"testing"
)

type packetTest struct {
	data   []byte
	packet Packet
	err    error
	plSize int
}

type loopreader struct {
	idx  int
	buff []byte
}

func LoopReader(b []byte) io.Reader {
	return &loopreader{0, b}
}

func (lr *loopreader) Read(b []byte) (int, error) {
	l := len(lr.buff[lr.idx:])

	if len(b) < l {
		copy(b, lr.buff[lr.idx:lr.idx+len(b)])
		lr.idx += len(b)
		return len(b), nil
	}
	copy(b, lr.buff[lr.idx:])
	lr.idx = 0
	return l, nil
}

var validPacket = []packetTest{
	{
		[]byte{0, 0x52, 0x45, 0x53, 0, 0, 0, 0x11, 0, 0, 0, 0x0d, 'i', 'n', 't', 'e', 'r', 'n', 'a', 'l', ' ', 'e', 'c', 'h', 'o'},
		internalEchoPacket,
		nil,
		1,
	},
	{
		[]byte{0, 0x52, 0x45, 0x51, 0, 0, 0, 0x11, 0, 0, 0, 0x0d, 'i', 'n', 't', 'e', 'r', 'n', 'a', 'l', 0, 'e', 'c', 'h', 'o'},
		nil,
		&UndefinedPacketError{Command(0x0052455100000011)},
		0,
	},
	{
		[]byte{0, 0x52, 0x45, 0x51, 0, 0, 0, 0x03, 0, 0, 0, 0x0d, 'i', 'n', 't', 'e', 'r', 'n', 'a', 'l', 0, 'e', 'c', 'h', 'o'},
		nil,
		PayloadInEmptyPacketError,
		0,
	},
	{
		[]byte{0, 0x52, 0x45, 0x51, 0, 0, 0, 0x03, 0, 0, 0, 0},
		resetAbilities,
		nil,
		0,
	},
	{
		[]byte{0, 0x52, 0x45, 0x53, 0, 0, 0, 0x0b, 0, 0, 0, 0x14, 0x48, 0x3a, 0x6c, 0x61, 0x70, 0x3a, 0x31, 0x00, 0x72, 0x65, 0x76, 0x65, 0x72, 0x73, 0x65, 0x00, 0x74, 0x65, 0x73, 0x74},
		BuildPacket(JOB_ASSIGN, Opacify([]byte("H:lap:1")), Opacify([]byte("reverse")), Opacify([]byte("test"))),
		nil,
		3,
	},
}

func Test_Packet(t *testing.T) {
	for Ti, vp := range validPacket {
		out := new(bytes.Buffer)
		pf := NewPacketFactory(bytes.NewBuffer(vp.data), 0)
		p, err := pf.Packet()

		if !validErr(t, err, vp.err) {
			t.Logf("T_%d\t[%x] %+v", Ti, vp.data, vp.packet)
			continue
		}

		if vp.err != nil {
			continue
		}

		if _, err := p.WriteTo(out); err != nil {
			t.Logf("T_%d\t[%x] %+v [%x]", Ti, vp.data, vp.packet, out.Bytes())
			t.Errorf("T_%d\t%+v", Ti, err)
			continue
		}

		if !bytes.Equal(vp.data, out.Bytes()) {
			t.Logf("T_%d\t[%x] %+v [%x]", Ti, vp.data, vp.packet, out.Bytes())
			t.Errorf("T_%d\t[%x] [%x] differs", Ti, vp.data, out.Bytes())
			continue
		}

		if p.Len() != vp.plSize {
			t.Errorf("T_%d\tgot [%d] expected [%d]", Ti, p.Len(), vp.plSize)
			continue
		}
	}

}

//	Unmarshal
func BenchmarkUnmarshalPkt0size(b *testing.B) {
	r := []byte{}
	for n := 0; n < b.N; n++ {
		RESET_ABILITIES.Unmarshal(r)
	}
}

func BenchmarkUnmarshalPkt1len(b *testing.B) {
	r := []byte{'i', 'n', 't', 'e', 'r', 'n', 'a', 'l', ' ', 'e', 'c', 'h', 'o'}
	for n := 0; n < b.N; n++ {
		ECHO_REQ.Unmarshal(r)
	}
}

func BenchmarkUnmarshalPktcommon(b *testing.B) {
	r := []byte{0x48, 0x3a, 0x6c, 0x61, 0x70, 0x3a, 0x31, 0, 0x72, 0x65, 0x76, 0x65, 0x72, 0x73, 0x65, 0, 0x74, 0x65, 0x73, 0x74}
	for n := 0; n < b.N; n++ {
		JOB_ASSIGN.Unmarshal(r)
	}
}

func BenchmarkMarshalPkt0size(b *testing.B) {
	var buff [12]byte
	for n := 0; n < b.N; n++ {
		resetAbilities.Encode(buff[:])
	}
}

func BenchmarkMarshalPkt1len(b *testing.B) {
	var buff [25]byte

	pkt := internalEchoPacket
	for n := 0; n < b.N; n++ {
		pkt.Encode(buff[:])
	}
}

func BenchmarkMarshalPktcommon(b *testing.B) {
	pkt := BuildPacket(JOB_ASSIGN, Opacify([]byte("H:lap:1")), Opacify([]byte("reverse")), Opacify([]byte("test")))
	var buff [32]byte

	for n := 0; n < b.N; n++ {
		pkt.Encode(buff[:])
	}
}
