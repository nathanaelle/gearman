package gearman

import (
	"io"
	"bytes"
	"testing"
)


type packet_test struct {
	data	[]byte
	packet	Packet
	err	error
	pl_size	int
}


type loopreader struct {
	idx	int
	buff	[]byte
}

func LoopReader(b []byte) io.Reader {
	return &loopreader { 0, b }
}

func (lr *loopreader)Read(b []byte) (int,error) {
	l := len(lr.buff[lr.idx:])

	if len(b) < l {
		copy(b, lr.buff[lr.idx:lr.idx+len(b)])
		lr.idx += len(b)
		return len(b),nil
	}
	copy(b, lr.buff[lr.idx:])
	lr.idx = 0
	return l,nil
}




var	valid_packet []packet_test = []packet_test {
	{
		[]byte{0, 0x52, 0x45, 0x53, 0, 0, 0, 0x11, 0, 0, 0, 0x0d, 'i','n','t','e','r','n','a','l',' ','e','c','h','o'},
		internal_echo_packet,
		nil,
		1,
	},
	{
		[]byte{0, 0x52, 0x45, 0x53, 0, 0, 0, 0x03, 0, 0, 0, 0x0d, 'i','n','t','e','r','n','a','l',0,'e','c','h','o'},
		nil,
		PayloadInEmptyPacketError,
		0,
	},
	{
		[]byte{0, 0x52, 0x45, 0x51, 0, 0, 0, 0x03, 0, 0, 0, 0},
		reset_abilities,
		nil,
		0,
	},
	{
		[]byte{0,0x52,0x45,0x53,0,0,0,0x0b,0,0,0,0x14,0x48,0x3a,0x6c,0x61,0x70,0x3a,0x31,0x00,0x72,0x65,0x76,0x65,0x72,0x73,0x65,0x00,0x74,0x65,0x73,0x74},
		res_packet(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test") ),
		nil,
		3,
	},
}


func Test_Packet(t *testing.T) {
	for _, vp := range valid_packet {
	 	out	:= new(bytes.Buffer)
		p,err	:= ReadPacket( bytes.NewBuffer(vp.data))
		if err != vp.err {
			t.Logf("[%x] %+v [%x]", vp.data, vp.packet, out.Bytes())
			t.Errorf("go error %+v expected %+v", err, vp.err)
		}

		if vp.err != nil {
			continue
		}

		if err	:= WritePacket(out, p); err != nil {
			t.Logf("[%x] %+v [%x]", vp.data, vp.packet, out.Bytes())
			t.Errorf("%+v", err)
			continue
		}

		if !bytes.Equal(vp.data, out.Bytes()) {
			t.Logf("[%x] %+v [%x]", vp.data, vp.packet, out.Bytes())
			t.Errorf("[%x] [%x] differs", vp.data, out.Bytes())
			continue
		}

		if p.Len() != vp.pl_size {
			t.Logf("[%x] %+v [%x]", vp.data, vp.packet, out.Bytes())
			t.Errorf("[%d] [%d] payload differs", p.Len(), vp.pl_size)
			continue
		}
	}

}



func BenchmarkReadPkt0size(b *testing.B) {
	r := LoopReader([]byte{0, 0x52, 0x45, 0x51, 0, 0, 0, 0x03, 0, 0, 0, 0})
        for n := 0; n < b.N; n++ {
		ReadPacket(r)
        }
}

func BenchmarkReadPkt1len(b *testing.B) {
	r := LoopReader([]byte{0, 0x52, 0x45, 0x53, 0, 0, 0, 0x10, 0, 0, 0, 0x0d, 'i','n','t','e','r','n','a','l',' ','e','c','h','o'})
        for n := 0; n < b.N; n++ {
		ReadPacket(r)
        }
}

func BenchmarkReadPktcommon(b *testing.B) {
	r := LoopReader([]byte{0,0x52,0x45,0x53,0,0,0,0x0b,0,0,0,0x14,0x48,0x3a,0x6c,0x61,0x70,0x3a,0x31,0x00,0x72,0x65,0x76,0x65,0x72,0x73,0x65,0x00,0x74,0x65,0x73,0x74})
        for n := 0; n < b.N; n++ {
		ReadPacket(r)
        }
}


func BenchmarkMarshalPkt0size(b *testing.B) {
	var	buff	[12]byte
        for n := 0; n < b.N; n++ {
		reset_abilities.Encode(buff[:])
        }
}

func BenchmarkMarshalPkt1len(b *testing.B) {
	var	buff	[25]byte

	pkt	:= internal_echo_packet
        for n := 0; n < b.N; n++ {
		pkt.Encode(buff[:])
        }
}


func BenchmarkMarshalPktcommon(b *testing.B) {
	pkt	:= res_packet(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test") )
	var	buff	[32]byte


        for n := 0; n < b.N; n++ {
		pkt.Encode(buff[:])
        }
}
