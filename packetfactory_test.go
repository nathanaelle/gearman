package gearman // import "github.com/nathanaelle/gearman"

import (
	//	"crypto/rand"
	"bytes"
	"crypto/sha256"
	"testing"
)

func BenchmarkPacketFactoryPkt0size(b *testing.B) {
	r := LoopReader([]byte{0, 0x52, 0x45, 0x51, 0, 0, 0, 0x03, 0, 0, 0, 0})
	pf := NewPacketFactory(r, 1<<20)
	for n := 0; n < b.N; n++ {
		pf.Packet()
	}
}

func BenchmarkPacketFactoryPkt1len(b *testing.B) {
	r := LoopReader([]byte{0, 0x52, 0x45, 0x53, 0, 0, 0, 0x10, 0, 0, 0, 0x0d, 'i', 'n', 't', 'e', 'r', 'n', 'a', 'l', ' ', 'e', 'c', 'h', 'o'})
	pf := NewPacketFactory(r, 1<<20)
	for n := 0; n < b.N; n++ {
		pf.Packet()
	}
}

func BenchmarkPacketFactoryPktcommon(b *testing.B) {
	r := LoopReader([]byte{0, 0x52, 0x45, 0x53, 0, 0, 0, 0x0b, 0, 0, 0, 0x14, 0x48, 0x3a, 0x6c, 0x61, 0x70, 0x3a, 0x31, 0x00, 0x72, 0x65, 0x76, 0x65, 0x72, 0x73, 0x65, 0x00, 0x74, 0x65, 0x73, 0x74})
	pf := NewPacketFactory(r, 1<<20)
	for n := 0; n < b.N; n++ {
		pf.Packet()
	}
}

func rootBuff() (ret [][]byte, size int) {
	pkt, _ := NOOP.Unmarshal(nil)
	ret = append(ret, pkt.Marshal())
	pkt, _ = ECHO_REQ.Unmarshal([]byte("hello world"))
	ret = append(ret, pkt.Marshal())
	pkt, _ = ECHO_REQ.Unmarshal([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies sed, dolor. Cras elementum ultrices diam. Maecenas ligula massa, varius a, semper congue, euismod non, mi. Proin porttitor, orci nec nonummy molestie, enim est eleifend mi, non fermentum diam nisl sit amet erat. Duis semper. Duis arcu massa, scelerisque vitae, consequat in, pretium a, enim. Pellentesque congue. Ut in risus volutpat libero pharetra tempor. Cras vestibulum bibendum augue. Praesent egestas leo in pede. Praesent blandit odio eu enim. Pellentesque sed dui ut augue blandit sodales. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Aliquam nibh."))
	ret = append(ret, pkt.Marshal())

	for _, b := range ret {
		size += len(b)
	}

	return
}

func TestPacketFactoryMixed(t *testing.T) {
	b, size := rootBuff()
	loop := size * len(b)
	buf := make([]byte, loop/len(b)*size)
	hashV := sha256.New()
	hashT := sha256.New()
	p := 0
	for i := 0; i < loop; i++ {
		v := b[i%len(b)]
		hashV.Write(v)
		copy(buf[p:], v)
		p += len(v)
	}

	r := LoopReader(buf)
	pf := NewPacketFactory(r, 1<<20)
	for n := 0; n < loop; n++ {
		p, err := pf.Packet()
		if err != nil {
			panic(err)
		}
		if _, err := p.WriteTo(hashT); err != nil {
			panic(err)
		}
	}
	sumV := hashV.Sum(nil)
	sumT := hashT.Sum(nil)
	if !bytes.Equal(sumV, sumT) {
		panic("life sucks")
	}
}

func BenchmarkPacketFactoryMixed(bench *testing.B) {
	b, size := rootBuff()
	loop := size * len(b)
	buf := make([]byte, loop/len(b)*size)
	p := 0
	for i := 0; i < loop; i++ {
		v := b[i%len(b)]
		copy(buf[p:], v)
		p += len(v)
	}

	r := LoopReader(buf)
	pf := NewPacketFactory(r, 1<<20)

	bench.ResetTimer()
	for n := 0; n < bench.N; n++ {
		pf.Packet()
	}
}
