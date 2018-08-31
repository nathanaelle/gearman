package gearman // import "github.com/nathanaelle/gearman"

import (
	"sync"

	"github.com/nathanaelle/gearman/v2/protocol"
)

type (
	// PacketEmiter is a interface to send packet to a socket pool
	PacketEmiter interface {
		Send(...protocol.Packet)
	}

	// Message is the structure for communication between a pool and a Client or a Worker
	Message struct {
		Reply  PacketEmiter
		Server Conn
		Pkt    protocol.Packet
	}

	chanPacketEmiter struct {
		c chan<- protocol.Packet
	}

	funcPacketEmiter struct {
		lock         *sync.Mutex
		packetEmiter func(protocol.Packet, PacketEmiter)
	}
)

var _ PacketEmiter = chanPacketEmiter{}
var _ PacketEmiter = &funcPacketEmiter{}

func newChanPacketEmiter(c chan<- protocol.Packet) PacketEmiter {
	return chanPacketEmiter{c}
}

func (cpe chanPacketEmiter) Send(pkts ...protocol.Packet) {
	for _, pkt := range pkts {
		cpe.c <- pkt
	}
}

func newFuncPacketEmiter(pe func(protocol.Packet, PacketEmiter)) PacketEmiter {
	return &funcPacketEmiter{
		packetEmiter: pe,
	}
}

func (fpe *funcPacketEmiter) Send(pkts ...protocol.Packet) {
	for _, pkt := range pkts {
		fpe.packetEmiter(pkt, fpe)
	}
}
