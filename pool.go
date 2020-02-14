package gearman // import "github.com/nathanaelle/gearman/v2"

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nathanaelle/gearman/v2/protocol"
)

var (
	IOTimeout    = 2 * time.Second
	RetryTimeout = 50 * time.Millisecond
)

type pool struct {
	sync.Mutex
	pool     map[Conn]PacketEmiter
	msgQueue chan<- Message
	ctx      context.Context
	handlers map[string]int32
}

func (p *pool) newPool(ctx context.Context, msgQueue chan<- Message) {
	p.pool = make(map[Conn]PacketEmiter)
	p.handlers = make(map[string]int32)
	p.msgQueue = msgQueue
	p.ctx = ctx
}

func (p *pool) addServer(server Conn) (PacketEmiter, error) {
	p.Lock()

	if _, ok := p.pool[server]; ok {
		p.Unlock()
		return nil, errors.New("server already exists: " + server.String())
	}

	pktemiter := p.packetEmiter(server)
	p.pool[server] = pktemiter
	p.Unlock()

	p.reconnect(server, pktemiter)
	go p.rloop(server, pktemiter)

	return pktemiter, nil
}

func (p *pool) listServers() []Conn {
	p.Lock()
	defer p.Unlock()

	list := make([]Conn, 0, len(p.pool))
	for k := range p.pool {
		list = append(list, k)
	}

	return list
}

func (p *pool) addHandler(h string) {
	p.Lock()
	defer p.Unlock()

	if _, ok := p.handlers[h]; !ok {
		p.handlers[h] = 0
	}

	canDo := protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte(h)))
	for _, server := range p.pool {
		server.Send(canDo)
	}
}

func (p *pool) delHandler(h string) {
	p.Lock()
	defer p.Unlock()

	cantDo := protocol.BuildPacket(protocol.CantDo, protocol.Opacify([]byte(h)))
	for _, server := range p.pool {
		server.Send(cantDo)
	}
}

func (p *pool) delAllHandlers() {
	p.Lock()
	defer p.Unlock()

	for h := range p.handlers {
		cantDo := protocol.BuildPacket(protocol.CantDo, protocol.Opacify([]byte(h)))
		for _, server := range p.pool {
			server.Send(cantDo)
		}
	}
}

func (p *pool) reconnect(server Conn, pktemiter PacketEmiter) {
	p.Lock()
	defer p.Unlock()

	server.Redial()

	for h := range p.handlers {
		pktemiter.Send(protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte(h))))
	}

	p.msgQueue <- Message{pktemiter, server, protocol.PktInternalEchoPacket}
}

func (p *pool) rloop(server Conn, pktemiter PacketEmiter) {
	var err error
	var pkt protocol.Packet
	defer server.Close()

	pf := protocol.NewPacketFactory(server, 1<<16)

	for {
		select {
		case <-p.ctx.Done():
			return

		default:
			server.SetReadDeadline(time.Now().Add(IOTimeout))
			pkt, err = pf.Packet()

			switch {
			case err == nil:
				p.msgQueue <- Message{pktemiter, server, pkt}

			case isTimeout(err):

			case isEOF(err):
				p.reconnect(server, pktemiter)

			default:
				time.Sleep(IOTimeout)
				// log.Println(err)
			}
		}
	}
}

func (p *pool) packetEmiter(server Conn) PacketEmiter {
	lock := &sync.Mutex{}

	return newFuncPacketEmiter(func(data protocol.Packet, pe PacketEmiter) {
		// log.Printf("  lock for %v\n", data.Cmd())
		lock.Lock()
		// defer log.Printf("unlock for %v\n", data.Cmd())
		defer lock.Unlock()

		server.SetWriteDeadline(time.Now().Add(IOTimeout))
		_, err := data.WriteTo(server)

		for err != nil {
			// log.Println(err)
			p.reconnect(server, pe)
			server.SetWriteDeadline(time.Now().Add(IOTimeout))
			_, err = data.WriteTo(server)
		}
	})
}
