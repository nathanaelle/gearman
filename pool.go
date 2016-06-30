package	gearman

import	(
	"sync"
	"time"
	"errors"
)



type pool struct {
	sync.Mutex
	pool		map[Conn] chan Packet
	s_queue		chan<- message
	r_end		<-chan struct{}
	handlers	map[string]int32
}


func (p *pool)new(s_queue chan<- message, r_end <-chan struct{}) {
	p.pool		= make(map[Conn] chan Packet)
	p.handlers	= make(map[string]int32)
	p.s_queue	= s_queue
	p.r_end		= r_end
}



func (p *pool)add_server(server Conn) error {
	p.Lock()

	if _,ok := p.pool[server]; ok {
		p.Unlock()
		return errors.New("server already exists: "+server.String())
	}

	p.pool[server]=make(chan Packet,10)
	p.Unlock()

	go p.rloop(server)
	go p.wloop(server,p.pool[server])
	p.reconnect(server)

	return nil
}


func (p *pool)list_servers() []Conn {
	p.Lock()
	defer 	p.Unlock()

	list	:= make([]Conn,0,len(p.pool))
	for k,_ := range p.pool {
		list = append(list, k)
	}

	return list
}


func (p *pool)add_handler(h string) error {
	p.Lock()
	defer 	p.Unlock()

	if _,ok := p.handlers[h]; ok {
		return errors.New("handler already exists: "+h)
	}
	p.handlers[h] = 0

	for _,server := range p.pool {
		server <- can_do(h)
	}
	return nil
}


func (p *pool)send_to(server Conn, pkt Packet) {
	p.Lock()
	defer 	p.Unlock()

	if c, ok := p.pool[server] ; ok {
		c <- pkt
	}
}

func (p *pool)reconnect(server Conn) {
	p.Lock()
	defer 	p.Unlock()

	server.Redial()

	for h,_ := range p.handlers {
		p.pool[server] <- can_do(h)
	}
}


func (p *pool)rloop(server Conn) {
	var	err	error
	var	pkt	Packet

	p.s_queue <- message{ p, server, internal_echo_packet }
	for {
		select	{
		case	<-p.r_end:
			return

		default:
			server.SetReadDeadline(time.Now().Add(10*time.Millisecond))
			pkt,err	= ReadPacket(server)

			for err != nil {
				p.reconnect(server)
				server.SetReadDeadline(time.Now().Add(10*time.Millisecond))
				pkt,err	= ReadPacket(server)
			}

			p.s_queue <- message{ p, server, pkt }
		}
	}
}


func (p *pool)wloop(server Conn,send_to <-chan Packet) {
	var	err	error

	for {
		select	{
		case	<-p.r_end:
			WritePacket(server, reset_abilities)
			return

		case	data := <-send_to:
			err	= WritePacket(server, data)
			for err != nil {
				p.reconnect(server)
				err	= WritePacket(server, data)
			}
		}
	}
}
