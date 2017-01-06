package	gearman // import "github.com/nathanaelle/gearman"

import	(
	"sync"
	"time"
	"errors"
)



type (
	Message	struct {
		Reply	chan<- Packet
		Server	Conn
		Pkt	Packet
	}


)


type pool struct {
	sync.Mutex
	pool		map[Conn] chan Packet
	s_queue		chan<- Message
	r_end		<-chan struct{}
	handlers	map[string]int32
}


func (p *pool)new(s_queue chan<- Message, r_end <-chan struct{}) {
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

	p.pool[server]=make(chan Packet,100)
	p.Unlock()

	go p.wloop(server,p.pool[server])
	p.reconnect(server)
	go p.rloop(server)

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


func (p *pool)add_handler(h string) {
	p.Lock()
	defer 	p.Unlock()

	if _,ok := p.handlers[h]; !ok {
		p.handlers[h] = 0
	}

	can_do := BuildPacket(CAN_DO, Opacify([]byte(h)))
	for _,server := range p.pool {
		server <- can_do
	}
}


func (p *pool)del_handler(h string) {
	p.Lock()
	defer 	p.Unlock()

	cant_do := BuildPacket(CANT_DO, Opacify([]byte(h)))
	for _,server := range p.pool {
		server <- cant_do
	}
}

func (p *pool)del_all_handlers() {
	p.Lock()
	defer 	p.Unlock()

	for h,_ := range p.handlers {
		cant_do	:= BuildPacket(CANT_DO, Opacify([]byte(h)))
		for _,server := range p.pool {
			server <- cant_do
		}
	}
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
		p.pool[server] <- BuildPacket(CAN_DO, Opacify([]byte(h)))
	}

	p.s_queue <- Message{ p.pool[server], server, internal_echo_packet }
}


func (p *pool)rloop(server Conn) {
	var	err	error
	var	pkt	Packet
	defer	server.Close()

	pf:= NewPacketFactory(server, 1<<20)

	for {
		select	{
		case	<-p.r_end:
			return

		default:
			server.SetReadDeadline(time.Now().Add(100*time.Millisecond))
			pkt,err	= pf.Packet()

			switch	{
			case	err == nil:
				p.s_queue <- Message{ p.pool[server], server, pkt }

			case	is_timeout(err):

			case	is_eof(err):
				p.reconnect(server)

			default:
				time.Sleep(5*time.Second)
//				log.Println(err)
			}
		}
	}
}


func (p *pool)wloop(server Conn,send_to <-chan Packet) {
	var	err	error
	defer	server.Close()

	for {
		select	{
		case	<-p.r_end:
			reset_abilities.WriteTo(server)
			return

		case	data := <-send_to:
			server.SetWriteDeadline(time.Now().Add(100*time.Millisecond))
			_, err = data.WriteTo(server)

			for err != nil {
//				log.Println(err)
				p.reconnect(server)
				server.SetWriteDeadline(time.Now().Add(100*time.Millisecond))
				_,err	= data.WriteTo(server)
			}
		}
	}
}
