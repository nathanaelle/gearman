package	gearman

import	(
	"io"
	"fmt"
	"net"
	"time"
	"sync"
)


type	(

	Conn	interface {
		io.Writer
		io.Reader
		SetReadDeadline(time.Time)
		Redial()
		String() string
	}


	netConn	struct {
		network,address string
		conn	net.Conn
	}

	testConn	struct {
		sync.Mutex
		r	[]byte
		w	[]byte
		r_ready	chan struct{}
		w_ready	chan struct{}
	}
)




func NetConn(network,address string) Conn {
 	return	&netConn{
		network: network,
		address: address,
	}
}


func (nc *netConn)String() string {
	return	fmt.Sprintf("%s[%s]", nc.network, nc.address)
}

func (nc *netConn)Redial() {
	if nc.conn != nil {
		nc.conn.Close()
	}

	nc.conn,_ = net.Dial(nc.network, nc.address)
}


func (nc *netConn)Read(b []byte) (int, error) {
	return	nc.conn.Read(b)
}

func (nc *netConn)SetReadDeadline(t time.Time) {
	nc.conn.SetReadDeadline(t)
}


func (nc *netConn)Write(b []byte) (int, error) {
	return	nc.conn.Write(b)
}



func TestConn() *testConn {
	return	&testConn {
		r_ready:	make(chan struct{},100),
		w_ready:	make(chan struct{},100),
	}

}

func (nc *testConn)String() string {
	return	"test conn"
}

func (nc *testConn)Redial() {
}


func (nc *testConn)Read(b []byte) (int, error) {
	<-nc.r_ready
	nc.Lock()
	defer nc.Unlock()

	if len(b) < len(nc.r) {
		copy(b, nc.r[0:len(b)])
		nc.r = nc.r[len(b):]
		nc.r_ready <- struct{}{}
		return len(b), nil
	}

	copy(b[0:len(nc.r)], nc.r)
	r := len(nc.r)
	nc.r = nc.r[0:0]

	return r, nil

}

func (nc *testConn)SetReadDeadline(_ time.Time) {
}


func (nc *testConn)Write(b []byte) (int,error) {
	nc.Lock()
	defer nc.Unlock()

	nc.w = append(nc.w, b...)
	nc.w_ready <- struct{}{}
	return len(b),nil
}

func (nc *testConn)Received() (b []byte) {
	<-nc.w_ready
	nc.Lock()
	defer nc.Unlock()

	b = nc.w
	nc.w = nc.w[0:0]

	return
}


func (nc *testConn)Send(b []byte) {
	nc.Lock()
	defer nc.Unlock()

	nc.r = append(nc.r, b...)
	nc.r_ready <- struct{}{}
}
