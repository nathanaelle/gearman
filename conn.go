package	gearman

import	(
	"io"
	"fmt"
	"net"
	"time"
	"sync"
	"sync/atomic"
)


type	(
	Conn	interface {
		io.Writer
		io.Reader
		SetReadDeadline(time.Time)
		Redial()
		String() string
		CounterAdd(int32)
		IsZeroCounter() bool
	}

	netConn	struct {
		counter		*int32
		network,address string
		conn		net.Conn
	}

	testConn	struct {
		sync.Mutex
		counter		*int32
		r		[]byte
		w		[]byte
		r_ready		chan []byte
		w_ready		chan []byte
	}
)


func NetConn(network,address string) Conn {
 	return	&netConn{
		counter:	new(int32),
		network:	network,
		address:	address,
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

func (nc *netConn)CounterAdd(d int32) {
	atomic.AddInt32(nc.counter, d)
}

func (nc *netConn)IsZeroCounter() bool {
	return	atomic.LoadInt32(nc.counter) == 0
}





func TestConn() *testConn {
	return	&testConn {
		counter:	new(int32),
		r_ready:	make(chan []byte,10),
		w_ready:	make(chan []byte,10),
	}

}


func (nc *testConn)CounterAdd(d int32) {
	atomic.AddInt32(nc.counter, d)
}

func (nc *testConn)IsZeroCounter() bool {
	return	atomic.LoadInt32(nc.counter) == 0
}


func (nc *testConn)String() string {
	return	"test conn"
}

func (nc *testConn)Redial() {
}


func (nc *testConn)Read(b []byte) (int, error) {
	nc.Lock()
	defer nc.Unlock()

	if len(nc.r) == 0 {
		nc.r = <-nc.r_ready
	}

	if len(b) < len(nc.r) {
		copy(b, nc.r[0:len(b)])
		nc.r = nc.r[len(b):]
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
	nc.w_ready <- b
	return len(b),nil
}

func (nc *testConn)Received() (b []byte) {
	return <- nc.w_ready
}


func (nc *testConn)Send(b []byte) {
	nc.r_ready <- b
}
