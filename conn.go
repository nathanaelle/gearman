package	gearman

import	(
	"io"
	"fmt"
	"net"
	"time"
	"sync/atomic"
)


type	(
	Conn	interface {
		io.Writer
		io.Reader
		SetReadDeadline(time.Time)
		SetWriteDeadline(time.Time)
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
)


func NetConn(network,address string) Conn {
 	nc := &netConn{
		counter:	new(int32),
		network:	network,
		address:	address,
	}

	return nc
}


func (nc *netConn)String() string {
	return	fmt.Sprintf("%s[%s]", nc.network, nc.address)
}

func (nc *netConn)Redial() {
	var err error
	if nc.conn != nil {
		nc.conn.Close()
	}

	nc.conn,err = net.Dial(nc.network, nc.address)
	for err != nil {
		time.Sleep(100*time.Millisecond)
		nc.conn,err = net.Dial(nc.network, nc.address)
	}
}


func (nc *netConn)Read(b []byte) (int, error) {
	if nc.conn == nil {
		nc.Redial()
	}
	return	nc.conn.Read(b)
}

func (nc *netConn)SetReadDeadline(t time.Time) {
	if nc.conn == nil {
		nc.Redial()
	}
	nc.conn.SetReadDeadline(t)
}

func (nc *netConn)SetWriteDeadline(t time.Time) {
	if nc.conn == nil {
		nc.Redial()
	}
	nc.conn.SetWriteDeadline(t)
}


func (nc *netConn)Write(b []byte) (int, error) {
	if nc.conn == nil {
		nc.Redial()
	}
	return	nc.conn.Write(b)
}

func (nc *netConn)CounterAdd(d int32) {
	atomic.AddInt32(nc.counter, d)
}

func (nc *netConn)IsZeroCounter() bool {
	return	atomic.LoadInt32(nc.counter) == 0
}
