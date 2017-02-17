package	gearman // import "github.com/nathanaelle/gearman"

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
		io.Closer
		SetReadDeadline(time.Time)
		SetWriteDeadline(time.Time)
		Redial()
		String() string
		CounterAdd(int32)
		IsZeroCounter() bool
	}

	netConn	struct {
		closed		int32
		counter		int32
		network,address string
		conn		atomic.Value
	}
)


func NetConn(network,address string) Conn {
 	nc := &netConn{
		network:	network,
		address:	address,
	}

	return nc
}


func (nc *netConn)Close() error {
	if !nc.isNotClosed() {
		return nil
	}
	atomic.AddInt32(&nc.closed, 1)
	conn := nc.nc()
	if conn != nil {
		return	conn.Close()
	}
	return	nil
}


func (nc *netConn)String() string {
	return	fmt.Sprintf("%s[%s]", nc.network, nc.address)
}

func (nc *netConn)Redial() {
	var err error
	conn := nc.nc()
	if conn != nil {
		conn.Close()
	}

	if nc.isNotClosed() {
		conn,err = net.Dial(nc.network, nc.address)
		nc.conn.Store(conn)
		if err != nil {
			fmt.Printf("!>	%v",err)
			time.Sleep(500*time.Millisecond)
		}
	}
}


func (nc *netConn)Read(b []byte) (int, error) {
	return	nc.nc().Read(b)
}

func (nc *netConn)SetReadDeadline(t time.Time) {
	nc.nc().SetReadDeadline(t)
}

func (nc *netConn)SetWriteDeadline(t time.Time) {
	nc.nc().SetWriteDeadline(t)
}


func (nc *netConn)Write(b []byte) (int, error) {
	return	nc.nc().Write(b)
}

func (nc *netConn)CounterAdd(d int32) {
	atomic.AddInt32(&nc.counter, d)
}

func (nc *netConn)IsZeroCounter() bool {
	return	atomic.LoadInt32(&nc.counter) == 0
}

func (nc *netConn)isNotClosed() bool {
	return	atomic.LoadInt32(&nc.closed) == 0
}

func (nc *netConn)nc() net.Conn {
	c := nc.conn.Load()
	if c == nil {
		return	nil
	}

	return	c.(net.Conn)
}
