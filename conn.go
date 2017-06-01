package	gearman // import "github.com/nathanaelle/gearman"

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
		io.Closer
		SetReadDeadline(time.Time)
		SetWriteDeadline(time.Time)
		Redial()
		String() string
		CounterAdd(int32)
		IsZeroCounter() bool
	}

	netConn	struct {
		lock		*sync.Mutex
		closed		int32
		counter		int32
		network,address string
		conn		atomic.Value
	}
)


func NetConn(network,address string) Conn {
 	nc := &netConn{
		lock:		new(sync.Mutex),
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
	conn := nc.conn.Load()
	if conn != nil {
		return	conn.(io.Closer).Close()
	}
	return	nil
}


func (nc *netConn)String() string {
	return	fmt.Sprintf("%s[%s]", nc.network, nc.address)
}

func (nc *netConn)Redial() {
	if conn := nc.conn.Load(); conn != nil {
		conn.(io.Closer).Close()
	}

	if nc.isNotClosed() {
		conn,err := net.Dial(nc.network, nc.address)
		if conn != nil {
			nc.conn.Store(conn)
		}
		if err != nil {
			time.Sleep(RetryTimeout)
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
	for nc.isNotClosed() {
		if c := nc.conn.Load(); c != nil {
			if conn, ok := c.(net.Conn); ok {
				return	conn
			}
		}
		time.Sleep(RetryTimeout)
	}
	return	&nullConn{ nc.network, nc.address }
}
