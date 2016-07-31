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
		close		bool
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
		close:		false,
	}

	return nc
}


func (nc *netConn)Close() error {
	nc.close = true
	if nc.conn != nil {
		return	nc.conn.Close()
	}
	return	nil
}


func (nc *netConn)String() string {
	return	fmt.Sprintf("%s[%s]", nc.network, nc.address)
}

func (nc *netConn)Redial() {
	var err error
	if nc.conn != nil {
		nc.conn.Close()
	}

	if !nc.close {
		nc.conn,err = net.Dial(nc.network, nc.address)
		if err != nil {
			fmt.Printf("!>	%v",err)
			time.Sleep(500*time.Millisecond)
		}
	}
}


func (nc *netConn)Read(b []byte) (int, error) {
	return	nc.conn.Read(b)
}

func (nc *netConn)SetReadDeadline(t time.Time) {
	nc.conn.SetReadDeadline(t)
}

func (nc *netConn)SetWriteDeadline(t time.Time) {
	nc.conn.SetWriteDeadline(t)
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
