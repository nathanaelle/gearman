package gearman

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type (
	testConn struct {
		sync.Mutex
		counter *int32
		r       []byte
		w       []byte
		r_ready chan []byte
		w_ready chan []byte
	}

	testNetConn struct {
		net.Conn
		counter *int32
	}
)

func ConnTest() *testConn {
	return &testConn{
		counter: new(int32),
		r_ready: make(chan []byte, 10),
		w_ready: make(chan []byte, 10),
	}

}

func (nc *testConn) CounterAdd(d int32) {
	atomic.AddInt32(nc.counter, d)
}

func (nc *testConn) IsZeroCounter() bool {
	return atomic.LoadInt32(nc.counter) == 0
}

func (nc *testConn) String() string {
	return "test conn"
}

func (nc *testConn) Redial() {
}


func (nc *testConn) Close() error {
	return nil
}


func (nc *testConn) Read(b []byte) (int, error) {
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

func (nc *testConn) SetReadDeadline(_ time.Time) {
}

func (nc *testConn) SetWriteDeadline(_ time.Time) {
}

func (nc *testConn) Write(b []byte) (int, error) {
	nc.w_ready <- b
	return len(b), nil
}

func (nc *testConn) Received() (b []byte) {
	return <-nc.w_ready
}

func (nc *testConn) Send(b Packet) {
	nc.r_ready <- b.Marshal()
}

func (nc *testConn) SendByte(b []byte) {
	nc.r_ready <- b
}

func NetConnTest(c net.Conn) *testNetConn {
	return &testNetConn{c, new(int32)}
}

func (nc *testNetConn) CounterAdd(d int32) {
	atomic.AddInt32(nc.counter, d)
}

func (nc *testNetConn) IsZeroCounter() bool {
	return atomic.LoadInt32(nc.counter) == 0
}

func (nc *testNetConn) String() string {
	return "testNetConn"
}

func (nc *testNetConn) Redial() {
}
