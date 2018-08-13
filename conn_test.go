package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type (
	testConn struct {
		sync.Mutex
		counter *int32
		sock    net.Conn
		mockup  net.Conn
	}

	testNetConn struct {
		net.Conn
		counter *int32
	}
)

func connPair() (net.Conn, net.Conn, error) {
	fds, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])

	sockF := os.NewFile(uintptr(fds[0]), "sock")
	defer sockF.Close()

	sock, err := net.FileConn(sockF)
	if err != nil {
		return nil, nil, err
	}

	mockF := os.NewFile(uintptr(fds[1]), "mockup")
	defer mockF.Close()
	mock, err := net.FileConn(mockF)
	if err != nil {
		sock.Close()
		return nil, nil, err
	}

	return sock, mock, nil
}

func connTest() *testConn {
	sock, mockup, err := connPair()
	if err != nil {
		panic(err)
	}

	return &testConn{
		counter: new(int32),
		sock:    sock,
		mockup:  mockup,
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
	nc.sock.Close()
	nc.mockup.Close()
	return nil
}

func (nc *testConn) Read(b []byte) (int, error) {
	return nc.sock.Read(b)
}

func (nc *testConn) SetReadDeadline(_ time.Time) {
}

func (nc *testConn) SetWriteDeadline(_ time.Time) {
}

func (nc *testConn) Write(b []byte) (int, error) {
	return nc.sock.Write(b)
}

func (nc *testConn) Received() []byte {
	buff := new(bytes.Buffer)

	if _, err := io.CopyN(buff, nc.mockup, 12); err != nil {
		panic(err)
	}
	t := int64(be2uint32((buff.Bytes())[8:12]))
	if t == 0 {
		return buff.Bytes()
	}

	if _, err := io.CopyN(buff, nc.mockup, t); err != nil {
		panic(err)
	}

	return buff.Bytes()
}

func (nc *testConn) Send(b Packet) {
	nc.mockup.Write(b.Marshal())
}

func (nc *testConn) SendByte(b []byte) {
	nc.mockup.Write(b)
}

func netConnTest(c net.Conn) *testNetConn {
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
