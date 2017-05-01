package	gearman // import "github.com/nathanaelle/gearman"

import	(
	"io"
	"fmt"
	"net"
	"time"
)


type	(
	nullConn	struct {
		network,address string
	}
)



func (nc *nullConn)Close() error {
	return nil
}

func (nc *nullConn)String() string {
	return	fmt.Sprintf("nullconn %s[%s]", nc.network, nc.address)
}

func (nc *nullConn)Read(b []byte) (int, error) {
	return	0, io.EOF
}

func (nc *nullConn)SetReadDeadline(t time.Time) error {
	return	nil
}

func (nc *nullConn)SetWriteDeadline(t time.Time) error {
	return	nil
}

func (nc *nullConn)SetDeadline(t time.Time) error {
	return	nil
}

func (nc *nullConn)Write(b []byte) (int, error) {
	return	0, io.EOF
}

func (nc *nullConn)LocalAddr() net.Addr {
	return	nil
}

func (nc *nullConn)RemoteAddr() net.Addr {
	return	nil
}
