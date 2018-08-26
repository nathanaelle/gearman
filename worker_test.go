package gearman // import "github.com/nathanaelle/gearman"

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/nathanaelle/gearman/v2/protocol"
)

func trivialWorker(end context.Context, t *testing.T, srv ...Conn) {
	w := NewWorker(end, nil)
	w.AddServers(srv...)
	w.AddHandler("reverse", JobHandler(func(payload io.Reader, reply io.Writer) error {
		buff := make([]byte, 1<<16)
		s, _ := payload.Read(buff)
		buff = buff[0:s]

		for i := len(buff); i > 0; i-- {
			reply.Write([]byte{buff[i-1]})
		}

		return nil
	}))

	<-end.Done()
	for _, s := range srv {
		s.Close()
	}
}

func Test_Worker_simple(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := connTest()
	go trivialWorker(end, t, srv)

	if !validAnyStep(t, srv.Received(), protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte("reverse"))), protocol.PktPreSleep) {
		return
	}

	if !validAnyStep(t, srv.Received(), protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte("reverse"))), protocol.PktPreSleep) {
		return
	}

	srv.Send(protocol.PktNoop)
	if !validStep(t, srv.Received(), protocol.PktGrabJob) {
		return
	}

	if !validStep(t, srv.Received(), protocol.PktGrabJobUniq) {
		return
	}

	srv.Send(protocol.PktNoJob)
	srv.Send(protocol.BuildPacket(protocol.JobAssign, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("test"))))
	if !validStep(t, srv.Received(), protocol.PktPreSleep) {
		return
	}
	if !validStep(t, srv.Received(), protocol.BuildPacket(protocol.WorkCompleteWorker, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("tset")))) {
		return
	}
}

func Test_Worker_two_servers(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv1 := connTest()
	srv2 := connTest()
	go trivialWorker(end, t, srv1, srv2)

	for _, srv := range []*testConn{srv1, srv2} {
		if !validAnyStep(t, srv.Received(), protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte("reverse"))), protocol.PktPreSleep) {
			return
		}

		if !validAnyStep(t, srv.Received(), protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte("reverse"))), protocol.PktPreSleep) {
			return
		}
	}

	srv2.Send(protocol.PktNoop)
	srv1.Send(protocol.PktNoop)

	for _, srv := range []*testConn{srv1, srv2} {
		if !validStep(t, srv.Received(), protocol.PktGrabJob) {
			return
		}
		if !validStep(t, srv.Received(), protocol.PktGrabJobUniq) {
			return
		}
	}

	srv1.Send(protocol.PktNoJob)
	srv2.Send(protocol.PktNoJob)

	srv1.Send(protocol.BuildPacket(protocol.JobAssign, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("test srv1"))))
	srv2.Send(protocol.BuildPacket(protocol.JobAssign, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("test srv2"))))

	rec := srv1.Received()
	if !validAnyStep(t, rec, protocol.PktPreSleep, protocol.BuildPacket(protocol.WorkCompleteWorker, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("1vrs tset")))) {
		return
	}

	rec = srv1.Received()
	if !validAnyStep(t, rec, protocol.PktPreSleep, protocol.BuildPacket(protocol.WorkCompleteWorker, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("1vrs tset")))) {
		return
	}

	rec = srv2.Received()
	if !validAnyStep(t, rec, protocol.PktPreSleep, protocol.BuildPacket(protocol.WorkCompleteWorker, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("2vrs tset")))) {
		return
	}

	rec = srv2.Received()
	if !validAnyStep(t, rec, protocol.PktPreSleep, protocol.BuildPacket(protocol.WorkCompleteWorker, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("2vrs tset")))) {
		return
	}
}

func Test_Worker_netcon(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
		return
	}
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	l, err := net.Listen("tcp", "localhost:60000")
	if err != nil {
		t.Errorf("got error %+v", err)
		return
	}
	defer l.Close()

	go trivialWorker(end, t, NetConn("tcp", "localhost:60000"))

	nbTests := 0
	for {
		select {
		case <-end.Done():
			return

		default:
			conn, err := l.Accept()
			if err != nil {
				t.Errorf("got error %+v", err)
				return
			}
			nbTests++
			if nbTests > 10 {
				return
			}

			go func(c net.Conn) {
				defer c.Close()

				pf := protocol.NewPacketFactory(c, 1<<16)
				if !packetReceivedIsAny(t, pf, protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte("reverse"))), protocol.PktPreSleep) {
					return
				}

				if !packetReceivedIsAny(t, pf, protocol.BuildPacket(protocol.CanDo, protocol.Opacify([]byte("reverse"))), protocol.PktPreSleep) {
					return
				}

				protocol.PktNoop.WriteTo(c)

				if !packetReceivedIs(t, pf, protocol.PktGrabJob) {
					return
				}

				if !packetReceivedIs(t, pf, protocol.PktGrabJobUniq) {
					return
				}

				protocol.PktNoJob.WriteTo(c)
				protocol.BuildPacket(protocol.JobAssign, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("test"))).WriteTo(c)

				if !packetReceivedIs(t, pf, protocol.PktPreSleep) {
					return
				}

				if !packetReceivedIs(t, pf, protocol.BuildPacket(protocol.WorkCompleteWorker, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("tset")))) {
					return
				}
			}(conn)
		}
	}
}

//*/
