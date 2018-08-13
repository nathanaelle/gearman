package gearman // import "github.com/nathanaelle/gearman"

import (
	"context"
	"io"
	"net"
	"testing"
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

	if !validAnyStep(t, srv.Received(), BuildPacket(CAN_DO, Opacify([]byte("reverse"))), preSleep) {
		return
	}

	if !validAnyStep(t, srv.Received(), BuildPacket(CAN_DO, Opacify([]byte("reverse"))), preSleep) {
		return
	}

	srv.Send(noop)
	if !validStep(t, srv.Received(), grabJob) {
		return
	}

	if !validStep(t, srv.Received(), grabJobUniq) {
		return
	}

	srv.Send(noJob)
	srv.Send(BuildPacket(JOB_ASSIGN, Opacify([]byte("H:lap:1")), Opacify([]byte("reverse")), Opacify([]byte("test"))))
	if !validStep(t, srv.Received(), preSleep) {
		return
	}
	if !validStep(t, srv.Received(), BuildPacket(WORK_COMPLETE_WRK, Opacify([]byte("H:lap:1")), Opacify([]byte("tset")))) {
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
		if !validAnyStep(t, srv.Received(), BuildPacket(CAN_DO, Opacify([]byte("reverse"))), preSleep) {
			return
		}

		if !validAnyStep(t, srv.Received(), BuildPacket(CAN_DO, Opacify([]byte("reverse"))), preSleep) {
			return
		}
	}

	srv2.Send(noop)
	srv1.Send(noop)

	for _, srv := range []*testConn{srv1, srv2} {
		if !validStep(t, srv.Received(), grabJob) {
			return
		}
		if !validStep(t, srv.Received(), grabJobUniq) {
			return
		}
	}

	srv1.Send(noJob)
	srv2.Send(noJob)

	srv1.Send(BuildPacket(JOB_ASSIGN, Opacify([]byte("H:lap:1")), Opacify([]byte("reverse")), Opacify([]byte("test srv1"))))
	srv2.Send(BuildPacket(JOB_ASSIGN, Opacify([]byte("H:lap:1")), Opacify([]byte("reverse")), Opacify([]byte("test srv2"))))

	rec := srv1.Received()
	if !validAnyStep(t, rec, preSleep, BuildPacket(WORK_COMPLETE_WRK, Opacify([]byte("H:lap:1")), Opacify([]byte("1vrs tset")))) {
		return
	}

	rec = srv1.Received()
	if !validAnyStep(t, rec, preSleep, BuildPacket(WORK_COMPLETE_WRK, Opacify([]byte("H:lap:1")), Opacify([]byte("1vrs tset")))) {
		return
	}

	rec = srv2.Received()
	if !validAnyStep(t, rec, preSleep, BuildPacket(WORK_COMPLETE_WRK, Opacify([]byte("H:lap:1")), Opacify([]byte("2vrs tset")))) {
		return
	}

	rec = srv2.Received()
	if !validAnyStep(t, rec, preSleep, BuildPacket(WORK_COMPLETE_WRK, Opacify([]byte("H:lap:1")), Opacify([]byte("2vrs tset")))) {
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

				pf := NewPacketFactory(c, 1<<16)
				if !packetReceivedIsAny(t, pf, BuildPacket(CAN_DO, Opacify([]byte("reverse"))), preSleep) {
					return
				}

				if !packetReceivedIsAny(t, pf, BuildPacket(CAN_DO, Opacify([]byte("reverse"))), preSleep) {
					return
				}

				noop.WriteTo(c)

				if !packetReceivedIs(t, pf, grabJob) {
					return
				}

				if !packetReceivedIs(t, pf, grabJobUniq) {
					return
				}

				noJob.WriteTo(c)
				BuildPacket(JOB_ASSIGN, Opacify([]byte("H:lap:1")), Opacify([]byte("reverse")), Opacify([]byte("test"))).WriteTo(c)

				if !packetReceivedIs(t, pf, preSleep) {
					return
				}

				if !packetReceivedIs(t, pf, BuildPacket(WORK_COMPLETE_WRK, Opacify([]byte("H:lap:1")), Opacify([]byte("tset")))) {
					return
				}
			}(conn)
		}
	}
}

//*/
