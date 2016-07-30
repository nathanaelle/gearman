package gearman

import (
	"io"
	"net"
	"testing"
)

func trivialWorker(t *testing.T, end chan struct{}, srv ...Conn) {
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

	<-end
	for _,s := range srv {
		s.Close()
	}
}

func Test_Worker_simple(t *testing.T) {
	end := make(chan struct{})
	defer close(end)

	srv := ConnTest()
	go trivialWorker(t, end, srv)


	if !valid_any_step(t, srv.Received(), BuildPacket(CAN_DO, []byte("reverse")), pre_sleep ) {
		return
	}

	if !valid_any_step(t, srv.Received(), BuildPacket(CAN_DO, []byte("reverse")), pre_sleep ) {
		return
	}

	srv.Send(noop)
	if !valid_step(t, srv.Received(), grab_job) {
		return
	}

	if !valid_step(t, srv.Received(), grab_job_uniq) {
		return
	}

	srv.Send(no_job)
	srv.Send(BuildPacket(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test")))
	if !valid_step(t, srv.Received(), pre_sleep) {
		return
	}
	if !valid_step(t, srv.Received(), BuildPacket(WORK_COMPLETE_WRK, []byte("H:lap:1"), []byte("tset"))) {
		return
	}
}

func Test_Worker_two_servers(t *testing.T) {
	end := make(chan struct{})
	defer close(end)

	srv1 := ConnTest()
	srv2 := ConnTest()
	go trivialWorker(t, end, srv1, srv2)

	for _, srv := range []*testConn{srv1, srv2} {
		if !valid_any_step(t, srv.Received(), BuildPacket(CAN_DO, []byte("reverse")), pre_sleep ) {
			return
		}

		if !valid_any_step(t, srv.Received(), BuildPacket(CAN_DO, []byte("reverse")), pre_sleep ) {
			return
		}
	}

	srv2.Send(noop)
	srv1.Send(noop)

	for _, srv := range []*testConn{srv1, srv2} {
		if !valid_step(t, srv.Received(), grab_job) {
			return
		}
		if !valid_step(t, srv.Received(), grab_job_uniq) {
			return
		}
	}

	srv1.Send(no_job)
	srv2.Send(no_job)

	srv1.Send(BuildPacket(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test srv1")))
	srv2.Send(BuildPacket(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test srv2")))

	rec := srv1.Received()
	if !valid_any_step(t, rec, pre_sleep, BuildPacket(WORK_COMPLETE_WRK, []byte("H:lap:1"), []byte("1vrs tset"))) {
		return
	}

	rec = srv1.Received()
	if !valid_any_step(t, rec, pre_sleep, BuildPacket(WORK_COMPLETE_WRK, []byte("H:lap:1"), []byte("1vrs tset"))) {
		return
	}

	rec = srv2.Received()
	if !valid_any_step(t, rec, pre_sleep, BuildPacket(WORK_COMPLETE_WRK, []byte("H:lap:1"), []byte("2vrs tset"))) {
		return
	}

	rec = srv2.Received()
	if !valid_any_step(t, rec, pre_sleep, BuildPacket(WORK_COMPLETE_WRK, []byte("H:lap:1"), []byte("2vrs tset"))) {
		return
	}
}


func Test_Worker_netcon(t *testing.T) {
	end := make(chan struct{})

	l, err := net.Listen("tcp", "localhost:60000")
	if err != nil {
		t.Errorf("got error %+v", err)
		return
	}
	defer l.Close()
	defer close(end)

	go trivialWorker(t, end, NetConn("tcp", "localhost:60000"))

	nb_test := 0
	for {
		select {
		case <-end:
			return

		default:
			conn, err := l.Accept()
			if err != nil {
				t.Errorf("got error %+v", err)
				return
			}
			nb_test++
			if nb_test > 10 {
				return
			}

			go func(c net.Conn) {
				defer c.Close()

				if !packet_received_is_any(t, c, BuildPacket(CAN_DO, []byte("reverse")),pre_sleep) {
					return
				}

				if !packet_received_is_any(t, c, BuildPacket(CAN_DO, []byte("reverse")),pre_sleep) {
					return
				}

				WritePacket(c, noop)

				if !packet_received_is(t, c, grab_job) {
					return
				}

				if !packet_received_is(t, c, grab_job_uniq) {
					return
				}

				WritePacket(c, no_job)
				WritePacket(c, BuildPacket(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test")))

				if !packet_received_is(t, c, pre_sleep) {
					return
				}

				if !packet_received_is(t, c, BuildPacket(WORK_COMPLETE_WRK, []byte("H:lap:1"), []byte("tset"))) {
					return
				}
			}(conn)
		}
	}
}

//*/
