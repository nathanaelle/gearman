package gearman

import (
	"io"
	_ "os"
	_ "log"
	"net"
	"testing"
)



func trivialWorker(t *testing.T,end chan struct{}, srv ...Conn)  {
	w	:= NewWorker(end, nil)
	w.AddServers( srv... )
	w.AddHandler("reverse", JobHandler(func(payload io.Reader,reply io.Writer) (error){
		buff	:= make([]byte,1<<16)
		s,_	:= payload.Read(buff)
		buff	= buff[0:s]

		for i:=len(buff); i>0; i-- {
			reply.Write([]byte{ buff[i-1] })
		}

		return nil
	} ))

	<-end
}




func Test_Worker_simple(t *testing.T) {
	end	:= make(chan struct{})
	defer	close(end)

	srv	:= ConnTest()
	go trivialWorker(t,end, srv)

	if !valid_step(t, srv.Received(), can_do("reverse").Marshal()) {
		return
	}

	if !valid_step(t, srv.Received(), pre_sleep.Marshal()) {
		return
	}

	srv.Send(noop.Marshal())
	if !valid_step(t, srv.Received(), grab_job.Marshal()) {
		return
	}

	if !valid_step(t, srv.Received(), grab_job_uniq.Marshal()) {
		return
	}

	srv.Send(no_job.Marshal())
	srv.Send(res_packet(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test") ).Marshal())
	if !valid_step(t, srv.Received(), pre_sleep.Marshal()) {
		return
	}
	if !valid_step(t, srv.Received(), res_packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("tset") ).Marshal()) {
		return
	}
}


func Test_Worker_two_servers(t *testing.T) {
	end	:= make(chan struct{})
	defer	close(end)

	srv1	:= ConnTest()
	srv2	:= ConnTest()
	go trivialWorker(t,end, srv1, srv2)

	for _,srv := range []*testConn{ srv1, srv2 } {
		if !valid_step(t, srv.Received(), can_do("reverse").Marshal()) {
			return
		}

		if !valid_step(t, srv.Received(), pre_sleep.Marshal()) {
			return
		}

	}

	srv2.Send(noop.Marshal())
	srv1.Send(noop.Marshal())

	for _,srv := range []*testConn{ srv1, srv2 } {
		if !valid_step(t, srv.Received(), grab_job.Marshal()) {
			return
		}
		if !valid_step(t, srv.Received(), grab_job_uniq.Marshal()) {
			return
		}
	}

	srv1.Send(no_job.Marshal())
	srv2.Send(no_job.Marshal())

	srv1.Send(res_packet(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test srv1") ).Marshal())
	srv2.Send(res_packet(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test srv2") ).Marshal())

	rec := srv1.Received()
	if !valid_any_step(t, rec, pre_sleep.Marshal(), res_packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("1vrs tset") ).Marshal()) {
		return
	}

	rec = srv1.Received()
	if !valid_any_step(t, rec, pre_sleep.Marshal(), res_packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("1vrs tset") ).Marshal()) {
		return
	}

	rec = srv2.Received()
	if !valid_any_step(t, rec, pre_sleep.Marshal(), res_packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("2vrs tset") ).Marshal()) {
		return
	}

	rec = srv2.Received()
	if !valid_any_step(t, rec, pre_sleep.Marshal(), res_packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("2vrs tset") ).Marshal()) {
		return
	}
}


func Test_Worker_netcon(t *testing.T) {
	end	:= make(chan struct{})

	l, err := net.Listen("tcp", "localhost:60000")
	if err != nil {
		t.Errorf("got error %+v", err )
		return
	}
	defer	l.Close()
	defer	close(end)


	go trivialWorker(t,end, NetConn("tcp", "localhost:60000"))

	nb_test := 0
	for {
		select {
		case <-end:
			return
		default:
			conn, err := l.Accept()
			if err != nil {
				t.Errorf("got error %+v", err )
				return
			}
			nb_test++
			if  nb_test >10 {
				return
			}

			go func(c net.Conn) {
				var pkt Packet
				var err error

				if pkt,err = ReadPacket(c); err != nil {
					t.Errorf("got error %+v", err )
					return
				}
				if !valid_step(t, pkt.Marshal(), can_do("reverse").Marshal()) {
					return
				}

				if pkt,err = ReadPacket(c); err != nil {
					t.Errorf("got error %+v", err )
					return
				}
				if !valid_step(t, pkt.Marshal(), pre_sleep.Marshal()) {
					return
				}

				WritePacket(c, noop)

				if pkt,err = ReadPacket(c); err != nil {
					t.Errorf("got error %+v", err )
					return
				}
				if !valid_step(t, pkt.Marshal(), grab_job.Marshal()) {
					return
				}

				if pkt,err = ReadPacket(c); err != nil {
					t.Errorf("got error %+v", err )
					return
				}
				if !valid_step(t, pkt.Marshal(), grab_job_uniq.Marshal()) {
					return
				}

				WritePacket(c, no_job)
				WritePacket(c, res_packet(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test") ))

				if pkt,err = ReadPacket(c); err != nil {
					t.Errorf("got error %+v", err )
					return
				}
				if !valid_step(t, pkt.Marshal(), pre_sleep.Marshal()) {
					return
				}

				if pkt,err = ReadPacket(c); err != nil {
					t.Errorf("got error %+v", err )
					return
				}
				if !valid_step(t, pkt.Marshal(), res_packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("tset") ).Marshal()) {
					return
				}
			}(conn)
		}
	}


}
