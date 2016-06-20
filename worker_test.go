package gearman

import (
	"io"
	//"os"
	//"log"
	"bytes"
	"testing"
)



func	valid_step(t *testing.T, rcvd, expected []byte) bool {
	if !bytes.Equal(rcvd,expected) {
		t.Errorf("received %+v expected %+v", rcvd, expected)
		return false
	}

	return true
}


func Test_Worker(t *testing.T) {
	srv	:= TestConn()
	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	end	:= make(chan bool)
	w	:= NewWorker(end, nil)

	w.AddServers( srv )
	w.AddHandler("reverse", JobHandler(func(payload io.Reader,reply io.Writer) (error){
		buff	:= make([]byte,1<<16)
		s,_	:= payload.Read(buff)
		buff	= buff[0:s]

		for i:=len(buff); i>0; i-- {
			reply.Write([]byte{ buff[i-1] })
		}

		return nil
	} ))

	if !valid_step(t, srv.Received(), can_do("reverse").Marshal()) {
		return
	}

	if !valid_step(t, srv.Received(), grab_job.Marshal()) {
		return
	}

	srv.Send(no_job.Marshal())
	if !valid_step(t, srv.Received(), pre_sleep.Marshal()) {
		return
	}

	srv.Send(noop.Marshal())
	if !valid_step(t, srv.Received(), grab_job.Marshal()) {
		return
	}

	srv.Send(res_packet(JOB_ASSIGN, []byte("H:lap:1"), []byte("reverse"), []byte("test") ).Marshal())

	if valid_step(t, srv.Received(), grab_job.Marshal()) {
		srv.Send(no_job.Marshal())
	}
	if valid_step(t, srv.Received(), pre_sleep.Marshal()) {
	}

	if !valid_step(t, srv.Received(), res_packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("tset") ).Marshal()) {
		return
	}


	close(end)
}
