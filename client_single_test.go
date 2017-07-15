package gearman // import "github.com/nathanaelle/gearman"

import (
	"context"
	"testing"
)

func TestSingleClient_simple(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := ConnTest()
	defer	srv.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := SingleServerClient(end, nil) //logger)

	cli.AddServers(srv)

	r := cli.Submit(NewTask("reverse", []byte("test")))

	if !valid_step(t, srv.Received(), BuildPacket(SUBMIT_JOB, Opacify([]byte("reverse")), Opacify([]byte("")), Opacify([]byte("test")))) {
		return
	}
	srv.Send(BuildPacket(JOB_CREATED, Opacify([]byte("H:lap:1"))))
	srv.Send(BuildPacket(WORK_COMPLETE, Opacify([]byte("H:lap:1")), Opacify([]byte("tset"))))

	if !valid_result(t, []byte("tset"), nil)(r.Value()) {
		return
	}
}

func TestSingleClient_unordered_result(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := ConnTest()
	defer	srv.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := SingleServerClient(end, nil) //logger)

	cli.AddServers(srv)

	r1 := cli.Submit(NewTask("reverse", []byte("test 1")))
	r2 := cli.Submit(NewTask("reverse", []byte("test 2")))
	r3 := cli.Submit(NewTask("reverse", []byte("test 3")))

	if !valid_step(t, srv.Received(), BuildPacket(SUBMIT_JOB, Opacify([]byte("reverse")), Opacify([]byte("")), Opacify([]byte("test 1")))) {
		return
	}
	if !valid_step(t, srv.Received(), BuildPacket(SUBMIT_JOB, Opacify([]byte("reverse")), Opacify([]byte("")), Opacify([]byte("test 2")))) {
		return
	}
	if !valid_step(t, srv.Received(), BuildPacket(SUBMIT_JOB, Opacify([]byte("reverse")), Opacify([]byte("")), Opacify([]byte("test 3")))) {
		return
	}

	srv.Send(BuildPacket(JOB_CREATED, Opacify([]byte("H:lap:1"))))
	srv.Send(BuildPacket(JOB_CREATED, Opacify([]byte("H:lap:2"))))
	srv.Send(BuildPacket(JOB_CREATED, Opacify([]byte("H:lap:3"))))

	srv.Send(BuildPacket(WORK_COMPLETE, Opacify([]byte("H:lap:2")), Opacify([]byte("2 tset"))))
	srv.Send(BuildPacket(WORK_COMPLETE, Opacify([]byte("H:lap:3")), Opacify([]byte("3 tset"))))
	srv.Send(BuildPacket(WORK_COMPLETE, Opacify([]byte("H:lap:1")), Opacify([]byte("1 tset"))))

	if !valid_result(t, []byte("2 tset"), nil)(r2.Value()) {
		return
	}

	if !valid_result(t, []byte("3 tset"), nil)(r3.Value()) {
		return
	}

	if !valid_result(t, []byte("1 tset"), nil)(r1.Value()) {
		return
	}
}
