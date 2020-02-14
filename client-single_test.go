package gearman // import "github.com/nathanaelle/gearman/v2"

import (
	"context"
	"testing"

	"github.com/nathanaelle/gearman/v2/protocol"
)

func TestSingleClient_simple(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := connTest()
	defer srv.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := SingleServerClient(end, nil) //logger)

	cli.AddServers(srv)

	r := cli.Submit(NewTask("reverse", []byte("test")))

	clientSrv(srv, "H:lap:000", "test", "tset", t)

	if !validResult(t, []byte("tset"), nil)(r.Value()) {
		return
	}
}

func TestSingleClient_unordered_result(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := connTest()
	defer srv.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := SingleServerClient(end, nil) //logger)

	cli.AddServers(srv)

	r1 := cli.Submit(NewTask("reverse", []byte("test 1")))
	r2 := cli.Submit(NewTask("reverse", []byte("test 2")))
	r3 := cli.Submit(NewTask("reverse", []byte("test 3")))

	if !validStep(t, srv.Received(), protocol.BuildPacket(protocol.SubmitJob, protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("")), protocol.Opacify([]byte("test 1")))) {
		return
	}
	if !validStep(t, srv.Received(), protocol.BuildPacket(protocol.SubmitJob, protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("")), protocol.Opacify([]byte("test 2")))) {
		return
	}
	if !validStep(t, srv.Received(), protocol.BuildPacket(protocol.SubmitJob, protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("")), protocol.Opacify([]byte("test 3")))) {
		return
	}

	srv.Send(protocol.BuildPacket(protocol.JobCreated, protocol.Opacify([]byte("H:lap:1"))))
	srv.Send(protocol.BuildPacket(protocol.JobCreated, protocol.Opacify([]byte("H:lap:2"))))
	srv.Send(protocol.BuildPacket(protocol.JobCreated, protocol.Opacify([]byte("H:lap:3"))))

	srv.Send(protocol.BuildPacket(protocol.WorkComplete, protocol.Opacify([]byte("H:lap:2")), protocol.Opacify([]byte("2 tset"))))
	srv.Send(protocol.BuildPacket(protocol.WorkComplete, protocol.Opacify([]byte("H:lap:3")), protocol.Opacify([]byte("3 tset"))))
	srv.Send(protocol.BuildPacket(protocol.WorkComplete, protocol.Opacify([]byte("H:lap:1")), protocol.Opacify([]byte("1 tset"))))

	if !validResult(t, []byte("3 tset"), nil)(r3.Value()) {
		return
	}

	if !validResult(t, []byte("2 tset"), nil)(r2.Value()) {
		return
	}

	if !validResult(t, []byte("1 tset"), nil)(r1.Value()) {
		return
	}
}
