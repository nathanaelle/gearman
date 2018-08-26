package gearman // import "github.com/nathanaelle/gearman"

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nathanaelle/gearman/v2/protocol"
)

func TestRRClient_simple(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv1 := connTest()
	srv2 := connTest()

	defer srv1.Close()
	defer srv2.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := RoundRobinClient(end, nil) //logger)
	defer cli.Close()

	cli.AddServers(srv1, srv2)

	r1 := cli.Submit(NewTask("reverse", []byte("test 1")))
	r2 := cli.Submit(NewTask("reverse", []byte("test 2")))
	r3 := cli.Submit(NewTask("reverse", []byte("test 3")))
	r4 := cli.Submit(NewTask("reverse", []byte("test 4")))

	clientSrv(srv1, "H:lap:1", "test 1", "1 tset", t)
	clientSrv(srv2, "H:lap:2", "test 2", "2 tset", t)
	clientSrv(srv1, "H:lap:3", "test 3", "3 tset", t)
	clientSrv(srv2, "H:lap:4", "test 4", "4 tset", t)

	if !validResult(t, []byte("1 tset"), nil)(r1.Value()) {
		return
	}

	if !validResult(t, []byte("2 tset"), nil)(r2.Value()) {
		return
	}

	if !validResult(t, []byte("3 tset"), nil)(r3.Value()) {
		return
	}

	if !validResult(t, []byte("4 tset"), nil)(r4.Value()) {
		return
	}
}

func TestRRClient_but_single(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv1 := connTest()

	defer srv1.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := RoundRobinClient(end, nil) //logger)
	defer cli.Close()

	cli.AddServers(srv1)

	r1 := cli.Submit(NewTask("reverse", []byte("test 1")))
	r2 := cli.Submit(NewTask("reverse", []byte("test 2")))
	r3 := cli.Submit(NewTask("reverse", []byte("test 3")))

	clientSrv(srv1, "H:lap:1", "test 1", "1 tset", t)
	clientSrv(srv1, "H:lap:2", "test 2", "2 tset", t)
	clientSrv(srv1, "H:lap:3", "test 3", "3 tset", t)

	if !validResult(t, []byte("1 tset"), nil)(r1.Value()) {
		return
	}

	if !validResult(t, []byte("2 tset"), nil)(r2.Value()) {
		return
	}

	if !validResult(t, []byte("3 tset"), nil)(r3.Value()) {
		return
	}

}

func TestRRClient_unordered_result(t *testing.T) {

	wg := new(sync.WaitGroup)
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv0 := connTest()
	srv1 := connTest()
	srv2 := connTest()
	defer srv0.Close()
	defer srv1.Close()
	defer srv2.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := RoundRobinClient(end, nil) //logger)

	cli.AddServers(srv0, srv1, srv2)

	wg.Add(3)
	go rrclientSrv(srv0, "1", wg, t)
	go rrclientSrv(srv1, "2", wg, t)
	go rrclientSrv(srv2, "3", wg, t)

	for _, idx := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
		for _, pre := range []int{1, 2, 3} {
			r := cli.Submit(NewTask("reverse", []byte(fmt.Sprintf("test %02d", idx+pre*10))))
			go func(pre, idx int, r Task, t *testing.T) {
				if !validResult(t, []byte(fmt.Sprintf("%02d tset", idx*10+pre)), nil)(r.Value()) {
					t.Error(fmt.Sprintf("wrong value for %02d", idx+pre*10))
					return
				}
			}(pre, idx, r, t)
		}
	}

	wg.Wait()

}

func clientSrv(srv *testConn, taskid, expected, answer string, t *testing.T) {
	if !validStep(t, srv.Received(), protocol.BuildPacket(protocol.SubmitJob, protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("")), protocol.Opacify([]byte(expected)))) {
		return
	}
	srv.Send(protocol.BuildPacket(protocol.JobCreated, protocol.Opacify([]byte(taskid))))
	srv.Send(protocol.BuildPacket(protocol.WorkComplete, protocol.Opacify([]byte(taskid)), protocol.Opacify([]byte(answer))))
}

func rrclientSrv(srv *testConn, prefix string, wg *sync.WaitGroup, t *testing.T) {
	defer wg.Done()

	for idx := byte('0'); idx <= '9'; idx++ {
		step := string([]byte{idx})
		res := srv.Received()
		if !validStep(t, res, protocol.BuildPacket(protocol.SubmitJob, protocol.Opacify([]byte("reverse")), protocol.Opacify([]byte("")), protocol.Opacify([]byte("test "+prefix+step)))) {
			return
		}
		taskid := []byte("GPREF:" + prefix + ":" + step)
		srv.Send(protocol.BuildPacket(protocol.JobCreated, protocol.Opacify(taskid)))

		time.Sleep(time.Millisecond * time.Duration(10*rand.Intn(10)))
		srv.Send(protocol.BuildPacket(protocol.WorkComplete, protocol.Opacify(taskid), protocol.Opacify([]byte(step+prefix+" tset"))))
	}
}
