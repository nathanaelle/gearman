package gearman // import "github.com/nathanaelle/gearman"

import (
	"fmt"
	"sync"
	"time"
	"context"
	"testing"
	"math/rand"
)

func TestRRClient_simple(t *testing.T) {
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv1 := ConnTest()
	srv2 := ConnTest()

	defer	srv1.Close()
	defer	srv2.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := RoundRobinClient(end, nil) //logger)
	defer cli.Close()

	cli.AddServers(srv1,srv2)

	r1 := cli.Submit(NewTask("reverse", []byte("test 1")))
	r2 := cli.Submit(NewTask("reverse", []byte("test 2")))

	if !valid_step(t, srv1.Received(), BuildPacket(SUBMIT_JOB, Opacify([]byte("reverse")), Opacify([]byte("")), Opacify([]byte("test 1")))) {
		return
	}
	srv1.Send(BuildPacket(JOB_CREATED, Opacify([]byte("H:lap:1"))))
	srv1.Send(BuildPacket(WORK_COMPLETE, Opacify([]byte("H:lap:1")), Opacify([]byte("1 tset"))))

	if !valid_step(t, srv2.Received(), BuildPacket(SUBMIT_JOB, Opacify([]byte("reverse")), Opacify([]byte("")), Opacify([]byte("test 2")))) {
		return
	}
	srv2.Send(BuildPacket(JOB_CREATED, Opacify([]byte("H:lap:2"))))
	srv2.Send(BuildPacket(WORK_COMPLETE, Opacify([]byte("H:lap:2")), Opacify([]byte("2 tset"))))

	if !valid_result(t, []byte("1 tset"), nil)(r1.Value()) {
		return
	}

	if !valid_result(t, []byte("2 tset"), nil)(r2.Value()) {
		return
	}

}

func TestRRClient_unordered_result(t *testing.T) {

	wg	:= new(sync.WaitGroup)
	end, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv0 := ConnTest()
	srv1 := ConnTest()
	srv2 := ConnTest()
	defer srv0.Close()
	defer srv1.Close()
	defer srv2.Close()

	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := RoundRobinClient(end, nil) //logger)

	cli.AddServers(srv0, srv1, srv2)

	wg.Add(3)
	go rrclient_srv(srv0, "1", wg, t)
	go rrclient_srv(srv1, "2", wg, t)
	go rrclient_srv(srv2, "3", wg, t)

	for _,idx := range([]int{0,1,2,3,4,5,6,7,8,9}) {
		for _,pre := range([]int{1,2,3}) {
			r := cli.Submit(NewTask("reverse", []byte(fmt.Sprintf("test %02d", idx+pre*10) )))
			go func(pre, idx int, r Task, t *testing.T) {
				if !valid_result(t, []byte(fmt.Sprintf("%02d tset", idx*10+pre)), nil)(r.Value()) {
					t.Error(fmt.Sprintf("wrong value for %02d", idx+pre*10))
					return
				}
			}(pre, idx, r, t)
		}
	}

	wg.Wait()

}


func rrclient_srv(srv *testConn, prefix string, wg *sync.WaitGroup, t *testing.T) {
	defer	wg.Done()

	for idx := byte('0'); idx <= '9'; idx++ {
		step := string([]byte{idx})
		res := srv.Received()
		if !valid_step(t, res, BuildPacket(SUBMIT_JOB, Opacify([]byte("reverse")), Opacify([]byte("")), Opacify([]byte("test "+prefix+step)))) {
			return
		}
		taskid := []byte("GPREF:" + prefix + ":" + step)
		srv.Send(BuildPacket(JOB_CREATED, Opacify(taskid)))

		time.Sleep(time.Millisecond*time.Duration(10*rand.Intn(10)))
		srv.Send(BuildPacket(WORK_COMPLETE, Opacify(taskid), Opacify( []byte(step+prefix+" tset") )))
	}
}
