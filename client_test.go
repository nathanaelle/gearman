package gearman

import (
	"bytes"
	"net"
	"testing"
)

func valid_step(t *testing.T, rcvd []byte, expected Packet) bool {
	if bytes.Equal(rcvd, expected.Marshal()) {
		return true
	}

	t.Errorf("received %+v expected %+v", rcvd, expected.Marshal())
	return false
}

func valid_byte(t *testing.T, rcvd, expected []byte) bool {
	if bytes.Equal(rcvd, expected) {
		return true
	}

	t.Errorf("received %+v expected %+v", rcvd, expected)
	return false
}

func valid_err(t *testing.T, err, expected_err error) bool {
	switch {
	case err != nil && expected_err != nil:
		if err.Error() != expected_err.Error() {
			t.Errorf("got error [%v] expected [%v]", err, expected_err)
			return false
		}

	default:
		if err != expected_err {
			t.Errorf("got error [%v] expected [%v]", err, expected_err)
			return false
		}
	}

	return true
}

func valid_any_step(t *testing.T, rcvd []byte, expecteds ...Packet) bool {
	for _, expected := range expecteds {
		if bytes.Equal(rcvd, expected.Marshal()) {
			return true
		}
	}

	t.Errorf("received %+v expected %+v", rcvd, expecteds)
	return false
}

func valid_result(t *testing.T, expected_res []byte, expected_err error) func([]byte, error) bool {
	return func(res []byte, err error) bool {
		return valid_err(t, err, expected_err) && valid_byte(t, res, expected_res)
	}
}

func packet_received_is(t *testing.T, c net.Conn, expected_pkt Packet) bool {
	pkt, err := ReadPacket(c)
	if err != nil {
		t.Errorf("got error %+v", err)
		return false
	}

	return valid_step(t, pkt.Marshal(), expected_pkt)
}


func packet_received_is_any(t *testing.T, c net.Conn, expected_pkts ...Packet) bool {
	pkt, err := ReadPacket(c)
	if err != nil {
		t.Errorf("got error %+v", err)
		return false
	}

	return valid_any_step(t, pkt.Marshal(), expected_pkts...)
}



func Test_Client_simple(t *testing.T) {
	end := make(chan struct{})
	defer close(end)

	srv := ConnTest()
	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := SingleServerClient(end, nil) //logger)

	cli.AddServers(srv)

	r := cli.Submit(NewTask("reverse", []byte("test")))

	if !valid_step(t, srv.Received(), packet(SUBMIT_JOB, []byte("reverse"), []byte(""), []byte("test"))) {
		return
	}
	srv.Send(packet(JOB_CREATED, []byte("H:lap:1")))
	srv.Send(packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("tset")))

	if !valid_result(t, []byte("tset"), nil)(r.Value()) {
		return
	}
}

func Test_Client_unordered_result(t *testing.T) {
	end := make(chan struct{})
	defer close(end)

	srv := ConnTest()
	//logger	:= log.New(os.Stderr, "logger: ", log.Lshortfile|log.Ltime)
	cli := SingleServerClient(end, nil) //logger)

	cli.AddServers(srv)

	r1 := cli.Submit(NewTask("reverse", []byte("test 1")))
	r2 := cli.Submit(NewTask("reverse", []byte("test 2")))
	r3 := cli.Submit(NewTask("reverse", []byte("test 3")))

	if !valid_step(t, srv.Received(), packet(SUBMIT_JOB, []byte("reverse"), []byte(""), []byte("test 1"))) {
		return
	}
	if !valid_step(t, srv.Received(), packet(SUBMIT_JOB, []byte("reverse"), []byte(""), []byte("test 2"))) {
		return
	}
	if !valid_step(t, srv.Received(), packet(SUBMIT_JOB, []byte("reverse"), []byte(""), []byte("test 3"))) {
		return
	}

	srv.Send(packet(JOB_CREATED, []byte("H:lap:1")))
	srv.Send(packet(JOB_CREATED, []byte("H:lap:2")))
	srv.Send(packet(JOB_CREATED, []byte("H:lap:3")))

	srv.Send(packet(WORK_COMPLETE, []byte("H:lap:2"), []byte("2 tset")))
	srv.Send(packet(WORK_COMPLETE, []byte("H:lap:3"), []byte("3 tset")))
	srv.Send(packet(WORK_COMPLETE, []byte("H:lap:1"), []byte("1 tset")))

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
