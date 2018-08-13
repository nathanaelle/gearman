package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"testing"
)

func validStep(t *testing.T, rcvd []byte, expected Packet) bool {
	if bytes.Equal(rcvd, expected.Marshal()) {
		return true
	}

	t.Errorf("received %+v expected %+v", rcvd, expected.Marshal())
	return false
}

func validByte(t *testing.T, rcvd, expected []byte) bool {
	if bytes.Equal(rcvd, expected) {
		return true
	}

	t.Errorf("received %+v expected %+v", rcvd, expected)
	return false
}

func validErr(t *testing.T, err, expectedErr error) bool {
	switch {
	case err != nil && expectedErr != nil:
		if err.Error() != expectedErr.Error() {
			t.Errorf("got error [%v] expected [%v]", err, expectedErr)
			return false
		}

	default:
		if err != expectedErr {
			t.Errorf("got error [%v] expected [%v]", err, expectedErr)
			return false
		}
	}

	return true
}

func validAnyStep(t *testing.T, rcvd []byte, expecteds ...Packet) bool {
	for _, expected := range expecteds {
		if bytes.Equal(rcvd, expected.Marshal()) {
			return true
		}
	}

	t.Errorf("received %+v expected %+v", rcvd, expecteds)
	return false
}

func validResult(t *testing.T, expectedRes []byte, expectedErr error) func([]byte, error) bool {
	return func(res []byte, err error) bool {
		return validErr(t, err, expectedErr) && validByte(t, res, expectedRes)
	}
}

func packetReceivedIs(t *testing.T, pf PacketFactory, expectedPkt Packet) bool {
	pkt, err := pf.Packet()
	if err != nil {
		t.Errorf("got error %+v", err)
		return false
	}

	return validStep(t, pkt.Marshal(), expectedPkt)
}

func packetReceivedIsAny(t *testing.T, pf PacketFactory, expectedPkts ...Packet) bool {
	pkt, err := pf.Packet()
	if err != nil {
		t.Errorf("got error %+v", err)
		return false
	}

	return validAnyStep(t, pkt.Marshal(), expectedPkts...)
}
