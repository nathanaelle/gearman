package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/nathanaelle/gearman/v2/protocol"
)

func Test_Opaque(t *testing.T) {
	var err error

	err = opaqueTest([]byte{})
	if err != nil {
		t.Errorf("got error %+v", err)
		return
	}

	err = opaqueTest([]byte("hello"))
	if err != nil {
		t.Errorf("got error %+v", err)
		return
	}

}

func opaqueTest(data []byte) error {
	var fn Function
	var tid TaskID

	opaq := protocol.Opacify(data)
	err := fn.Cast(opaq)
	if err != nil {
		return err
	}

	raw, err := fn.MarshalGearman()
	if err != nil {
		return err
	}
	if !bytes.Equal(data, raw) {
		return fmt.Errorf("%s MarshalGearman() expected [%v] got [%v]", "Function", data, raw)
	}

	err = tid.Cast(opaq)
	if err != nil {
		return err
	}

	raw, err = tid.MarshalGearman()
	if err != nil {
		return err
	}
	if !bytes.Equal(data, raw) {
		return fmt.Errorf("%s MarshalGearman() expected [%v] got [%v]", "TaskID", data, raw)
	}

	return nil
}
