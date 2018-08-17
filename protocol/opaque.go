package protocol

import "errors"

type (
	// MarshalerGearman describe methods needed to marshal a interface to a []byte
	MarshalerGearman interface {
		MarshalGearman() ([]byte, error)
		Len() int
	}

	// UnmarshalerGearman describe methods needed to unmarshal a []byte to an interface
	UnmarshalerGearman interface {
		UnmarshalGearman([]byte) error
		Cast(Opaque) error
	}

	// Opaque describe an opaque data attribute to a gearman Function
	Opaque interface {
		MarshalerGearman
		UnmarshalerGearman
		Bytes() []byte
	}

	opaque []byte

	opaque0size struct{}
)

var emptyOpaque Opaque = &opaque0size{}

// Opacify create an Opaque data from a []byte
func Opacify(b []byte) Opaque {
	if len(b) == 0 {
		return emptyOpaque
	}

	o := opaque(b)

	return &o
}

func (o *opaque) UnmarshalGearman(d []byte) error {
	*o = d
	return nil
}

func (o *opaque) Cast(opq Opaque) error {
	return ErrCastOpaqueAsOpaque
}

func (o opaque) MarshalGearman() ([]byte, error) {
	return o.Bytes(), nil
}

func (o opaque) Bytes() []byte {
	return []byte(o)
}

func (o opaque) Len() int {
	return len(o.Bytes())
}

func (o *opaque0size) UnmarshalGearman(d []byte) error {
	if len(d) > 0 {
		return errors.New("empty_opaque can't unmarshal data")
	}
	return nil
}

func (o *opaque0size) Cast(opq Opaque) error {
	return ErrCastOpaqueAsOpaque
}

func (o opaque0size) MarshalGearman() ([]byte, error) {
	return []byte{}, nil
}

func (o opaque0size) Bytes() []byte {
	return []byte{}
}

func (o opaque0size) Len() int {
	return 0
}
