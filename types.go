package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"encoding/base64"
	"errors"
)

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

	// TaskID is used as the ID of a gearman Task
	TaskID    []byte
	TaskMapID [64]byte

	Function []byte

	// ClientID is used as the ID of a client in a gearman Task
	ClientID []byte

	opaque []byte

	opaque0size struct{}
)

var emptyOpaque Opaque = &opaque0size{}

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

// Cast is UnmarshalerGearman.Cast
func (tid *TaskID) Cast(o Opaque) error {
	return tid.UnmarshalGearman(o.Bytes())
}

// UnmarshalGearman is UnmarshalerGearman.UnmarshalGearman
func (tid *TaskID) UnmarshalGearman(d []byte) error {
	if len(d) > 64 {
		return errors.New("too Long TaskID")
	}
	*tid = TaskID(d)
	return nil
}

// MarshalGearman is MarshalerGearman.MarshalGearman
func (tid TaskID) MarshalGearman() ([]byte, error) {
	if len([]byte(tid)) > 64 {
		return nil, errors.New("too Long TaskID")
	}
	return []byte(tid), nil
}

// Len is MarshalerGearman.Len
func (tid TaskID) Len() int {
	return len([]byte(tid))
}

func (tid TaskID) String() string {
	return base64.RawURLEncoding.EncodeToString([]byte(tid))
}

// UnmarshalGearman is UnmarshalerGearman.UnmarshalGearman
func (fn *Function) UnmarshalGearman(d []byte) error {
	*fn = Function(d)
	return nil
}

// Cast is UnmarshalerGearman.Cast
func (fn *Function) Cast(o Opaque) error {
	return fn.UnmarshalGearman(o.Bytes())
}

// MarshalGearman is MarshalerGearman.MarshalGearman
func (fn Function) MarshalGearman() ([]byte, error) {
	return []byte(fn), nil
}

// Len is MarshalerGearman.Len
func (fn Function) Len() int {
	return len([]byte(fn))
}

func (fn Function) String() string {
	return base64.RawURLEncoding.EncodeToString([]byte(fn))
}

func (fn Function) IsEqual(f2 Function) bool {
	return bytes.Equal(fn, f2)
}

// UnmarshalGearman is UnmarshalerGearman.UnmarshalGearman
func (clid *ClientID) UnmarshalGearman(d []byte) error {
	*clid = d
	return nil
}

// Cast is UnmarshalerGearman.Cast
func (clid *ClientID) Cast(o Opaque) error {
	return clid.UnmarshalGearman(o.Bytes())
}

// MarshalGearman is MarshalerGearman.MarshalGearman
func (clid ClientID) MarshalGearman() ([]byte, error) {
	return []byte(clid), nil
}

// Len is MarshalerGearman.Len
func (clid ClientID) Len() int {
	return len([]byte(clid))
}
