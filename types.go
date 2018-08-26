package gearman // import "github.com/nathanaelle/gearman"

import (
	"bytes"
	"encoding/base64"
	"errors"

	"github.com/nathanaelle/gearman/v2/protocol"
)

type (

	// TaskID is used as the ID of a gearman Task
	TaskID    []byte
	TaskMapID [64]byte

	Function []byte

	// ClientID is used as the ID of a client in a gearman Task
	ClientID []byte
)

// Cast is UnmarshalerGearman.Cast
func (tid *TaskID) Cast(o protocol.Opaque) error {
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
func (fn *Function) Cast(o protocol.Opaque) error {
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
func (clid *ClientID) Cast(o protocol.Opaque) error {
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
