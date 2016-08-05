package	gearman // import "github.com/nathanaelle/gearman"

import	(
	"bytes"
	"errors"
	"encoding/base64"
)


type	(
	MarshalerGearman interface {
		MarshalGearman() ([]byte,error)
		Len()	int
	}

	UnmarshalerGearman interface {
		UnmarshalGearman([]byte) error
		Cast(Opaque) error
	}

	Opaque		interface {
		MarshalerGearman
		UnmarshalerGearman
		Bytes()	[]byte
	}

	TaskID		[64]byte

	Function	[]byte
	ClientId	[]byte


	opaque		[]byte

	opaque0size	struct {}
)

var empty_opaque	*opaque0size = &opaque0size{}
var CastOpaqueAsOpaqueError	error	= errors.New("Can't cast Opaque as Opaque")


func Opacify(b []byte) Opaque {
	if len(b) == 0 {
		return empty_opaque
	}

	o := opaque(b)

	return &o
}

func (o *opaque)UnmarshalGearman(d []byte) error {
	*o = d
	return	nil
}

func (o *opaque)MarshalGearman() ([]byte,error) {
	return o.Bytes(), nil
}

func (o *opaque)Bytes()	[]byte {
	return	[]byte(*o)
}

func (o *opaque)Len() int {
	return	len(o.Bytes())
}

func (fn *opaque)Cast(o Opaque) error {
	return	CastOpaqueAsOpaqueError
}




func (_ *opaque0size)UnmarshalGearman(d []byte) error {
	if len(d) > 0 {
		return errors.New("empty_opaque can't unmarshal data")
	}
	return	nil
}

func (_ *opaque0size)MarshalGearman() ([]byte,error) {
	return []byte{}, nil
}

func (_ *opaque0size)Bytes() []byte {
	return []byte{}
}

func (_ *opaque0size)Len() int {
	return	0
}


func (fn *opaque0size)Cast(o Opaque) error {
	return	CastOpaqueAsOpaqueError
}




func (tid TaskID)MarshalGearman() ([]byte,error) {
	return	tid[0:tid.Len()],nil
}

func (tid *TaskID)Cast(o Opaque) error {
	return	tid.UnmarshalGearman(o.Bytes())
}


func (tid TaskID)Len() int {
	end := 63
	for end > -1 && tid[end] == 0 {
		end--
	}

	return end+1
}


func (tid *TaskID)UnmarshalGearman(d []byte) error {
	if len(d) > 64 {
		return	errors.New("tid too long")
	}

	for _,v := range d {
		if v == 0 {
			return errors.New("invalid TaskID")
		}
	}

	copy(tid[0:len(d)], d[:])
	return	nil
}

func (fn *Function)UnmarshalGearman(d []byte) error {
	*fn = Function(d)
	return	nil
}

func (fn *Function)Cast(o Opaque) error {
	return	fn.UnmarshalGearman(o.Bytes())
}

func (fn Function)MarshalGearman() ([]byte,error) {
	return fn, nil
}

func (fn Function)Len() int {
	return	len(fn)
}

func (fn Function)String() string {
	return	base64.RawURLEncoding.EncodeToString([]byte(fn))
}

func (f1 Function)IsEqual(f2 Function) bool {
	return	bytes.Equal(f1, f2)
}

func (clid *ClientId)UnmarshalGearman(d []byte) error {
	*clid = d
	return	nil
}

func (clid *ClientId)Cast(o Opaque) error {
	return	clid.UnmarshalGearman(o.Bytes())
}


func (clid ClientId)MarshalGearman() ([]byte,error) {
	return clid, nil
}

func (clid ClientId)Len() int {
	return	len([]byte(clid))
}
