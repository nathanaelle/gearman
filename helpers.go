package gearman // import "github.com/nathanaelle/gearman/v2"

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"log"
	"net"
)

func debug(dbg *log.Logger, msg string, args ...interface{}) {
	if dbg == nil {
		return
	}
	dbg.Printf(msg, args...)
}

func randID() (string, error) {
	var raw [24]byte

	_, err := rand.Read(raw[:])
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func isEOF(err error) bool {
	return err == io.EOF
}

func isTimeout(err error) bool {
	switch tErr := err.(type) {
	case net.Error:
		return tErr.Timeout()
	}
	return false
}
