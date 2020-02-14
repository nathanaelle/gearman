package protocol // import "github.com/nathanaelle/gearman/v2/protocol"

import "testing"

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
