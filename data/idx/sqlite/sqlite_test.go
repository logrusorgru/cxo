package sqliteidx

import (
	"os"
	"testing"
)

func TestNewIdxDB(t *testing.T) {

	// broken file
	defer os.Remove(testFileName)

	fl, err := os.Create(testFileName)
	if err != nil {
		t.Error(err)
		return
	}
	defer fl.Close()

	if _, err := fl.Write([]byte("Abra-Cadabra")); err != nil {
		t.Error(err)
		return
	}

	if idx, err := NewIdxDB(testFileName); err == nil {
		t.Error("missing error")
		idx.Close()
	}

}
