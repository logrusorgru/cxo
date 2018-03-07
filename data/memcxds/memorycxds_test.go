package memcxds

import (
	"os"
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests"
)

func testShouldNotPanic(t *testing.T) {
	if pc := recover(); pc != nil {
		t.Error("unexpected panic:", pc)
	}
}

func TestNewCXDS(t *testing.T) {
	// NewCXDS() (ds data.CXDS, err error)

	ds := NewCXDS()
	defer ds.Close()
}

func TestCXDS_Get(t *testing.T) {
	// Get(key cipher.SHA256) (val []byte, rc uint32, err error)

	t.Run("memory", func(t *testing.T) {
		tests.CXDSGet(t, NewCXDS())
	})
}

func TestCXDS_Set(t *testing.T) {
	// Set(key cipher.SHA256, val []byte) (rc uint32, err error)

	t.Run("memory", func(t *testing.T) {
		tests.CXDSSet(t, NewCXDS())
	})
}

func TestCXDS_Inc(t *testing.T) {
	// Inc(key cipher.SHA256) (rc uint32, err error)

	t.Run("memory", func(t *testing.T) {
		tests.CXDSInc(t, NewCXDS())
	})
}

func TestCXDS_Close(t *testing.T) {
	// Close() (err error)

	t.Run("memory", func(t *testing.T) {
		tests.CXDSClose(t, NewCXDS())
	})
}
