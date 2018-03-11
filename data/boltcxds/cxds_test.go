package cxds

import (
	"os"
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests"
)

const testFileName = "test.db.go.ignore"

func testShouldNotPanic(t *testing.T) {
	if pc := recover(); pc != nil {
		t.Error("unexpected panic:", pc)
	}
}

func testDS(t *testing.T) (ds data.CXDS) {
	var err error
	if ds, err = NewCXDS(testFileName, 0644, nil); err != nil {
		t.Fatal(err)
	}
	return
}

func TestNewCXDS(t *testing.T) {
	ds := testDS(t)
	defer ds.Close()
}

func runTestCase(t *testing.T, testCase func(t *testing.T, ds data.CXDS)) {
	ds := testDS(t)
	defer os.Remove(testFileName)
	defer ds.Close()
	testCase(t, ds)
}

func TestCXDS_Get(t *testing.T) {
	runTestCase(t, tests.CXDSGet)
}

func TestCXDS_Set(t *testing.T) {
	runTestCase(t, tests.CXDSSet)
}

func TestCXDS_Inc(t *testing.T) {
	runTestCase(t, tests.CXDSInc)
}

func TestCXDS_Iterate(t *testing.T) {
	runTestCase(t, tests.CXDSIterate)
}

func TestCXDS_IterateDel(t *testing.T) {
	runTestCase(t, tests.CXDSIterateDel)
}

func TestCXDS_Close(t *testing.T) {
	runTestCase(t, tests.CXDSClose)
}
