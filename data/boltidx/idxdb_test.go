package idxdb

import (
	"os"
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests"
)

const testFileName string = "test.db.goignore"

func testNewIdxDB(t *testing.T) (idx data.IdxDB) {
	var err error
	if idx, err = NewIdxDB(testFileName); err != nil {
		t.Fatal(err)
	}
	return
}

func runTestCase(t *testing.T, testCase func(t *testing.T, idx data.IdxDB)) {
	idx := testNewIdxDB(t)
	defer os.Remove(testFileName)
	defer idx.Close()

	testCase(t, idx)
}

func TestIdxDB_Tx(t *testing.T) {
	// TODO (kostyarin): test it
}

func TestIdxDB_Close(t *testing.T) {
	runTestCase(t, tests.IdxDBClose)
}
