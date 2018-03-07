package idxdb

import (
	"errors"
	"os"
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests"
)

const testFileName string = "test.db.goignore"

var errTestError = errors.New("test error")

func testNewDriveIdxDB(t *testing.T) (idx data.IdxDB) {
	var err error
	if idx, err = NewDriveIdxDB(testFileName); err != nil {
		t.Fatal(err)
	}
	return
}

func TestIdxDB_Tx(t *testing.T) {
	// Tx(func(Tx) error) error

	// TODO (kostyarin):
}

func TestIdxDB_Close(t *testing.T) {
	// Close() error

	// TODO (kostyarin): memory

	t.Run("drive", func(t *testing.T) {
		idx := testNewDriveIdxDB(t)
		defer os.Remove(testFileName)
		defer idx.Close()

		tests.IdxDBClose(t, idx)
	})

}
