package sqliteidx

import (
	"testing"

	"github.com/skycoin/cxo/data/tests"
)

func TestRoots_Ascend(t *testing.T) {
	runTestCase(t, tests.RootsAscend)
}

func TestRoots_Descend(t *testing.T) {
	runTestCase(t, tests.RootsDescend)
}

func TestRoots_Set(t *testing.T) {
	runTestCase(t, tests.RootsSet)
}

func TestRoots_Del(t *testing.T) {
	runTestCase(t, tests.RootsDel)
}

func TestRoots_Get(t *testing.T) {
	runTestCase(t, tests.RootsGet)
}

func TestRoots_Has(t *testing.T) {
	runTestCase(t, tests.RootsHas)
}
