package memory

import (
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests/idx"
)

func runTestCase(t *testing.T, testCase func(t *testing.T, idx data.IdxDB)) {
	var m = NewMemory(ScanBy)
	defer m.Close()
	testCase(t, m)
}

func TestMemory_AddFeed(t *testing.T)      { runTestCase(t, idx.AddFeed) }
func TestMemory_DelFeed(t *testing.T)      { runTestCase(t, idx.DelFeed) }
func TestMemory_IterateFeeds(t *testing.T) { runTestCase(t, idx.IterateFeeds) }
func TestMemory_HasFeed(t *testing.T)      { runTestCase(t, idx.HasFeed) }
func TestMemory_FeedsLen(t *testing.T)     { runTestCase(t, idx.FeedsLen) }
func TestMemory_AddHead(t *testing.T)      { runTestCase(t, idx.AddHead) }
func TestMemory_DelHead(t *testing.T)      { runTestCase(t, idx.DelHead) }
func TestMemory_HasHead(t *testing.T)      { runTestCase(t, idx.HasHead) }
func TestMemory_IterateHeads(t *testing.T) { runTestCase(t, idx.IterateHeads) }
func TestMemory_HeadsLen(t *testing.T)     { runTestCase(t, idx.HeadsLen) }
func TestMemory_AscendRoots(t *testing.T)  { runTestCase(t, idx.AscendRoots) }
func TestMemory_DescendRoots(t *testing.T) { runTestCase(t, idx.DescendRoots) }
func TestMemory_HasRoot(t *testing.T)      { runTestCase(t, idx.HasRoot) }
func TestMemory_RootsLen(t *testing.T)     { runTestCase(t, idx.RootsLen) }
func TestMemory_SetRoot(t *testing.T)      { runTestCase(t, idx.SetRoot) }

func TestMemory_SetNotTouchRoot(t *testing.T) {
	runTestCase(t, idx.SetNotTouchRoot)
}

func TestMemory_GetRoot(t *testing.T) { runTestCase(t, idx.GetRoot) }

func TestMemory_GetNotTouchRoot(t *testing.T) {
	runTestCase(t, idx.GetNotTouchRoot)
}

func TestMemory_TakeRoot(t *testing.T) { runTestCase(t, idx.TakeRoot) }
func TestMemory_DelRoot(t *testing.T)  { runTestCase(t, idx.DelRoot) }

func TestMemory_IsSafeClosed(t *testing.T) {
	var m = NewMemory(ScanBy)
	defer m.Close()
	idx.IsSafeClosed(t, m, func() (data.IdxDB, error) {
		var err error
		m = NewMemory(ScanBy)
		return m, err
	})
}

func TestMemory_Close(t *testing.T) { runTestCase(t, idx.Close) }
