package bolt

import (
	"os"
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests/idx"
)

var dbFileName = "test.bolt.go.ignore"

func newBolt(t *testing.T) (b *Bolt) {
	os.Remove(dbFileName)
	var err error
	if b, err = NewBolt(dbFileName, 0644, nil, ScanBy); err != nil {
		t.Fatal(err)
	}
	return
}

// clean up db after all
func closeBolt(t *testing.T, b *Bolt) {
	defer os.Remove(dbFileName)
	if err := b.Close(); err != nil {
		t.Error(err)
	}
}

func runTestCase(t *testing.T, testCase func(t *testing.T, idx data.IdxDB)) {
	var b = newBolt(t)
	defer closeBolt(t, b)

	testCase(t, b)
}

func TestBolt_AddFeed(t *testing.T)      { runTestCase(t, idx.AddFeed) }
func TestBolt_DelFeed(t *testing.T)      { runTestCase(t, idx.DelFeed) }
func TestBolt_IterateFeeds(t *testing.T) { runTestCase(t, idx.IterateFeeds) }
func TestBolt_HasFeed(t *testing.T)      { runTestCase(t, idx.HasFeed) }
func TestBolt_FeedsLen(t *testing.T)     { runTestCase(t, idx.FeedsLen) }
func TestBolt_AddHead(t *testing.T)      { runTestCase(t, idx.AddHead) }
func TestBolt_DelHead(t *testing.T)      { runTestCase(t, idx.DelHead) }
func TestBolt_HasHead(t *testing.T)      { runTestCase(t, idx.HasHead) }
func TestBolt_IterateHeads(t *testing.T) { runTestCase(t, idx.IterateHeads) }
func TestBolt_HeadsLen(t *testing.T)     { runTestCase(t, idx.HeadsLen) }
func TestBolt_AscendRoots(t *testing.T)  { runTestCase(t, idx.AscendRoots) }
func TestBolt_DescendRoots(t *testing.T) { runTestCase(t, idx.DescendRoots) }
func TestBolt_HasRoot(t *testing.T)      { runTestCase(t, idx.HasRoot) }
func TestBolt_RootsLen(t *testing.T)     { runTestCase(t, idx.RootsLen) }
func TestBolt_SetRoot(t *testing.T)      { runTestCase(t, idx.SetRoot) }

func TestBolt_SetNotTouchRoot(t *testing.T) {
	runTestCase(t, idx.SetNotTouchRoot)
}

func TestBolt_GetRoot(t *testing.T) { runTestCase(t, idx.GetRoot) }

func TestBolt_GetNotTouchRoot(t *testing.T) {
	runTestCase(t, idx.GetNotTouchRoot)
}

func TestBolt_TakeRoot(t *testing.T) { runTestCase(t, idx.TakeRoot) }
func TestBolt_DelRoot(t *testing.T)  { runTestCase(t, idx.DelRoot) }

func TestBolt_IsSafeClosed(t *testing.T) {
	var b = newBolt(t)
	defer closeBolt(t, b)
	idx.IsSafeClosed(t, b, func() (data.IdxDB, error) {
		var err error
		b, err = NewBolt(dbFileName, 0644, nil, ScanBy)
		return b, err
	})
}

func TestBolt_Close(t *testing.T) { runTestCase(t, idx.Close) }
