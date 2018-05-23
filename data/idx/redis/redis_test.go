package redis

import (
	"flag"
	"testing"

	"github.com/mediocregopher/radix.v3"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests/idx"
)

var Address = "127.0.0.1:6379"

func init() {
	flag.StringVar(&Address,
		"a",
		Address,
		"redis server address for tests")
	flag.Parse()
}

func newRedis(t *testing.T) (r *Redis) {
	var (
		conn radix.Conn
		err  error
	)
	if conn, err = radix.Dial("tcp", Address); err != nil {
		t.Fatal(err)
	}
	defer conn.Close() // once or twice

	// initialize
	if err = conn.Do(radix.Cmd(nil, "FLUSHDB")); err != nil {
		t.Fatal(err)
	}
	if err = conn.Do(radix.Cmd(nil, "SCRIPT", "FLUSH")); err != nil {
		t.Fatal(err)
	}

	conn.Close() // terminate connection

	if r, err = NewRedis("tcp", Address, nil); err != nil {
		t.Fatal(err)
	}
	return
}

// clean up db after all
func closeRedis(t *testing.T, r *Redis) {
	var err error
	if err = r.Close(); err != nil {
		t.Error(err)
	}

	// finialize db
	var conn radix.Conn
	if conn, err = radix.Dial("tcp", Address); err != nil {
		t.Fatal(err)
	}
	defer conn.Close() // once or twice

	// initialize
	if err = conn.Do(radix.Cmd(nil, "FLUSHDB")); err != nil {
		t.Error(err)
	}
	if err = conn.Do(radix.Cmd(nil, "SCRIPT", "FLUSH")); err != nil {
		t.Error(err)
	}

}

func runTestCase(t *testing.T, testCase func(t *testing.T, ds data.IdxDB)) {
	var r = newRedis(t)
	defer closeRedis(t, r)

	testCase(t, r)
}

func TestRedis_AddFeed(t *testing.T)      { runTestCase(t, idx.AddFeed) }
func TestRedis_DelFeed(t *testing.T)      { runTestCase(t, idx.DelFeed) }
func TestRedis_IterateFeeds(t *testing.T) { runTestCase(t, idx.IterateFeeds) }
func TestRedis_HasFeed(t *testing.T)      { runTestCase(t, idx.HasFeed) }
func TestRedis_FeedsLen(t *testing.T)     { runTestCase(t, idx.FeedsLen) }
func TestRedis_AddHead(t *testing.T)      { runTestCase(t, idx.AddHead) }
func TestRedis_DelHead(t *testing.T)      { runTestCase(t, idx.DelHead) }
func TestRedis_HasHead(t *testing.T)      { runTestCase(t, idx.HasHead) }
func TestRedis_IterateHeads(t *testing.T) { runTestCase(t, idx.IterateHeads) }
func TestRedis_HeadsLen(t *testing.T)     { runTestCase(t, idx.HeadsLen) }
func TestRedis_AscendRoots(t *testing.T)  { runTestCase(t, idx.AscendRoots) }
func TestRedis_DescendRoots(t *testing.T) { runTestCase(t, idx.DescendRoots) }
func TestRedis_HasRoot(t *testing.T)      { runTestCase(t, idx.HasRoot) }
func TestRedis_RootsLen(t *testing.T)     { runTestCase(t, idx.RootsLen) }
func TestRedis_SetRoot(t *testing.T)      { runTestCase(t, idx.SetRoot) }

func TestRedis_SetNotTouchRoot(t *testing.T) {
	runTestCase(t, idx.SetNotTouchRoot)
}

func TestRedis_GetRoot(t *testing.T) { runTestCase(t, idx.GetRoot) }

func TestRedis_GetNotTouchRoot(t *testing.T) {
	runTestCase(t, idx.GetNotTouchRoot)
}

func TestRedis_TakeRoot(t *testing.T) { runTestCase(t, idx.TakeRoot) }
func TestRedis_DelRoot(t *testing.T)  { runTestCase(t, idx.DelRoot) }

func TestRedis_IsSafeClosed(t *testing.T) {

	var r = newRedis(t)
	defer closeRedis(t, r)

	idx.IsSafeClosed(t, r, func() (data.IdxDB, error) {
		var err error
		r, err = NewRedis("tcp", Address, nil)
		return r, err
	})
}

func TestRedis_Close(t *testing.T) { runTestCase(t, idx.Close) }
