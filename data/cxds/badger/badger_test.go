package bolt

import (
	"os"
	"testing"

	"github.com/dgraph-io/badger"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests/cxds"
)

var dbDirName = "test.bolt.go.ignore"

func newBadger(t *testing.T) (b *Badger) {
	os.RemoveAll(dbDirName)

	var opts = badger.DefaultOptions
	opts.Dir = dbDirName
	opts.ValueDir = dbDirName

	var err error
	if b, err = NewBadger(opts, ScanBy); err != nil {
		t.Fatal(err)
	}
	return
}

// clean up db after all
func closeBadger(t *testing.T, b *Badger) {
	defer os.RemoveAll(dbDirName)
	if err := b.Close(); err != nil {
		t.Error(err)
	}
}

func runTestCase(t *testing.T, testCase func(t *testing.T, ds data.CXDS)) {
	var b = newBadger(t)
	defer closeBadger(t, b)

	testCase(t, b)
}

func Test_encodeDecode(t *testing.T) {
	var inf1 metaInfo
	inf1.amount.all = 10
	inf1.amount.used = 11
	inf1.volume.all = 100
	inf1.volume.used = 101
	inf1.isSafeClosed = true

	var p = inf1.encode()

	var inf2 metaInfo
	if err := inf2.decode(p); err != nil {
		t.Fatal(err)
	}

	var assert = func(b bool) {
		t.Helper()
		if b == true {
			return
		}
		t.Fatal("wrong")
	}

	assert(inf1.amount.all == inf2.amount.all)
	assert(inf1.amount.used == inf2.amount.used)
	assert(inf1.volume.all == inf2.volume.all)
	assert(inf1.volume.used == inf2.volume.used)
	assert(inf1.isSafeClosed == inf2.isSafeClosed)

}

func TestBadger_Hooks(t *testing.T) { runTestCase(t, cxds.Hooks) }

func TestBadger_Touch(t *testing.T) { runTestCase(t, cxds.Touch) }

func TestBadger_Get(t *testing.T)         { runTestCase(t, cxds.Get) }
func TestBadger_GetIncr(t *testing.T)     { runTestCase(t, cxds.GetIncr) }
func TestBadger_GetNotTouch(t *testing.T) { runTestCase(t, cxds.GetNotTouch) }
func TestBadger_GetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.GetIncrNotTouch)
}

func TestBadger_Set(t *testing.T)         { runTestCase(t, cxds.Set) }
func TestBadger_SetIncr(t *testing.T)     { runTestCase(t, cxds.SetIncr) }
func TestBadger_SetNotTouch(t *testing.T) { runTestCase(t, cxds.SetNotTouch) }
func TestBadger_SetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.SetIncrNotTouch)
}
func TestBadger_SetRaw(t *testing.T) { runTestCase(t, cxds.SetRaw) }

func TestBadger_Incr(t *testing.T)         { runTestCase(t, cxds.Incr) }
func TestBadger_IncrNotTouch(t *testing.T) { runTestCase(t, cxds.IncrNotTouch) }

func TestBadger_Take(t *testing.T) { runTestCase(t, cxds.Take) }
func TestBadger_Del(t *testing.T)  { runTestCase(t, cxds.Del) }

func TestBadger_Iterate(t *testing.T) { runTestCase(t, cxds.Iterate) }

func TestBadger_Amount(t *testing.T) {
	var b = newBadger(t)
	defer closeBadger(t, b)
	cxds.Amount(t, b, func() (data.CXDS, error) {
		var opts = badger.DefaultOptions
		opts.Dir = dbDirName
		opts.ValueDir = dbDirName
		var err error
		b, err = NewBadger(opts, ScanBy)
		return b, err
	})
}

func TestBadger_Volume(t *testing.T) {
	var b = newBadger(t)
	defer closeBadger(t, b)
	cxds.Volume(t, b, func() (data.CXDS, error) {
		var opts = badger.DefaultOptions
		opts.Dir = dbDirName
		opts.ValueDir = dbDirName
		var err error
		b, err = NewBadger(opts, ScanBy)
		return b, err
	})
}

func TestBadger_IsSafeClosed(t *testing.T) {
	var b = newBadger(t)
	defer closeBadger(t, b)
	cxds.IsSafeClosed(t, b, func() (data.CXDS, error) {
		var opts = badger.DefaultOptions
		opts.Dir = dbDirName
		opts.ValueDir = dbDirName
		var err error
		b, err = NewBadger(opts, ScanBy)
		return b, err
	})
}

func TestBadger_Close(t *testing.T) { runTestCase(t, cxds.Close) }
