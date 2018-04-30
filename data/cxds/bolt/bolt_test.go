package bolt

import (
	"os"
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests/cxds"
)

var dbFileName = "test.bolt.go.ignore"

func newBolt(t *testing.T) (b *Bolt) {
	os.Remove(dbFileName)
	var err error
	if b, err = NewBolt(dbFileName, 0644, nil, 2); err != nil {
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

func runTestCase(t *testing.T, testCase func(t *testing.T, ds data.CXDS)) {
	var b = newBolt(t)
	defer closeBolt(t, b)

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

func TestBolt_Hooks(t *testing.T) { runTestCase(t, cxds.Hooks) }

func TestBolt_Touch(t *testing.T) { runTestCase(t, cxds.Touch) }

func TestBolt_Get(t *testing.T)         { runTestCase(t, cxds.Get) }
func TestBolt_GetIncr(t *testing.T)     { runTestCase(t, cxds.GetIncr) }
func TestBolt_GetNotTouch(t *testing.T) { runTestCase(t, cxds.GetNotTouch) }
func TestBolt_GetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.GetIncrNotTouch)
}

func TestBolt_Set(t *testing.T)         { runTestCase(t, cxds.Set) }
func TestBolt_SetIncr(t *testing.T)     { runTestCase(t, cxds.SetIncr) }
func TestBolt_SetNotTouch(t *testing.T) { runTestCase(t, cxds.SetNotTouch) }
func TestBolt_SetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.SetIncrNotTouch)
}
func TestBolt_SetRaw(t *testing.T) { runTestCase(t, cxds.SetRaw) }

func TestBolt_Incr(t *testing.T)         { runTestCase(t, cxds.Incr) }
func TestBolt_IncrNotTouch(t *testing.T) { runTestCase(t, cxds.IncrNotTouch) }

func TestBolt_Take(t *testing.T) { runTestCase(t, cxds.Take) }
func TestBolt_Del(t *testing.T)  { runTestCase(t, cxds.Del) }

func TestBolt_Iterate(t *testing.T) { runTestCase(t, cxds.Iterate) }

func TestBolt_Amount(t *testing.T) { runTestCase(t, cxds.Amount) }
func TestBolt_Volume(t *testing.T) { runTestCase(t, cxds.Volume) }

func TestBolt_IsSafeClosed(t *testing.T) {

	var b = newBolt(t)
	defer closeBolt(t, b)

	cxds.IsSafeClosed(t, b, func() (data.CXDS, error) {
		return NewBolt(dbFileName, 0644, nil, 2)
	})
}

func TestBolt_Close(t *testing.T) { runTestCase(t, cxds.Close) }
