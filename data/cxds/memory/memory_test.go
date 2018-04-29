package memory

import (
	"testing"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests/cxds"
)

func runTestCase(t *testing.T, testCase func(t *testing.T, ds data.CXDS)) {
	testCase(t, NewMemory())
}

func TestMemory_Hooks(t *testing.T) { runTestCase(t, cxds.Hooks) }

func TestMemory_Touch(t *testing.T) { runTestCase(t, cxds.Touch) }

func TestMemory_Get(t *testing.T)         { runTestCase(t, cxds.Get) }
func TestMemory_GetIncr(t *testing.T)     { runTestCase(t, cxds.GetIncr) }
func TestMemory_GetNotTouch(t *testing.T) { runTestCase(t, cxds.GetNotTouch) }
func TestMemory_GetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.GetIncrNotTouch)
}

func TestMemory_Set(t *testing.T)         { runTestCase(t, cxds.Set) }
func TestMemory_SetIncr(t *testing.T)     { runTestCase(t, cxds.SetIncr) }
func TestMemory_SetNotTouch(t *testing.T) { runTestCase(t, cxds.SetNotTouch) }
func TestMemory_SetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.SetIncrNotTouch)
}
func TestMemory_SetRaw(t *testing.T) { runTestCase(t, cxds.SetRaw) }

func TestMemory_Incr(t *testing.T)         { runTestCase(t, cxds.Incr) }
func TestMemory_IncrNotTouch(t *testing.T) { runTestCase(t, cxds.IncrNotTouch) }

func TestMemory_Take(t *testing.T) { runTestCase(t, cxds.Take) }
func TestMemory_Del(t *testing.T)  { runTestCase(t, cxds.Del) }

func TestMemory_Iterate(t *testing.T) { runTestCase(t, cxds.Iterate) }

func TestMemory_Amount(t *testing.T) { runTestCase(t, cxds.Amount) }
func TestMemory_Volume(t *testing.T) { runTestCase(t, cxds.Volume) }

func TestMemory_IsSafeClosed(t *testing.T) {
	cxds.IsSafeClosed(t, NewMemory(), nil)
}

func TestMemory_Close(t *testing.T) { runTestCase(t, cxds.Close) }
