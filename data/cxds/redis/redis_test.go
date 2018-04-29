package redis

import (
	"flag"
	"testing"

	"github.com/mediocregopher/radix.v3"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/data/tests/cxds"
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

func runTestCase(t *testing.T, testCase func(t *testing.T, ds data.CXDS)) {
	var r = newRedis(t)
	defer closeRedis(t, r)

	testCase(t, r)
}

func TestRedis_Hooks(t *testing.T) { runTestCase(t, cxds.Hooks) }

func TestRedis_Touch(t *testing.T) { runTestCase(t, cxds.Touch) }

func TestRedis_Get(t *testing.T)         { runTestCase(t, cxds.Get) }
func TestRedis_GetIncr(t *testing.T)     { runTestCase(t, cxds.GetIncr) }
func TestRedis_GetNotTouch(t *testing.T) { runTestCase(t, cxds.GetNotTouch) }
func TestRedis_GetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.GetIncrNotTouch)
}

func TestRedis_Set(t *testing.T)         { runTestCase(t, cxds.Set) }
func TestRedis_SetIncr(t *testing.T)     { runTestCase(t, cxds.SetIncr) }
func TestRedis_SetNotTouch(t *testing.T) { runTestCase(t, cxds.SetNotTouch) }
func TestRedis_SetIncrNotTouch(t *testing.T) {
	runTestCase(t, cxds.SetIncrNotTouch)
}
func TestRedis_SetRaw(t *testing.T) { runTestCase(t, cxds.SetRaw) }

func TestRedis_Incr(t *testing.T)         { runTestCase(t, cxds.Incr) }
func TestRedis_IncrNotTouch(t *testing.T) { runTestCase(t, cxds.IncrNotTouch) }

func TestRedis_Take(t *testing.T) { runTestCase(t, cxds.Take) }
func TestRedis_Del(t *testing.T)  { runTestCase(t, cxds.Del) }

func TestRedis_Iterate(t *testing.T) { runTestCase(t, cxds.Iterate) }

func TestRedis_Amount(t *testing.T) { runTestCase(t, cxds.Amount) }
func TestRedis_Volume(t *testing.T) { runTestCase(t, cxds.Volume) }

func TestRedis_IsSafeClosed(t *testing.T) {

	var r = newRedis(t)
	defer closeRedis(t, r)

	cxds.IsSafeClosed(t, r, func() (data.CXDS, error) {
		return NewRedis("tcp", Address, nil)
	})
}

func TestRedis_Close(t *testing.T) { runTestCase(t, cxds.Close) }
