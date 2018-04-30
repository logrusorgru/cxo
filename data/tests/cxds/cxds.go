// Package cxds consis of test cases for data.CXDS
// <github.com/skycoin/cxo/data#CXDS> and Hooks.
package cxds

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

func keyValueByString(s string) (key cipher.SHA256, val []byte) {
	val = []byte(s)
	key = cipher.SumSHA256(val)
	return
}

func areObjectsEqual(o, e *data.Object) (eq bool) {
	eq = bytes.Compare(o.Val, e.Val) == 0 &&
		o.RC == e.RC &&
		o.Access.Equal(e.Access) &&
		o.Create.Equal(o.Create)
	return
}

func incrBys() []int64 {
	return []int64{-1, 0, +1}
}

type stat struct {
	All, Used int64
}

func statShouldBe(t *testing.T, ds data.CXDS, amount, volume stat) {
	t.Helper()
	var all, used = ds.Amount()
	if amount.All != all {
		t.Errorf("wrong amount of all objects %d, want %d", all, amount.All)
	}
	if amount.Used != used {
		t.Errorf("wrong amount of used objects %d, want %d", used, amount.Used)
	}
	all, used = ds.Volume()
	if volume.All != all {
		t.Errorf("wrong volume of all objects %d, want %d", all, volume.All)
	}
	if volume.Used != used {
		t.Errorf("wrong volume of used objects %d, want %d", used, volume.Used)
	}
}

func mapFromSlice(keys []cipher.SHA256) (m map[cipher.SHA256]bool) {
	m = make(map[cipher.SHA256]bool, len(keys))
	for _, k := range keys {
		m[k] = false
	}
	return
}

func dsShouldHave(t *testing.T, ds data.CXDS, keys ...cipher.SHA256) {
	t.Helper()
	var (
		m     = mapFromSlice(keys)
		count = len(m)

		err error
	)
	err = ds.Iterate(func(key cipher.SHA256) (err error) {
		if visit, ok := m[key]; ok == false {
			t.Error("missing object:", key.Hex()[:7])
		} else if visit == true {
			t.Errorf("got %s twice", key.Hex()[:7])
		}
		m[key] = true
		count--
		return
	})
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if count > 0 {
		t.Errorf("missing %d objects", count)
	} else if count < 0 {
		t.Errorf("have %d unexpected objects", count)
	}
}

func dsShouldBeBlank(t *testing.T, ds data.CXDS) {
	t.Helper()
	statShouldBe(t, ds, stat{}, stat{})
	dsShouldHave(t, ds)
}

// Hooks test case.
func Hooks(t *testing.T, ds data.CXDS) {
	// Hooks() (hooks Hooks)

	// nothing to test here, just be sure that the method doesn not panic
	ds.Hooks()
}

// Touch test case.
func Touch(t *testing.T, ds data.CXDS) {
	// Touch(key cipher.SHA256) (access time.Time, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
	)

	t.Run("not exist", func(t *testing.T) {
		if _, err := ds.Touch(key); err == nil {
			t.Error("missing 'not found' error")
		} else if err != data.ErrNotFound {
			t.Error("unexpected error:", err)
		}
		dsShouldBeBlank(t, ds) // don't create by the Touch
	})

	t.Run("exist", func(t *testing.T) {

		var obj, err = ds.Set(key, val)
		if err != nil {
			t.Error(err)
			return
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)

		var access time.Time
		if access, err = ds.Touch(key); err != nil {
			t.Error(err)
			return
		}

		// last access time is createing time
		if access.Equal(obj.Create) == false {
			t.Errorf("unexpected last access time: %s, want %s",
				obj.Access, access)
			return
		}

		var tobj *data.Object
		if tobj, err = ds.Get(key); err != nil {
			t.Error("unexpected error")
			return
		}

		if obj.Access.Before(tobj.Access) == false {
			t.Error("not touched")
		}

		// compare objects
		obj.Access = tobj.Access
		if areObjectsEqual(obj, tobj) == false {
			t.Error("something changed in objects")
		}

		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})
}

// Get test case.
func Get(t *testing.T, ds data.CXDS) {
	// Get(key cipher.SHA256) (obj *Object, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
	)

	t.Run("not exist", func(t *testing.T) {
		if _, err := ds.Get(key); err == nil {
			t.Error("missing error")
		} else if err != data.ErrNotFound {
			t.Error("unexpected error")
		}
		dsShouldBeBlank(t, ds) // don't create by the Get
	})

	t.Run("exists", func(t *testing.T) {
		var obj, err = ds.Set(key, val)
		if err != nil {
			t.Error(err)
			return
		}
		var gobj *data.Object
		if gobj, err = ds.Get(key); err != nil {
			t.Error(err)
			return
		}
		// compare
		obj.Access = obj.Create // (to compare easy way)
		if areObjectsEqual(obj, gobj) == false {
			t.Error("object has been changed")
		}
		// touch
		if gobj, err = ds.Get(key); err != nil {
			t.Error(err)
			return
		}
		// updated access time
		if gobj.Access.After(obj.Create) == false {
			t.Error("access time not updated")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})
}

// GetIncr test case.
func GetIncr(t *testing.T, ds data.CXDS) {
	// GetIncr(key cipher.SHA256, incrBy int64) (obj *Object, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
	)

	t.Run("not exist", func(t *testing.T) {
		for _, incrBy := range incrBys() {
			if _, err := ds.GetIncr(key, incrBy); err == nil {
				t.Error("missing error")
			} else if err != data.ErrNotFound {
				t.Error("unexpected error")
			}
		}
		dsShouldBeBlank(t, ds) // don't create by the GetIncr
	})

	t.Run("exists", func(t *testing.T) {
		var obj, err = ds.Set(key, val)
		if err != nil {
			t.Error(err)
			return
		}
		var (
			gobj *data.Object
			rc   int64 = 1
		)
		for i, incrBy := range incrBys() {
			rc += incrBy
			t.Logf("cycle: %d, incr by: %d, rc: %d", i, incrBy, rc)
			if gobj, err = ds.GetIncr(key, incrBy); err != nil {
				t.Error(err)
				return
			}
			if gobj.Access.After(obj.Access) == false {
				t.Error("access time not updated")
			}
			obj.Access = gobj.Access // for next loop
			if gobj.RC != rc {
				t.Errorf("wrong RC %d, want %d", gobj.RC, rc)
			}
			if rc > 0 {
				statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
			} else {
				statShouldBe(t, ds, stat{1, 0}, stat{vol, 0})
			}
		}
		dsShouldHave(t, ds, key)
	})
}

// GetNoTouch test case.
func GetNotTouch(t *testing.T, ds data.CXDS) {
	// GetNotTouch(key cipher.SHA256) (obj *Object, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
	)

	t.Run("not exist", func(t *testing.T) {
		if _, err := ds.GetNotTouch(key); err == nil {
			t.Error("missing error")
		} else if err != data.ErrNotFound {
			t.Error("unexpected error")
		}
		dsShouldBeBlank(t, ds)
	})

	t.Run("exists", func(t *testing.T) {
		var obj, err = ds.Set(key, val)
		if err != nil {
			t.Error(err)
			return
		}
		var gobj *data.Object
		if gobj, err = ds.GetNotTouch(key); err != nil {
			t.Error(err)
			return
		}
		// compare
		obj.Access = obj.Create // to compare easy way
		if areObjectsEqual(obj, gobj) == false {
			t.Error("object has been changed")
		}
		if gobj, err = ds.GetNotTouch(key); err != nil {
			t.Error(err)
			return
		}
		if gobj.Access.Equal(obj.Access) == false {
			t.Error("access time updated")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})
}

// GetIncrNotTouch test case.
func GetIncrNotTouch(t *testing.T, ds data.CXDS) {
	// GetIncrNotTouch(key cipher.SHA256, incrBy int64) (obj *Object, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
	)

	t.Run("not exist", func(t *testing.T) {
		for _, incrBy := range incrBys() {
			if _, err := ds.GetIncrNotTouch(key, incrBy); err == nil {
				t.Error("missing error")
			} else if err != data.ErrNotFound {
				t.Error("unexpected error")
			}
		}
		dsShouldBeBlank(t, ds)
	})

	t.Run("exists", func(t *testing.T) {
		var obj, err = ds.Set(key, val)
		if err != nil {
			t.Error(err)
			return
		}
		var (
			gobj *data.Object
			rc   int64 = 1
		)
		for i, incrBy := range incrBys() {
			rc += incrBy
			t.Logf("cycle: %d, incr by: %d, rc: %d", i, incrBy, rc)
			if gobj, err = ds.GetIncrNotTouch(key, incrBy); err != nil {
				t.Error(err)
				return
			}
			if gobj.Access.Equal(obj.Create) == false {
				t.Error("access time updated")
			}
			if gobj.RC != rc {
				t.Errorf("wrong RC %d, want %d", gobj.RC, rc)
			}
			if rc > 0 {
				statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
			} else {
				statShouldBe(t, ds, stat{1, 0}, stat{vol, 0})
			}
		}
		dsShouldHave(t, ds, key)
	})
}

// Set test case.
func Set(t *testing.T, ds data.CXDS) {
	// Set(key cipher.SHA256, val []byte) (obj *Object, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))

		create time.Time // access time (access on create)
	)

	t.Run("create", func(t *testing.T) {
		var (
			tp       = time.Now() // time point before creating
			obj, err = ds.Set(key, val)
		)
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("object contains invalid value")
		}
		if obj.RC != 1 {
			t.Errorf("invalid RC %d, want 1", obj.RC)
		}
		if obj.Create.After(tp) == false {
			t.Error("invalid create time")
		}
		if obj.Access.UnixNano() != 0 {
			t.Error("invalid access time (shold be the begining of unix epoch)")
		}
		create = obj.Create // keep for next test
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})

	if t.Failed() == true {
		t.Skip("can't continue, because of previous test")
		return
	}

	t.Run("overwrite", func(t *testing.T) {
		var obj, err = ds.Set(key, val) // overwrite
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("object contains invalid value")
		}
		if obj.RC != 2 {
			t.Errorf("invalid RC %d, want 2", obj.RC)
		}
		if obj.Create.Equal(create) == false {
			t.Error("invalid create time")
		}
		// last access time (i.e. previous)
		if obj.Access.Equal(create) == false {
			t.Error("invalid access time")
		}
		// access time
		var last time.Time
		if last, err = ds.Touch(key); err != nil {
			t.Error(err)
			return
		}
		if last.After(obj.Access) == false {
			t.Error("access time not updated")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})

}

// SetIncr test case.
func SetIncr(t *testing.T, ds data.CXDS) {
	// SetIncr(key cipher.SHA256, val []byte, incrBy int64) (obj *Object, err error)

	var (
		key, val = keyValueByString("something") //
		vol      = int64(len(val))               //
		tp       = time.Now()                    // time point

		create time.Time // create time
		last   time.Time // last access time
		rc     int64     // expeted rc
	)

	for i, incrBy := range incrBys() { // -1, 0, +1

		rc += incrBy

		var obj, err = ds.SetIncr(key, val, incrBy)

		if err != nil {
			t.Error(err)
			return
		}

		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("object contains invalid value")
		}

		if obj.RC != rc {
			t.Errorf("invalid RC %d, want %d", obj.RC, rc)
		}

		if i == 0 {
			if obj.Create.After(tp) == false {
				t.Error("invalid creating time")
			}
			create = obj.Create
			if obj.Access.UnixNano() != 0 {
				t.Error("invalid access time")
			}
			last = obj.Access
		} else {
			if obj.Create.Equal(create) == false {
				t.Error("invalid create time")
			}
			if i == 1 {
				if obj.Access.Equal(obj.Create) == false {
					t.Error("invalid access time")
				}
				last = obj.Access
			} else {
				if obj.Access.After(last) == false {
					t.Error("invalid access time")
				}
			}
		}

		if rc > 0 {
			statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		} else {
			statShouldBe(t, ds, stat{1, 0}, stat{vol, 0})
		}
		dsShouldHave(t, ds, key)

	}

}

// SetNotTouch test case.
func SetNotTouch(t *testing.T, ds data.CXDS) {
	// SetNotTouch(key cipher.SHA256, val []byte) (obj *Object, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))

		create time.Time // create and access time
	)

	t.Run("create", func(t *testing.T) {
		var (
			tp       = time.Now()
			obj, err = ds.SetNotTouch(key, val)
		)
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("object contains invalid value")
		}
		if obj.RC != 1 {
			t.Errorf("invalid RC %d, want 1", obj.RC)
		}
		if obj.Create.After(tp) == false {
			t.Error("invalid create time")
		}
		if obj.Access.UnixNano() != 0 {
			t.Error("invalid access time")
		}
		create = obj.Create // keep for next test
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})

	if t.Failed() == true {
		t.Skip("can't continue, because of previous test")
		return
	}

	t.Run("overwrite", func(t *testing.T) {
		var obj, err = ds.SetNotTouch(key, val) // overwrite
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("object contains invalid value")
		}
		if obj.RC != 2 {
			t.Errorf("invalid RC %d, want 2", obj.RC)
		}
		if obj.Create.Equal(create) == false {
			t.Error("invalid create time")
		}
		// last access time (i.e. previous)
		if obj.Access.Equal(create) == false {
			t.Error("invalid access time")
		}
		//
		// access time
		//
		var last time.Time
		if last, err = ds.Touch(key); err != nil {
			t.Error(err)
			return
		}
		if last.Equal(create) == false {
			t.Error("access time was updated")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})
}

// SetIncrNotTouch test case.
func SetIncrNotTouch(t *testing.T, ds data.CXDS) {
	// SetIncrNotTouch(key cipher.SHA256, val []byte, incrBy int64) (obj *Object, err error)

	var (
		key, val = keyValueByString("something") //
		vol      = int64(len(val))               //
		tp       = time.Now()                    // time point

		create time.Time // create time
		rc     int64     // expeted rc
	)

	for i, incrBy := range incrBys() { // -1, 0, +1
		rc += incrBy
		var obj, err = ds.SetIncrNotTouch(key, val, incrBy)
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("object contains invalid value")
		}
		if obj.RC != rc {
			t.Errorf("invalid RC %d, want %d", obj.RC, rc)
		}
		if i == 0 {
			if obj.Create.After(tp) == false {
				t.Error("invalid create time")
			}
			create = obj.Create
			if obj.Access.UnixNano() != 0 {
				t.Error("invalid access time")
			}
		} else {
			if obj.Create.Equal(create) == false {
				t.Error("invalid create time")
			}
			if obj.Access.Equal(create) == false {
				t.Error("invalid access time")
			}
		}
		if rc > 0 {
			statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		} else {
			statShouldBe(t, ds, stat{1, 0}, stat{vol, 0})
		}
		dsShouldHave(t, ds, key)
	}
}

// SetRaw test case.
func SetRaw(t *testing.T, ds data.CXDS) {
	// SetRaw(key cipher.SHA256, obj *Object) (err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
		obj      = new(data.Object)
	)

	obj.Val = val
	obj.RC = 101
	// the begining of unix epoch
	obj.Access = time.Unix(0, 0)
	obj.Create = time.Unix(0, 0)

	t.Run("create", func(t *testing.T) {
		var err error
		if err = ds.SetRaw(key, obj); err != nil {
			t.Error(err)
			return
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
		if obj, err = ds.Get(key); err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("wrong value")
		}
		if obj.RC != 101 {
			t.Errorf("wrong RC %d, want %d", obj.RC, 101)
		}
		if obj.Access.UnixNano() != 0 {
			t.Error("access time has been changed", obj.Access)
		}
		if obj.Create.UnixNano() != 0 {
			t.Error("create time has been changed", obj.Create)
		}
	})

	if t.Failed() == true {
		t.Skip("can't continue, because of previous test case")
		return
	}

	t.Run("overwrite", func(t *testing.T) {
		var (
			now = time.Now()
			err error
		)
		obj.Create = now
		obj.Access = now
		obj.RC = -101
		if err = ds.SetRaw(key, obj); err != nil {
			t.Error(err)
			return
		}
		statShouldBe(t, ds, stat{1, 0}, stat{vol, 0})
		dsShouldHave(t, ds, key)
		if obj, err = ds.Get(key); err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("wrong value")
		}
		if obj.RC != -101 {
			t.Errorf("wrong RC %d, want %d", obj.RC, -101)
		}
		if obj.Access.Equal(now) == false {
			t.Error("access time has been changed", obj.Access, now)
		}
		if obj.Create.Equal(now) == false {
			t.Error("create time has been changed", obj.Create, now)
		}
	})

	t.Run("reborn", func(t *testing.T) {
		var (
			now = time.Now()
			err error
		)
		obj.Create = now
		obj.Access = now
		obj.RC = 1
		if err = ds.SetRaw(key, obj); err != nil {
			t.Error(err)
			return
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
		if obj, err = ds.Get(key); err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("wrong value")
		}
		if obj.RC != 1 {
			t.Errorf("wrong RC %d, want %d", obj.RC, 1)
		}
		if obj.Access.Equal(now) == false {
			t.Error("access time has been changed", obj.Access, now)
		}
		if obj.Create.Equal(now) == false {
			t.Error("create time has been changed", obj.Create, now)
		}
	})

	t.Run("still alive", func(t *testing.T) {
		var (
			now = time.Now()
			err error
		)
		obj.Create = now
		obj.Access = now
		obj.RC = 101
		if err = ds.SetRaw(key, obj); err != nil {
			t.Error(err)
			return
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
		if obj, err = ds.Get(key); err != nil {
			t.Error(err)
			return
		}
		if bytes.Compare(val, obj.Val) != 0 {
			t.Error("wrong value")
		}
		if obj.RC != 101 {
			t.Errorf("wrong RC %d, want %d", obj.RC, 101)
		}
		if obj.Access.Equal(now) == false {
			t.Error("access time has been changed", obj.Access, now)
		}
		if obj.Create.Equal(now) == false {
			t.Error("create time has been changed", obj.Create, now)
		}
	})

}

// Incr test case.
func Incr(t *testing.T, ds data.CXDS) {
	// Incr(key cipher.SHA256, incrBy int64) (rc int64, access time.Time, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
	)

	t.Run("not exist", func(t *testing.T) {
		if _, _, err := ds.Incr(key, 10); err == nil {
			t.Error("missing error")
		} else if err != data.ErrNotFound {
			t.Error("unexpected error:", err)
		}
		dsShouldBeBlank(t, ds)
	})

	var at = time.Now() // time before the Set
	if _, err := ds.Set(key, val); err != nil {
		t.Error(err)
		return
	}

	t.Run("increase", func(t *testing.T) {
		var (
			tp              = time.Now()
			rc, access, err = ds.Incr(key, 1)
		)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != 2 {
			t.Error("wrong rc", rc)
		}
		if access.After(at) == false {
			t.Error("wrong last access")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
		at = tp
	})

	if t.Failed() {
		t.Skip("previous test required")
		return
	}

	t.Run("zero", func(t *testing.T) {
		var (
			tp              = time.Now()
			rc, access, err = ds.Incr(key, 0)
		)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != 2 {
			t.Error("wrong rc", rc)
		}
		if access.After(at) == false {
			t.Error("wrong last access")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
		at = tp
	})

	if t.Failed() {
		t.Skip("previous test required")
		return
	}

	t.Run("reduce", func(t *testing.T) {
		var rc, access, err = ds.Incr(key, -100)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != 2-100 {
			t.Error("wrong rc", rc)
		}
		if access.After(at) == false {
			t.Error("wrong last access")
		}
		statShouldBe(t, ds, stat{1, 0}, stat{vol, 0})
		dsShouldHave(t, ds, key)
	})

}

// IncrNotTouch test case.
func IncrNotTouch(t *testing.T, ds data.CXDS) {
	// IncrNotTouch(key cipher.SHA256, incrBy int64) (rc int64, access time.Time, err error)

	var (
		key, val = keyValueByString("something")
		vol      = int64(len(val))
	)

	t.Run("not exist", func(t *testing.T) {
		if _, _, err := ds.IncrNotTouch(key, 10); err == nil {
			t.Error("missing error")
		} else if err != data.ErrNotFound {
			t.Error("unexpected error:", err)
		}
		dsShouldBeBlank(t, ds)
	})

	var obj, err = ds.Set(key, val)
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("increase", func(t *testing.T) {
		var rc, access, err = ds.IncrNotTouch(key, 1)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != 2 {
			t.Error("wrong rc", rc)
		}
		if access.Equal(obj.Create) == false {
			t.Error("wrong last access (touched)")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})

	if t.Failed() {
		t.Skip("previous test required")
		return
	}

	t.Run("zero", func(t *testing.T) {
		var rc, access, err = ds.IncrNotTouch(key, 0)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != 2 {
			t.Error("wrong rc", rc)
		}
		if access.Equal(obj.Create) == false {
			t.Error("wrong last access (touched)")
		}
		statShouldBe(t, ds, stat{1, 1}, stat{vol, vol})
		dsShouldHave(t, ds, key)
	})

	if t.Failed() {
		t.Skip("previous test required")
		return
	}

	t.Run("reduce", func(t *testing.T) {
		var rc, access, err = ds.IncrNotTouch(key, -100)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != 2-100 {
			t.Error("wrong rc", rc)
		}
		if access.Equal(obj.Create) == false {
			t.Error("wrong last access (touched)")
		}
		statShouldBe(t, ds, stat{1, 0}, stat{vol, 0})
		dsShouldHave(t, ds, key)
	})

}

// Take test case.
func Take(t *testing.T, ds data.CXDS) {
	// Take(key cipher.SHA256) (obj *Object, err error)

	var key, val = keyValueByString("something")

	t.Run("not exist", func(t *testing.T) {
		if _, err := ds.Take(key); err == nil {
			t.Error("missing error")
		} else if err != data.ErrNotFound {
			t.Error("unexpected error:", err)
		}
	})

	var obj, err = ds.Set(key, val)
	if err != nil {
		t.Error(err)
		return
	}
	obj.Access = obj.Create

	t.Run("take alive", func(t *testing.T) {
		var tobj, err = ds.Take(key)
		if err != nil {
			t.Error(err)
			return
		}
		if areObjectsEqual(tobj, obj) == false {
			t.Error("objects are not equal")
		}
		dsShouldBeBlank(t, ds)
	})

	if obj, err = ds.SetIncr(key, val, -100); err != nil {
		t.Error(err)
		return
	}
	obj.Access = obj.Create

	t.Run("take dead", func(t *testing.T) {
		var tobj, err = ds.Take(key)
		if err != nil {
			t.Error(err)
			return
		}
		if areObjectsEqual(tobj, obj) == false {
			t.Error("objects are not equal")
		}
		dsShouldBeBlank(t, ds)
	})

}

// Del test case.
func Del(t *testing.T, ds data.CXDS) {
	// Del(key cipher.SHA256) (err error)

	var key, val = keyValueByString("something")

	t.Run("not exist", func(t *testing.T) {
		if err := ds.Del(key); err == nil {
			t.Error("missing error")
		} else if err != data.ErrNotFound {
			t.Error("unexpected error:", err)
		}
	})

	if _, err := ds.Set(key, val); err != nil {
		t.Error(err)
		return
	}

	t.Run("del alive", func(t *testing.T) {
		if err := ds.Del(key); err != nil {
			t.Error(err)
		}
		dsShouldBeBlank(t, ds)
	})

	if _, err := ds.SetIncr(key, val, -100); err != nil {
		t.Error(err)
		return
	}

	t.Run("del dead", func(t *testing.T) {
		if err := ds.Del(key); err != nil {
			t.Error(err)
			return
		}
		dsShouldBeBlank(t, ds)
	})
}

// Iterate test case.
func Iterate(t *testing.T, ds data.CXDS) {
	// Iterate(iterateFunc IterateKeysFunc) (err error)

	// blank
	dsShouldBeBlank(t, ds)

	// one object
	var (
		someKey, someVal = keyValueByString("something")
		someVol          = int64(len(someVal))

		err error
	)
	if _, err = ds.Set(someKey, someVal); err != nil {
		t.Error(err)
		return
	}

	statShouldBe(t, ds, stat{1, 1}, stat{someVol, someVol})
	dsShouldHave(t, ds, someKey)

	// second object
	var (
		otherKey, otherVal = keyValueByString("someother")
		otherVol           = int64(len(otherVal))
	)
	if _, err = ds.Set(otherKey, otherVal); err != nil {
		t.Error(err)
		return
	}

	statShouldBe(t, ds, stat{2, 2},
		stat{someVol + otherVol, someVol + otherVol})
	dsShouldHave(t, ds, someKey, otherKey)

	// stop iteration
	var called int
	err = ds.Iterate(func(cipher.SHA256) error {
		called++
		return data.ErrStopIteration
	})
	if err != nil {
		t.Error(err)
		return
	}

	if called != 1 {
		t.Error("wrong times called", called)
	}

	// pass error through
	var breakingError = errors.New("breaking error")
	called = 0
	err = ds.Iterate(func(cipher.SHA256) error {
		called++
		return breakingError
	})
	if err == nil {
		t.Error("missing error")
	} else if err != breakingError {
		t.Error("unexpected error:", err)
	} else if called != 1 {
		t.Error("wrong times called", called)
	}

}

// Amount test case.
func Amount(t *testing.T, ds data.CXDS) {
	// Amount() (all, used int64)

	// other tests
}

// Volume test case.
func Volume(t *testing.T, ds data.CXDS) {
	// Volume() (all, used int64)

	// other tests
}

// IsSafeClosed test case. The reopen fucntion can be nil.
func IsSafeClosed(
	t *testing.T, //                     : the T pointer
	ds data.CXDS, //                     : ds already opened
	reopen func() (data.CXDS, error), // : reopen ds to check the flag
) {
	// IsSafeClosed() bool

	if ds.IsSafeClosed() == false {
		t.Error("fresh db is not safe closed")
	}

	if reopen == nil {
		return
	}

	var err error
	if err = ds.Close(); err != nil {
		t.Error(err)
	}

	if ds, err = reopen(); err != nil {
		t.Error(err)
	}

	if ds.IsSafeClosed() == false {
		t.Error("not safe closed, after reopenning")
	}

}

// Close test case.
func Close(t *testing.T, ds data.CXDS) {
	// Close() (err error)

	for i := 0; i < 2; i++ {
		if err := ds.Close(); err != nil {
			t.Error(i, err)
		}
	}

}
