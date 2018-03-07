package tests

import (
	"bytes"
	"errors"
	"testing"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

func testKeyValue(s string) (key cipher.SHA256, val []byte) {
	val = []byte(s)
	key = cipher.SumSHA256(val)
	return
}

func testIncs() []int {
	return []int{-1, 0, 1}
}

func shouldNotExistInCXDS(t *testing.T, ds data.CXDS, key cipher.SHA256) {
	t.Helper()

	if _, rc, err := ds.Get(key, 0); err == nil {
		t.Error("missing error")
	} else if err != data.ErrNotFound {
		t.Error("unexpected error:", err)
	} else if rc != 0 {
		t.Error("wrong rc:", rc)
	}
}

func shouldExistInCXDS(
	t *testing.T,
	ds data.CXDS,
	key cipher.SHA256,
	rc uint32,
	val []byte,
) {

	t.Helper()

	if gval, grc, err := ds.Get(key, 0); err != nil {
		t.Error(err)
	} else if grc != rc {
		t.Errorf("wrong rc %d, want %d", grc, rc)
	} else if want, got := string(val), string(gval); want != got {
		t.Errorf("wrong value: want %q, got %q", want, got)
	}
}

func shouldPanic(t *testing.T) {
	t.Helper()

	if recover() == nil {
		t.Error("missing panic")
	}
}

func addValues(
	t *testing.T, //         :
	ds data.CXDS, //         :
	vals ...string, //       :
) (
	keys []cipher.SHA256, // :
	vlaues [][]byte, //        :
) {
	t.Helper()

	keys = make([]cipher.SHA256, 0, len(vals))
	vlaues = make([][]byte, 0, len(vals))

	for _, val := range vals {

		var k, v = testKeyValue(val)

		if _, err := ds.Set(k, v, 1); err != nil {
			t.Fatal(err)
		}

		keys = append(keys, k)
		vlaues = append(vlaues, v)

	}

	return
}

// CXDSGet tests Get method of CXDS
func CXDSGet(t *testing.T, ds data.CXDS) {
	t.Helper()

	key, value := testKeyValue("something")

	t.Run("not exist", func(t *testing.T) {

		for _, inc := range testIncs() {
			if val, rc, err := ds.Get(key, inc); err == nil {
				t.Error("missing error")
			} else if err != data.ErrNotFound {
				t.Error("unexpected error:", err)
			} else if rc != 0 {
				t.Error("wrong rc", rc)
			} else if val != nil {
				t.Error("not nil")
			}
		}
	})

	if _, err := ds.Set(key, value, 1); err != nil {
		t.Error(err)
		return
	}

	t.Run("existing", func(t *testing.T) {

		t.Run("inc 0", func(t *testing.T) {
			if val, rc, err := ds.Get(key, 0); err != nil {
				t.Error(err)
			} else if rc != 1 {
				t.Error("wrong rc", rc)
			} else if want, got := string(value), string(val); want != got {
				t.Errorf("wrong value: want %q, got %q", want, got)
			}
		})

		t.Run("inc 1", func(t *testing.T) {
			if val, rc, err := ds.Get(key, 1); err != nil {
				t.Error(err)
			} else if rc != 2 {
				t.Error("wrong rc", rc)
			} else if want, got := string(value), string(val); want != got {
				t.Errorf("wrong value: want %q, got %q", want, got)
			}
		})

		t.Run("dec 1", func(t *testing.T) {
			if val, rc, err := ds.Get(key, -1); err != nil {
				t.Error(err)
			} else if rc != 1 {
				t.Error("wrong rc", rc)
			} else if want, got := string(value), string(val); want != got {
				t.Errorf("wrong value: want %q, got %q", want, got)
			}
		})

		t.Run("remove", func(t *testing.T) {
			for i := 0; i < 2; i++ {
				if val, rc, err := ds.Get(key, -1); err != nil {
					t.Error(err)
				} else if rc != 0 {
					t.Error("wrong rc", rc)
				} else if want, got := string(value), string(val); want != got {
					t.Errorf("wrong value: want %q, got %q", want, got)
				}
				shouldExistInCXDS(t, ds, key, 0, value)
			}
		})

	})

}

// CXDSSet tests Set method of CXDS
func CXDSSet(t *testing.T, ds data.CXDS) {
	t.Helper()

	key, value := testKeyValue("something")

	t.Run("zero", func(t *testing.T) {
		defer shouldPanic(t)
		ds.Set(key, value, 0)
	})

	t.Run("negative", func(t *testing.T) {
		defer shouldPanic(t)
		ds.Set(key, value, -1)
	})

	t.Run("new", func(t *testing.T) {
		if rc, err := ds.Set(key, value, 1); err != nil {
			t.Error(err)
		} else if rc != 1 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 1, value)
	})

	t.Run("twice", func(t *testing.T) {
		if rc, err := ds.Set(key, value, 1); err != nil {
			t.Error(err)
		} else if rc != 2 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 2, value)
	})

	t.Run("three times", func(t *testing.T) {
		if rc, err := ds.Set(key, value, 2); err != nil {
			t.Error(err)
		} else if rc != 4 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 4, value)
	})

}

// CXDSInc tests Inc method of CXDS
func CXDSInc(t *testing.T, ds data.CXDS) {
	t.Helper()

	var key, value = testKeyValue("something")

	t.Run("not exist", func(t *testing.T) {
		for _, inc := range testIncs() {
			if rc, err := ds.Inc(key, inc); err == nil {
				t.Error("missing error")
			} else if err != data.ErrNotFound {
				t.Error("unexpected error:", err)
			} else if rc != 0 {
				t.Error("wrong rc", rc)
			}
			shouldNotExistInCXDS(t, ds, key)
		}
	})

	if _, err := ds.Set(key, value, 1); err != nil {
		t.Error(err)
		return
	}

	t.Run("zero", func(t *testing.T) {
		if rc, err := ds.Inc(key, 0); err != nil {
			t.Error(err)
		} else if rc != 1 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 1, value)
	})

	t.Run("inc", func(t *testing.T) {
		if rc, err := ds.Inc(key, 1); err != nil {
			t.Error(err)
		} else if rc != 2 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 2, value)
	})

	t.Run("dec", func(t *testing.T) {
		if rc, err := ds.Inc(key, -1); err != nil {
			t.Error(err)
		} else if rc != 1 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 1, value)
	})

	t.Run("inc 2", func(t *testing.T) {
		if rc, err := ds.Inc(key, 2); err != nil {
			t.Error(err)
		} else if rc != 3 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 3, value)
	})

	t.Run("dec 2", func(t *testing.T) {
		if rc, err := ds.Inc(key, -2); err != nil {
			t.Error(err)
		} else if rc != 1 {
			t.Error("wrong rc", rc)
		}
		shouldExistInCXDS(t, ds, key, 1, value)
	})

	t.Run("remove", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			if rc, err := ds.Inc(key, -1); err != nil {
				t.Error(err)
			} else if rc != 0 {
				t.Error("wrong rc", rc)
			}
			shouldExistInCXDS(t, ds, key, 0, value)
		}
	})

}

func CXDSDel(t *testing.T, ds data.CXDS) {
	t.Helper()

	var key, value = testKeyValue("something")

	t.Run("not found", func(t *testing.T) {
		if err := ds.Del(key); err != nil {
			t.Error(err)
		}
	})

	t.Run("found", func(t *testing.T) {
		if _, err := ds.Set(key, value, 1); err != nil {
			t.Fatal(err)
		}
		if err := ds.Del(key); err != nil {
			t.Error(err)
		}
		shouldNotExistInCXDS(t, ds, key)
	})
}

func CXDSIterate(t *testing.T, ds data.CXDS) {
	t.Helper()

	t.Run("no objects", func(t *testing.T) {

		var called int

		var err = ds.Iterate(cipher.SHA256{},
			func(hash cipher.SHA256, rc uint32, val []byte) (err error) {
				called++
				return
			})

		if err != nil {
			t.Error(err)
		}

		if called != 0 {
			t.Errorf("wrong times called: expected 0, called %d", called)
		}

	})

	var keys, values = addValues(t, ds, "one", "two", "three", "four")

	// make a value to be with zero-rc for the test
	if _, err := ds.Inc(keys[0], -1); err != nil {
		t.Fatal(err)
	}

	t.Run("since", func(t *testing.T) {

		for shift, since := range keys {
			var called int

			var err = ds.Iterate(since,
				func(hash cipher.SHA256, rc uint32, val []byte) (err error) {

					if called >= len(keys) {
						t.Errorf("wrong times called: expected %d, got %d",
							len(keys), called+1)
						return data.ErrStopIteration
					}

					var index = (shift + called) % len(keys)

					if hash != keys[index] {
						t.Error("wrong hash", shift, called)
					}

					if bytes.Compare(val, values[index]) != 0 {
						t.Error("wrong value", shift, called)
					}

					called++
					return
				})

			if err != nil {
				t.Error(err)
			}

			if called >= len(keys) {
				t.Errorf("wrong times called: expected %d, got %d",
					len(keys), called)
			}

		}

	})

	t.Run("stop", func(t *testing.T) {

		var called int

		var err = ds.Iterate(cipher.SHA256{},
			func(cipher.SHA256, uint32, []byte) error {
				called++
				return data.ErrStopIteration
			})

		if err != nil {
			t.Error(err)
		}

		if called != 1 {
			t.Errorf("wrong times called: expected 1, got %d", called)
		}

	})

	t.Run("pass error", func(t *testing.T) {

		var (
			called    int
			testError = errors.New("test error")
		)

		var err = ds.Iterate(cipher.SHA256{},
			func(cipher.SHA256, uint32, []byte) error {
				called++
				return data.ErrStopIteration
			})

		if err == nil {
			t.Error("missing error")
		} else if err != testError {
			t.Error("unexpected error:", err)
		}

		if called != 1 {
			t.Errorf("wrong times called: expected 1, got %d", called)
		}

	})

}

func CXDSIterateDel(t *testing.T, ds data.CXDS) {
	t.Helper()

	//
}

func CXDSAmount(t *testing.T, ds data.CXDS) {
	t.Helper()

	//
}

func CXDSVolume(t *testing.T, ds data.CXDS) {
	t.Helper()

	//
}

// CXDSClose tests Close method of CXDS
func CXDSClose(t *testing.T, ds data.CXDS) {
	t.Helper()

	if err := ds.Close(); err != nil {
		t.Error(err)
	}
	if err := ds.Close(); err != nil {
		t.Error(err)
	}
}
