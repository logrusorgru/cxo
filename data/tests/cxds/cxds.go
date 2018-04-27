// Package cxds consis of test cases for data.CXDS
// <github.com/skycoin/cxo/data#CXDS> and Hooks.
package cxds

import (
	"bytes"
	"errors"
	"testing"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

/*

func keyValueFromString(s string) (key cipher.SHA256, val []byte) {
	val = []byte(s)
	key = cipher.SumSHA256(val)
	return
}

func incrBys() []int {
	return []int{-1, 0, 1}
}

func shouldNotExist(t *testing.T, ds data.CXDS, key cipher.SHA256) {
	t.Helper()
	t.Log("should not exist", key.Hex()[:7])

	if obj, err = ds.GetNotTouch(key); err == nil {
		if obj == nil {
			t.Error("error is nil, and object is nil (fatality)")
			return
		}
		t.Error("unexpected object (should not exist)")
	} else if err != data.ErrNotFound {
		t.Errorf("unexpected error, want 'not found', got %q", err)
	}
}

func shouldExist(
	t *testing.T,
	ds data.CXDS,
	key cipher.SHA256,
	rc uint32,
	val []byte,
) {

	t.Helper()
	t.Log("should exist", key.Hex()[:7])

	var obj, err = ds.GetNotTouch(key)

	if err != nil {
		if err == data.ErrNotFound {
			t.Error("object not found (should exist)")
			return
		}
		t.Error("unexpected error:", err)
		return
	}

	if rc != obj.RC {
		t.Error("wrong RC, want %d, got %d", rc, obj.RC)
	}

	if bytes.Compare(val, obj.Val) != 0 {
		t.Error("wrong value of object")
	}
}

func shouldPanic(t *testing.T) {
	t.Helper()
	t.Log("should panic")

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

		if _, err := ds.Set(k, v); err != nil {
			t.Fatal(err)
		}

		keys = append(keys, k)
		vlaues = append(vlaues, v)
	}

	return
}

// Get tests Get method of CXDS
func Get(t *testing.T, ds data.CXDS) {

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

// Set tests Set method of CXDS
func Set(t *testing.T, ds data.CXDS) {

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

// Inc tests Inc method of CXDS
func Inc(t *testing.T, ds data.CXDS) {

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

// Del is test case for Del method
func Del(t *testing.T, ds data.CXDS) {

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

func indexOf(keys []cipher.SHA256, key cipher.SHA256) (i int) {
	var k cipher.SHA256
	for i, k = range keys {
		if k == key {
			return
		}
	}
	return -1 // not found
}

// Iterate is test case for Iterate method
func Iterate(t *testing.T, ds data.CXDS) {

	t.Run("no objects", func(t *testing.T) {

		var called int

		var err = ds.Iterate(func(cipher.SHA256, uint32, []byte) (_ error) {
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

	t.Run("four objects", func(t *testing.T) {

		var called int

		var err = ds.Iterate(
			func(hash cipher.SHA256, rc uint32, val []byte) (err error) {

				if called >= len(keys) {
					t.Errorf("wrong times called: expected %d, got %d",
						len(keys), called+1)
					return data.ErrStopIteration
				}

				var index = indexOf(keys, hash)

				if index < 0 {
					t.Error("unexpected hash:", hash.Hex(), called)
					return data.ErrStopIteration
				}

				if bytes.Compare(val, values[index]) != 0 {
					t.Error("wrong value", called, index)
				}

				called++
				return
			})

		if err != nil {
			t.Error(err)
		}

		if called > len(keys) {
			t.Errorf("wrong times called: expected %d, got %d",
				len(keys), called)
		}

	})

	t.Run("parallel get", func(t *testing.T) {

		var (
			called int
			get    = make(chan struct{})
			done   = make(chan struct{})
		)

		go func() {
			defer close(done)
			for i := 0; i < len(keys); i++ {

				<-get

				var val, rc, err = ds.Get(keys[i], 0)

				if err != nil {
					t.Error(err)
				}

				if (i == 0 && rc != 0) || (i != 0 && rc != 1) {
					t.Error("wrong rc")
				}

				if bytes.Compare(val, values[i]) != 0 {
					t.Error("wrong value")
				}
			}
		}()

		var err = ds.Iterate(
			func(hash cipher.SHA256, rc uint32, val []byte) (err error) {

				get <- struct{}{} // invoke parallel get

				if called >= len(keys) {
					t.Errorf("wrong times called: expected %d, got %d",
						len(keys), called+1)
					return data.ErrStopIteration
				}

				var index = indexOf(keys, hash)

				if index < 0 {
					t.Error("unexpected hash:", hash.Hex(), called)
					return data.ErrStopIteration
				}

				if bytes.Compare(val, values[index]) != 0 {
					t.Error("wrong value", called, index)
				}

				called++
				return
			})

		if err != nil {
			t.Error(err)
		}

		if called > len(keys) {
			t.Errorf("wrong times called: expected %d, got %d",
				len(keys), called)
		}

		<-done

	})

	// TODO (kostyarin): test Set/Inc/Del inside the Iterate

	t.Run("stop", func(t *testing.T) {

		var called int

		var err = ds.Iterate(
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

		var err = ds.Iterate(
			func(cipher.SHA256, uint32, []byte) error {
				called++
				return testError
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

// Amount is test case for Amount method
func Amount(t *testing.T, ds data.CXDS) {
	//
}

// Volume is test case for Volume method
func Volume(t *testing.T, ds data.CXDS) {
	//
}

// Close is test case for Close method
func Close(t *testing.T, ds data.CXDS) {
	if err := ds.Close(); err != nil {
		t.Error(err)
	}
	if err := ds.Close(); err != nil {
		t.Error(err)
	}
}

*/
