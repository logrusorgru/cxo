package data

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/skycoin/skycoin/src/cipher"
)

func TestDB_IdxDB(t *testing.T) {
	var (
		dc, di = new(dummyCXDS), new(dummyIdx)
		db     = NewDB(dc, di)
	)
	defer db.Close()
	if db.CXDS() != dc {
		t.Error("wrong CXDS")
	}
	if db.IdxDB() != di {
		t.Error("wrong IdxDB")
	}
}

func TestDB_CXDS(t *testing.T) {
	// moved to TestDB_IdxDB
}

func TestDB_Close(t *testing.T) {

	var e1, e2 = errors.New("error 1"), errors.New("error 2")

	var db = NewDB(&dummyCXDS{err: e1}, &dummyIdx{err: e2})
	if err := db.Close(); err == nil {
		t.Error("missing error")
	} else if err != e1 {
		t.Error("wrong error returned")
	}

	db = NewDB(&dummyCXDS{}, &dummyIdx{e2})
	if err := db.Close(); err == nil {
		t.Error("missing error")
	} else if err != e2 {
		t.Error("wrong error")
	}

	db = NewDB(&dummyCXDS{}, &dummyIdx{})
	if err := db.Close(); err != nil {
		t.Error("unexpected error:", err)
	}

}

func shouldPanic(t *testing.T) {
	if err := recover(); err == nil {
		t.Error("missing panic")
	}
}

func TestNewDB(t *testing.T) {

	t.Run("nil cxds", func(t *testing.T) {
		defer shouldPanic(t)
		NewDB(nil, nil)
	})

	t.Run("nil idx", func(t *testing.T) {
		defer shouldPanic(t)
		NewDB(&dummyCXDS{}, nil)
	})

	t.Run("valid", func(t *testing.T) {
		var (
			dc, di = new(dummyCXDS), new(dummyIdx)
			db     = NewDB(dc, di)
		)
		defer db.Close()
	})

}

func (r *Root) equal(s *Root) (eq bool) {
	eq = r.Hash == s.Hash &&
		r.Sig == s.Sig &&
		r.Access.Equal(s.Access) &&
		r.Create.Equal(s.Create)
	return
}

func TestRoot_Validate(t *testing.T) {
	var r = new(Root)

	if err := r.Validate(); err == nil {
		t.Error("missing error")
	}

	r.Hash = cipher.SumSHA256([]byte("value"))

	if err := r.Validate(); err == nil {
		t.Error("missing error")
	}

	var _, sk = cipher.GenerateKeyPair()

	r.Sig = cipher.SignHash(r.Hash, sk)

	if err := r.Validate(); err != nil {
		t.Error(err)
	}
}

func TestRoot_Touch(t *testing.T) {

	var r = new(Root)

	if last := r.Touch(); last.IsZero() == false {
		t.Error("invalid last access time")
	}

	var access = r.Access

	if last := r.Touch(); access.Equal(last) == false {
		t.Error("invalid last access time")
	}
}

func TestRoot_Encode(t *testing.T) {

	var (
		o, e = new(Root), new(Root)
		p    = o.Encode()
	)

	// blank

	if p == nil {
		t.Fatal("Encode returns nil")
	} else if len(p) == 0 {
		t.Fatal("Encode returns empty slice")
	}

	if err := e.Decode(p); err != nil {
		return
	}

	if o.equal(e) == false {
		t.Error("decoded Root is different")
	}

	// full

	o.Hash = cipher.SumSHA256([]byte("value"))

	var _, sk = cipher.GenerateKeyPair()
	o.Sig = cipher.SignHash(o.Hash, sk)

	o.Create = time.Now()
	o.Access = time.Now()

	p = o.Encode()

	if p == nil {
		t.Fatal("Encode returns nil")
	} else if len(p) == 0 {
		t.Fatal("Encode returns empty slice")
	}

	if err := e.Decode(p); err != nil {
		t.Fatal("decoding error:", err)
	}

	if o.equal(e) == false {
		t.Error("decoded Root is different")
	}

	// short input

	for i := len(p) - 1; i > 0; i-- {
		if err := e.Decode(p[:i]); err == nil {
			t.Error("missing error", i)
		}
	}

}

func TestRoot_Decode(t *testing.T) {
	// moved to TestRoot_Encode
}

// equal is test mehod to compare two Objects
func (o *Object) equal(e *Object) (eq bool) {
	eq = bytes.Compare(o.Val, e.Val) == 0 &&
		o.RC == e.RC &&
		o.Access.Equal(e.Access) &&
		o.Create.Equal(o.Create)
	return
}

func TestObject_Encode(t *testing.T) {

	var (
		o, e = new(Object), new(Object)
		p    = o.Encode()
	)

	// blank

	if p == nil {
		t.Fatal("Encode returns nil")
	} else if len(p) == 0 {
		t.Fatal("Encode returns empty slice")
	}

	if err := e.Decode(p); err != nil {
		return
	}

	if o.equal(e) == false {
		t.Error("decoded object is different")
	}

	// full

	o.Val = []byte("value")
	o.RC = 1050

	o.Create = time.Now()
	o.Access = time.Now()

	p = o.Encode()

	if p == nil {
		t.Fatal("Encode returns nil")
	} else if len(p) == 0 {
		t.Fatal("Encode returns empty slice")
	}

	if err := e.Decode(p); err != nil {
		t.Fatal("decoding error:", err)
	}

	if o.equal(e) == false {
		t.Error("decoded Root is different")
	}

	// short input

	for i := len(p) - 1; i > 0; i-- {
		t.Log("length p:", len(p), i)
		if err := e.Decode(p[:i]); err == nil {
			t.Error("missing error", i)
		}
	}

}

func TestObject_Decode(t *testing.T) {
	// moved to TestObject_Encode
}

func TestObject_Touch(t *testing.T) {

	var o = new(Object)

	if last := o.Touch(); last.IsZero() == false {
		t.Error("invalid last access time")
	}

	var access = o.Access

	if last := o.Touch(); access.Equal(last) == false {
		t.Error("invalid last access time")
	}

}

func TestObject_Incr(t *testing.T) {

	var o = new(Object)

	if rc := o.Incr(10); rc != 10 {
		t.Error("invalid new RC:", rc)
	}

	if rc := o.Incr(-10); rc != 0 {
		t.Error("invalid new RC:", rc)
	}

	if rc := o.Incr(100); rc != 100 {
		t.Error("invalid new RC:", rc)
	}

	if rc := o.Incr(100); rc != 200 {
		t.Error("invalid new RC:", rc)
	}

}

//
// dummy types implements CXDS and Idx interfaces
//

type dummyCXDS struct {
	Hooks
	err error
}

func (*dummyCXDS) Touch(cipher.SHA256) (access time.Time, err error) { return }
func (*dummyCXDS) Get(cipher.SHA256) (obj *Object, err error)        { return }

func (*dummyCXDS) GetIncr(cipher.SHA256, int64) (obj *Object, err error) {
	return
}

func (*dummyCXDS) GetNotTouch(cipher.SHA256) (obj *Object, err error) { return }

func (*dummyCXDS) GetIncrNotTouch(
	cipher.SHA256, int64,
) (obj *Object, err error) {
	return
}

func (*dummyCXDS) Set(
	cipher.SHA256, []byte,
) (obj *Object, err error) {
	return
}

func (*dummyCXDS) SetIncr(
	cipher.SHA256, []byte, int64,
) (obj *Object, err error) {
	return
}

func (*dummyCXDS) SetNotTouch(
	cipher.SHA256, []byte,
) (obj *Object, err error) {
	return
}

func (*dummyCXDS) SetIncrNotTouch(
	cipher.SHA256, []byte, int64,
) (obj *Object, err error) {
	return
}

func (*dummyCXDS) SetRaw(cipher.SHA256, *Object) (err error) { return }

func (*dummyCXDS) Incr(
	cipher.SHA256, int64,
) (rc int64, access time.Time, err error) {
	return
}

func (*dummyCXDS) IncrNotTouch(
	cipher.SHA256, int64,
) (rc int64, access time.Time, err error) {
	return
}

func (*dummyCXDS) Take(cipher.SHA256) (obj *Object, err error)  { return }
func (*dummyCXDS) Del(cipher.SHA256) (err error)                { return }
func (*dummyCXDS) Iterate(IterateObjectsFunc) (err error)       { return }
func (*dummyCXDS) IterateDel(IterateObjectsDelFunc) (err error) { return }
func (*dummyCXDS) Amount() (all, used int64)                    { return }
func (*dummyCXDS) Volume() (all, used int64)                    { return }
func (*dummyCXDS) IsSafeClosed() (sc bool)                      { return }

func (d *dummyCXDS) Close() error {
	return d.err
}

type dummyIdx struct {
	err error
}

func (*dummyIdx) Tx(func(Feeds) error) (err error) { return }

func (d *dummyIdx) Close() error {
	return d.err
}
