package data

import (
	"errors"
	"time"

	"github.com/skycoin/skycoin/src/cipher"
	"github.com/skycoin/skycoin/src/cipher/encoder"
)

// common errors
var (
	ErrNotFound      = errors.New("not found")
	ErrStopIteration = errors.New("stop iteration")
	ErrNoSuchFeed    = errors.New("no such feed")
	ErrNoSuchHead    = errors.New("no such head")
	ErrInvalidSize   = errors.New("invalid size of encoded data")
)

// A DB represents joiner of IdxDB and CXDS
type DB struct {
	cxds  CXDS
	idxdb IdxDB
}

// IdxDB of the DB
func (d *DB) IdxDB() IdxDB {
	return d.idxdb
}

// CXDS of the DB
func (d *DB) CXDS() CXDS {
	return d.cxds
}

// Close the DB and all underlying
func (d *DB) Close() (err error) {
	if err = d.cxds.Close(); err != nil {
		d.idxdb.Close() // drop error
	} else {
		err = d.idxdb.Close() // use this error
	}
	return
}

// NewDB creates DB by given CXDS and IdxDB.
// The arguments must not be nil
func NewDB(cxds CXDS, idxdb IdxDB) *DB {
	if cxds == nil {
		panic("missing CXDS")
	}
	if idxdb == nil {
		panic("missing IdxDB")
	}
	return &DB{cxds, idxdb}
}

// A Root represents meta information
// of a saved skyobject.Root
type Root struct {
	// Hash of the Root
	Hash cipher.SHA256
	// Sig contains signature
	// of the Root
	Sig cipher.Sig

	// Access contains last
	// access time in DB
	Access time.Time
	// Create contains time whie the
	// Root has been saved in DB
	Create time.Time
}

// root used to encode and decode the Root
type root struct {
	Hash   cipher.SHA256 // hash
	Sig    cipher.Sig    // sig
	Access int64         // unix nano
	Create int64         // unix nano
}

// Validate the Root
func (r *Root) Validate() (err error) {
	if r.Hash == (cipher.SHA256{}) {
		return errors.New("(idxdb.Root.Validate) empty Hash")
	}

	if r.Sig == (cipher.Sig{}) {
		return errors.New("(idxdb.Root.Validate) empty Sig")
	}
	return
}

// Touch updates last access time of the Root
// returning previous access time
func (r *Root) Touch() (access time.Time) {
	access = r.Access
	r.Access = time.Now()
	return
}

// Encode the Root
func (r *Root) Encode() (p []byte) {
	var s root
	s.Hash = r.Hash
	s.Sig = r.Sig

	if r.Access.IsZero() == false {
		s.Access = r.Access.UnixNano()
	}

	if r.Create.IsZero() == false {
		s.Create = r.Create.UnixNano()
	}

	return encoder.Serialize(&s)
}

// Decode given encoded Root to this one.
// The Decode method never check input
// length and if the input is longer
// then encoded value, then no error
// returned
func (r *Root) Decode(p []byte) (err error) {

	var s root
	if err = encoder.DeserializeRaw(p, &s); err != nil {
		return
	}

	r.Hash = s.Hash
	r.Sig = s.Sig

	if s.Access == 0 {
		r.Access = time.Time{}
	} else {
		r.Access = time.Unix(0, s.Access)
	}

	if s.Create == 0 {
		r.Create = time.Time{}
	} else {
		r.Create = time.Unix(0, s.Create)
	}

	return
}

// Object represents CX object
type Object struct {
	Val    []byte    // encoded value
	RC     int64     // references counter
	Access time.Time // last access time
	Create time.Time // created at
	User   []byte    // user provided meta-information (string)
}

type object struct {
	Val    []byte
	RC     int64
	Access int64 // unix nano
	Create int64 // unix nano
	User   []byte
}

// Encode the Object to []bye
func (o *Object) Encode() (b []byte) {

	var obj object

	obj.Val = o.Val
	obj.RC = o.RC

	if o.Access.IsZero() == false {
		obj.Access = o.Access.UnixNano()
	}

	if o.Create.IsZero() == false {
		obj.Create = o.Create.UnixNano()
	}

	obj.User = o.User

	return encoder.Serialize(&obj)
}

// Decode the Object from given []byte. The Decode
// method never check input length, i.e. if input
// is longer then encoded Object then the Decode
// method doesn't return an error
func (o *Object) Decode(p []byte) (err error) {

	var obj object
	if err = encoder.DeserializeRaw(p, &obj); err != nil {
		return
	}

	o.Val = obj.Val
	o.RC = obj.RC

	if obj.Access == 0 {
		o.Access = time.Time{} // reset existing value to zero time
	} else {
		o.Access = time.Unix(0, obj.Access)
	}

	if obj.Create == 0 {
		o.Create = time.Time{} // reset existing value to zero time
	} else {
		o.Create = time.Unix(0, obj.Create)
	}

	o.User = obj.User
	return
}

// Touch updates last access time of the Object
// returning previous last access time
func (o *Object) Touch() (lastAccess time.Time) {
	lastAccess = o.Access
	o.Access = time.Now()
	return
}

// Incr incr- or decrements RC usign provided value
func (o *Object) Incr(incrBy int64) (rc int64) {
	o.RC += incrBy
	return o.RC
}
