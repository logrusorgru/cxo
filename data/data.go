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

	// Access contains last access time in DB.
	// Zero time can be checked using
	// `.UnixNano() == 0` instead of `.IsZero()`
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

	s.Access = r.Access.UnixNano()
	s.Create = r.Create.UnixNano()

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

	r.Access = time.Unix(0, s.Access)
	r.Create = time.Unix(0, s.Create)

	return
}

// Object represents CX object
type Object struct {
	// Val contains encoded value
	Val []byte
	// RC is references counter
	RC int64
	// Access is last access time.
	// Zero time can be checked using
	// `.UnixNano() == 0` instead of
	// `.IsZero()`
	Access time.Time
	// Create is time where the
	// Object was saved in DB.
	Create time.Time
}

type object struct {
	Val    []byte
	RC     int64
	Access int64 // unix nano
	Create int64 // unix nano
}

// Encode the Object to []bye
func (o *Object) Encode() (b []byte) {

	var obj object

	obj.Val = o.Val
	obj.RC = o.RC

	obj.Access = o.Access.UnixNano()
	obj.Create = o.Create.UnixNano()

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

	o.Access = time.Unix(0, obj.Access)
	o.Create = time.Unix(0, obj.Create)

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
