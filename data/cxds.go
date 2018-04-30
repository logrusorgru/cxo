package data

import (
	"time"

	"github.com/skycoin/skycoin/src/cipher"
)

//
// Iterator.
//

// An IterateKeysFunc used to iterate over keys.
type IterateKeysFunc func(key cipher.SHA256) (err error)

//
// CXDS in person.
//

// A CXDS is interface of CX data store. The CXDS is
// key-value store with references counters. There is
// data/cxds implementations that contains boltdb based
// and in-memory (golang map based) implementations of
// the CXDS. The CXDS returns ErrNotFound from this
// package if any value is not found. The references
// counter (RC) is number of objects that points to an
// object. The RC iz zero or above by design, but it's
// not restricted to use negative RC for some developer
// reasons. The CXO can't make an RC negative. E.g. a
// developer for his own needs may make it negative and
// it's ok for the CXDS.
//
// Schema of the CXDS is
//
//     key -> {val, rc, access_time, created_at_time}
//
// The CXDS keeps elements with rc == 0. End-user should
// track size of the DB and remove objects that doesn't
// used to free up space.
//
// Order of obejcts is not defined.
//
// Access and create times.
//
// The access time is time of last access. Methods,
// that returns Object or access time, returns previous
// access time (because last access time is 'now' and
// no one need it). Every method (except *NotTouch)
// updates the access time in DB returning previous
// one. The *NotTouch methods doesn't update the access
// time, but the SetNotTouch and SetIncrNotTouch methos
// set the access time to now creating object (e.g. if
// object doesn't exist before the Set* call). Thus,
// real access time in DB is equal to create time or
// later. But the Set* methos returns previous access
// time that is the begining of Unix Epoch (e.g.
// accessTime.UnixNano() == 0) if object created.
//
// Also, ther is SetRaw method that puts value as it.
// And this way, if access time or crate time is zero,
// then it zero. No changes.
//
// RC in response.
//
// All methods returns new RC everytime. E.g. *Incr*
// methods used to change the RC of an obejct in DB.
// For example the GetIncr changes the RC and returns
// object with new (changed) RC.
//
// Not touch.
//
// The *NotTouch methods doesn't update last access
// time. Keep in mind that the SetNotTouch method
// sets access time to now if object doesn't exist.
type CXDS interface {

	// Hooks retursn accessor to hooks of the CXDS.
	// The Hooks retuns nil if DB doesn't support
	// hooks. The Hooks are experimatal and not
	// tested.
	Hooks() (hooks Hooks)

	//
	// Touch (last access time).
	//
	//
	// Touch object by its key updating its last access time.
	// The Touch method returns ErrNotFound if object doesn't
	// exist. The Touch returns previous last access time.
	Touch(key cipher.SHA256) (access time.Time, err error)

	//
	// Get* methods.
	//
	//
	// Get* methods used to obtain an object. If object dosn't
	// exist, then the Get* methods return ErrNotFound.
	//
	// Get Object by key updating its last access time.
	Get(key cipher.SHA256) (obj *Object, err error)
	// GetIncr is the same as the Get but it changes
	// RC using provided argument. The argument can
	// be zero, actually.
	GetIncr(key cipher.SHA256, incrBy int64) (obj *Object, err error)
	// GetNotTouch is the same as the Get but it
	// doesn't update last access time.
	GetNotTouch(key cipher.SHA256) (obj *Object, err error)
	// GetIncNotTouch is the same as the GetIncr but
	// it doesn't update last access time.
	GetIncrNotTouch(key cipher.SHA256, incrBy int64) (obj *Object, err error)

	//
	// Set* methods.
	//
	//
	// Set* methods used to create new object. If obejct is
	// alredy exists, then Set* method updates access time,
	// and increments RC. The Set and SetNotTouch methods
	// increments the RC by +1. The SetIcnr and
	// SetIncrNotTouch methods use provided value.
	//
	// Set creates new object or updates existsing. The Set
	// method equal to the SetIncr method with `incrBy = 1`.
	Set(key cipher.SHA256, val []byte) (obj *Object, err error)
	// SetIncr uses provided inrBy argument to change
	// RC of object. If object already exists, then
	// no auto +1 added. The SetIncr with `incrBy = 1`
	// is the same as the Set.
	SetIncr(
		key cipher.SHA256, // : hash of the object
		val []byte, //        : encoded object
		incrBy int64, //      : inc- or decrement RC by this value
	) (
		obj *Object, //       : object with new RC and previous last access time
		err error, //         : error if any
	)
	// SetNotTouch is the same as the Set but it
	// doesn't update last access time.
	SetNotTouch(
		key cipher.SHA256, // : hash of the object
		val []byte, //        : encoded object
	) (
		obj *Object, //       : object with new RC and previous last access time
		err error, //         : error if any
	)
	// SetIncrNotTouch is the same as the SetIncr but
	// it doesn't update last access time.
	SetIncrNotTouch(
		key cipher.SHA256, // : hash of the object
		val []byte, //        : encoded object
		incrBy int64, //      : inc- or decrement RC by this value
	) (
		obj *Object, //       : object with new RC and previous last access time
		err error, //         : error if any
	)
	// SetRaw sets given object as is. If object alreday exists,
	// then the SetRaw method overwrites existing one.
	SetRaw(key cipher.SHA256, obj *Object) (err error)

	//
	// Incr methods (references counter).
	//
	//
	// Incr methods used to change RC of an object
	// returning new RC or error if any. If object
	// doesn't exist then Incr methods return
	// ErrNotFound error.
	//
	// Incr inc- or decrements RC of object with given
	// key using provided value. The Incr returns new
	// RC or error if any.
	Incr(
		key cipher.SHA256, // : hash of the object
		incrBy int64, //      : inr- or decrement by
	) (
		rc int64, //          : new RC
		access time.Time, //  : previous last access time
		err error, //         : error if any
	)
	// IncrNotTouch is the same as the Incr but it
	// doesn't update last access time.
	IncrNotTouch(
		key cipher.SHA256, // : hash of the object
		incrBy int64, //      : inr- or decrement by
	) (
		rc int64, //          : new RC
		access time.Time, //  : previous last access time
		err error, //         : error if any
	)

	//
	// Delete methods.
	//
	//
	// Take deletes an object unconditionally returinig:
	// (1) deleted object, (2) ErrNotFound if object
	// doesn't exist (3) any other error (DB failure,
	// for exmple).
	Take(key cipher.SHA256) (obj *Object, err error)
	// Del deletes an object unconditionally. The Del
	// method returns ErrNotFound error if object doens't
	// exist in DB.
	Del(key cipher.SHA256) (err error)

	//
	// Iterate methods.
	//
	//
	// Iterate all keys in CXDS. Use ErrStopIteration to stop
	// an iteration. The Iterate method never lock DB and any
	// parallel Get-/Set-/Incr-/Del/etc call can be performed
	// with call of the Iterate at the same time. But, the
	// Iterate can lock DB for time of the IterateKeysFunc
	// call.
	//
	// Iterate never updates last access time.
	//
	// Iterate can skip new objects, and use deleted objects.
	Iterate(iterateFunc IterateKeysFunc) (err error)

	//
	// Stat.
	//
	//
	// Amount of objects. The 'all' means amount of all objects
	// and the 'used' is amount of objects with RC greater then
	// zero.
	Amount() (all, used int64)
	// Volume of objects. The volume measured
	// in bytes. The volume consist of payload
	// only and not includes keys and any other
	// meta information like references counter
	// etc. The 'all' is volume of all objects,
	// and the 'used' is volume of objects with
	// RC greater then zero.
	Volume() (all, used int64)

	// IsSafeClosed is flag that means that DB has been
	// closed successfully last time. If the IsSafeClosed
	// returns false, then may be some repair required (it
	// depends).
	IsSafeClosed() bool

	// Close the CXDS
	Close() (err error)
}
