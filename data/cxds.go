package data

import (
	"time"

	"github.com/skycoin/skycoin/src/cipher"
)

//
// Iterators.
//

// An IterateObjectsFunc used to iterate over objects
// of the CXDS. All arguments are read only and must
// not be modified.
type IterateObjectsFunc func(key cipher.SHA256, obj *Object) (err error)

// An IterateObjectsDelFunc used to iterate over objects
// deleting them by choose. All arguments are read only
// and must not be modified.
type IterateObjectsDelFunc func(
	key cipher.SHA256,
	obj *Object,
) (
	del bool,
	err error,
)

//
// CXDS in person.
//

// A CXDS is interface of CX data store. The CXDS is
// key-value store with references counters. There is
// data/cxds implementation that contains boltdb based
// and in-memory (golang map based) implementations of
// the CXDS. The CXDS returns ErrNotFound from this
// package if any value has not been found. The
// references counters is number of objects that points
// to an object. E.g. schema of the CXDS is
//
//     key -> {val, rc, last_access_time, created_at_time}
//
// The CXDS keeps elements with rc == 0. End-user should
// track size of the DB and remove objects that doesn't
// used to free up space.
//
// Object in the CXDS can be strored by some order, but
// but the oreder can be chaotic. The basic requirement
// is ability to continue an iteration after pause.
type CXDS interface {

	// Hooks retursn accessor to hooks of the CXDS.
	// The Hooks retuns nil if DB doesn't support
	// hooks.
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
	// Get methods.
	//
	//
	// Get methods used to obtain an object. If object dosn't
	// exist, then Get methods return ErrNotFound.
	//
	// Get Object by key updating its last access time.
	// Result contains previous last access time.
	Get(key cipher.SHA256) (obj *Object, err error)
	// GetIncr is the same as the Get but it changes
	// references counter using provided argument.
	// Result contains new RC values.
	GetIncr(key cipher.SHA256, incrBy int64) (obj *Object, err error)
	// GetNotTouch is the same as the Get but it
	// doesn't update last access time.
	GetNotTouch(key cipher.SHA256) (obj *Object, err error)
	// GetIncNotTouch is the same as the GetIncr but
	// it doesn't update last access time.
	GetIncrNotTouch(key cipher.SHA256, incrBy int64) (obj *Object, err error)

	//
	// Set methods.
	//
	//
	// Set methods used to create new object. The Set method
	// increments RC by 1 (except SetIncr and SetIncrNotTouch).
	// Result allways contains new RC and previous last access
	// time.
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
	// SetRaw set given object as is. If object alreday exists,
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
	// parallel Get-/Set-/Incr-/Del call can be performed with
	// call of the Iterate at the same time. The Iterate
	// method can skip elements created during its call.
	// But the Iterate never called for deleted obejcts.
	// Any order is not guaranteed. The Iterate can lock DB
	// by time of the IterateObjectsFunc call. The Iterate
	// method never updates access time. See also docs
	// for the IterateObjectsFunc.
	Iterate(iterateFunc IterateObjectsFunc) (err error)
	// IterateDel used to remove objects. See also docs for
	// the IterateObjectsDelFunc. The IterateDel works like
	// the Iterate (e.g. don't lock DB allowing long calls).
	IterateDel(iterateFunc IterateObjectsDelFunc) error

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
	// etc. The 'all' is volume of all obejcts,
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
