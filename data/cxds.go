package data

import (
	"github.com/skycoin/skycoin/src/cipher"
)

// An IterateObjectsFunc used to iterate over objects
// of the CXDS. All arguments are read only and must
// not be modified.
type IterateObjectsFunc func(key cipher.SHA256, rc uint32, val []byte) error

// An IterateObjectsDelFunc used to iterate over objects
// deleting them by choose. All arguments are read only
// and must not be modified.
type IterateObjectsDelFunc func(
	key cipher.SHA256,
	rc uint32,
	val []byte,
) (
	del bool,
	err error,
)

// A CXDS is interface of CX data store. The CXDS is
// key-value store with references counters. There is
// data/cxds implementation that contains boltdb based
// and in-memory (golang map based) implementations of
// the CXDS. The CXDS returns ErrNotFound from this
// package if any value has not been found. The
// references counters is number of objects that points
// to an object. E.g. schema of the CXDS is
//
//     key -> {rc, val}
//
// The CXDS keeps elements with rc == 0. End-user should
// track size of the DB and remove objects that doesn't
// used to free up space.
//
// Object in the CXDS can be strored by some order, but
// but the oreder can be chaotic. The basic requirement
// is ability to continue an iteration after pause.
type CXDS interface {

	// Get and change references counter (rc). If the
	// inc argument is zero then the rc will be leaved
	// as is. If value with given key doesn't exist, then
	// the Get method returns (nil, 0, data.ErrNotFound).
	// Use negative inc argument to reduce the rc and
	// positive to increase it
	Get(key cipher.SHA256, inc int) (val []byte, rc uint32, err error)

	// Set and change references counter (rc). If the inc
	// argument is negative or zero, then the Set method
	// panics. Other words, the Set method used to create
	// and increase the rc (increase at least by one). E.g.
	// it's impossible to set vlaue with zero-rc
	Set(key cipher.SHA256, val []byte, inc int) (rc uint32, err error)

	// Inc increments or decrements (if given inc is negative)
	// references count for value with given key. If given
	// inc argument is zero, then the Inc method checks
	// presence of the value. E.g. if it returns ErrNotFound
	// then value doesn't exist. The Inc returns new rc
	Inc(key cipher.SHA256, inc int) (rc uint32, err error)

	// Iterate all keys in CXDS. Use ErrStopIteration to stop
	// an iteration. The Iterate method never lock DB and any
	// parallel Get/Set/Inc/Del call can be performed with call
	// of the Iterate at the same time. The Iterate guarantees
	// that all elements of DB will be iterated inclusive or
	// exclusive elements created during call of the Iterate
	// method. And exclusive elements deleted during call of
	// the Iterate method. Any order is not guaranteed. The
	// Iterate can lock DB by time of the IterateObjectsFunc
	// call. See also docs for the IterateObjectsFunc.
	Iterate(iterateFunc IterateObjectsFunc) (err error)

	// IterateDel used to remove objects. See also docs for
	// the IterateObjectsDelFunc. The IterateDel works like
	// the Iterate (e.g. don't lock DB allowing long calls).
	IterateDel(iterateFunc IterateObjectsDelFunc) error

	// Del removes object with given key unconditionally.
	// The Del method doesn't return an error if object
	// doesn't exist. Handle with care.
	Del(key cipher.SHA256) (err error)

	//
	// Stat
	//

	// Amount of objects.
	Amount() (all, used int)
	// Volume of objects. The volume measured
	// in bytes. The volume consist of payload
	// only and not includes keys and any other
	// meta information like references counter
	// etc.
	Volume() (all, used int)

	// IsSafeClosed is flag that means that DB has been
	// closed successfully last time. If the IsSafeClosed
	// returns false, then may be some repair required (it
	// depends).
	IsSafeClosed() bool

	//
	// Close
	//

	// Close the CXDS
	Close() (err error)
}
