package rediscxds

import (
	"bufio"
	"fmt"
	"time"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/skycoin/src/cipher"

	"github.com/mediocregopher/radix.v3"
	"github.com/mediocregopher/radix.v3/resp"
)

// Convention
//   - if 'create' field is zero (that is equal to time.Time{})
//     then object doesn't exist

// A Redis implments data.CXDS
// interface over Redis database.
type Redis struct {
	pool         *radix.Pool // conenctions pool
	isSafeClosed bool        // current state

	// LRU timeout feature
	expire     time.Duration //
	expireFunc ExpireFunc    //

	hooks
}

// NewCXDS creates CXDS based on Redis
func NewCXDS(
	network string, // : "tcp", "tcp4" or "tcp6"
	addr string, //    : address of Redis server
	conf *Config, //   : configurations
) (
	ds data.CXDS, //   : the data.CXDS
	err error, //      : error if any
) {

	var pool *radix.Pool
	if pool, err = radix.NewPool(network, addr, size, opts...); err != nil {
		return
	}

	var rc Redis
	rc.pool = pool

	if rc.isSafeClosed, err = rc.getSafeClosed(); err != nil {
		pool.Close()
		return
	}

	if err = rc.setSafeClosed(false); err != nil {
		pool.Close()
		return
	}

	ds = &rc
	return
}

func (r *Redis) setSafeClosed(t bool) (err error) {
	err = r.pool.Do(radix.FlatCmd(nil, "SET", "safeClosed", t))
	return
}

func (r *Redis) getSafeClosed() (safeClosed bool, err error) {
	err = r.pool.Do(radix.FlatCmd(&safeClosed, "GET", "safeClosed"))
	return
}

// =============================================================================
// -----------------------------------------------------------------------------




	Touch(key cipher.SHA256) (access time.Time, err error)

	Get(key cipher.SHA256) (obj *Object, err error)
	GetIncr(key cipher.SHA256, incrBy int64) (obj *Object, err error)
	GetNotTouch(key cipher.SHA256) (obj *Object, err error)
	GetIncrNotTouch(key cipher.SHA256, incrBy int64) (obj *Object, err error)

	Set(key cipher.SHA256, val []byte) (obj *Object, err error)
	SetIncr(
		key cipher.SHA256, // : hash of the object
		val []byte, //        : encoded object
		incrBy int64, //      : inc- or decrement RC by this value
	) (
		obj *Object, //       : object with new RC and previous last access time
		err error, //         : error if any
	)
	SetNotTouch(
		key cipher.SHA256, // : hash of the object
		val []byte, //        : encoded object
	) (
		obj *Object, //       : object with new RC and previous last access time
		err error, //         : error if any
	)
	SetIncrNotTouch(
		key cipher.SHA256, // : hash of the object
		val []byte, //        : encoded object
		incrBy int64, //      : inc- or decrement RC by this value
	) (
		obj *Object, //       : object with new RC and previous last access time
		err error, //         : error if any
	)
	SetRaw(key cipher.SHA256, obj *Object) (err error)

	Incr(
		key cipher.SHA256, // : hash of the object
		incrBy int64, //      : inr- or decrement by
	) (
		rc int64, //          : new RC
		access time.Time, //  : previous last access time
		err error, //         : error if any
	)
	IncrNotTouch(
		key cipher.SHA256, // : hash of the object
		incrBy int64, //      : inr- or decrement by
	) (
		rc int64, //          : new RC
		access time.Time, //  : previous last access time
		err error, //         : error if any
	)

	Take(key cipher.SHA256) (obj *Object, err error)
	Del(key cipher.SHA256) (err error)

	Iterate(iterateFunc IterateObjectsFunc) (err error)
	IterateDel(iterateFunc IterateObjectsDelFunc) error

	Amount() (all, used int64)
	Volume() (all, used int64)


// -----------------------------------------------------------------------------
// =============================================================================

// IsSafeClosed is flag that means that DB has been
// closed successfully last time. If the IsSafeClosed
// returns false, then may be some repair required (it
// depends).
func (r *Redis) IsSafeClosed() (safeClosed bool) {
	return r.isSafeClosed
}

// Close the CXDS
func (r *Redis) Close() (err error) {

	err = r.client.Do(radix.FlatCmd(nil, "SET", "safeClosed", true))
	if err != nil {
		r.client.Close() // drop error
		return
	}

	return r.client.Close()
}
