// Package cxoutils implements common utilities for
// CXO, that can be used, or can be not used by
// end-user. The package implements methods to
// remove old Root objects and to remove ownerless
// objects from databases.
//
// The CXO never remove objects, even if an objects
// is not used anymore. And every object has rc
// (references counter). If the rc is zero, then
// this object is ownerless and can be removed.
//
// And the same for Root objects. The CXO keeps all
// Root objects. But who interest old, replaced Root
// objects?
package cxoutils

import (
	"time"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/skyobject"
)

// RemoveRootObjects removes old Root objects from given
// *skyobject.Container keeping last n-th. Call this method
// using some interval. The method affects all feeds of
// the Container and all heads.
//
// If a feed contains more then one head, then the method
// keeps last n-th Root objects of every head.
func RemoveRootObjects(c *skyobject.Container, keepLast int) (err error) {

	for _, pk := range c.Feeds() {

		var heads []uint64
		if heads, err = c.Heads(pk); err != nil {
			return
		}

	HeadLoop:
		for _, nonce := range heads {
			var seq uint64
			if seq, err = c.LastRootSeq(pk, nonce); err != nil {
				err = nil // clear
				continue  // not a real error
			}

			if seq < uint64(keepLast) {
				continue
			}

			var goDown = seq - uint64(keepLast) // positive

			for ; goDown > 0; goDown-- {

				if err = c.DelRoot(pk, nonce, goDown); err != nil {
					if err == data.ErrNotFound {
						err = nil // clear error
						continue HeadLoop
					}
					return // a failure (CXDS not found error?)
				}

			}

			// seq = 0 (goDown == 0)
			if err = c.DelRoot(pk, nonce, 0); err != nil {
				if err == data.ErrNotFound {
					err = nil // clear error
					continue HeadLoop
				}

				return
			}

			// continue HeadLoop

		} // head loop

	} // feed loop

	return
}

// RemoveObjects with rc == 0 from CXDS
func RemoveObjects(c *skyobject.Container, timeout time.Duration) (err error) {

	var (
		db = c.DB().CXDS()

		tc <-chan time.Time
	)

	if timeout > 0 {
		var tm = time.NewTimer(timeout)
		tc = tm.C
		defer tm.Stop()
	}

	err = db.IterateDel(
		func(key cipher.SHA256, rc uint32, _ []byte) (del bool, err error) {

			// delte if rc is zero and value is not cached
			del = (rc == 0) && (c.IsCached(key) == false)

			select {
			case <-tc:
				err = data.ErrStopIteration // stop by timeout
			default:
			}

			return
		})

	return
}

/*

// CXDSDumpElement used to encode elements of CXDS to dump
type CXDSDumpElement struct {
	Key cipher.SHA256 // key of the objects
	RC  uint32        // references counter
	Val []byte        // value
}

// CXDSDumpHead used to keep CXDS version and brief info
type CXDSDumpHead struct {
	//
}

func Dump(ds data.CXDS, to io.Writer) (err error) {
	ds.Iterate(func(key cipher.SHA256, rc uint32, val []byte))
}

*/
