package bolt

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

// ScanBy used by iterators
const ScanBy int = 100

var (
	infoBucket = []byte("i") //
	infoKey    = infoBucket  // safe closed
)

// A Bolt implements data.IdxDB interface.
// The Bolt based on <github.com/boltdb/bolt>.
type Bolt struct {
	b *bolt.DB

	scanBy       int
	isSafeClosed bool
}

// NewBolt
func NewBolt(
	path string, //       :
	mode os.FileMode, //  :
	opt *bolt.Options, // :
	scanBy int, //        :
) (
	b *Bolt, //           :
	err error, //         :
) {

	var db *bolt.DB
	if db, err = bolt.Open(path, mode, opts); err != nil {
		return
	}

	var x = new(Bolt)
	x.b = db

	if scanBy <= 0 {
		x.scanBy = ScanBy
	} else {
		x.scanBy = scanBy
	}

	if x.isSafeClosed, err = x.getSafeClosed(); err != nil {
		return
	}

	if err = x.setSafeClosed(false); err != nil {
		return
	}

	return x, nil
}

func (b *Bolt) getSafeClosed() (t bool, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		var info = tx.Bucket(infoBucket)
		if info == nil {
			t = true // fresh DB
			return
		}
		var val = info.Get(infoKey)
		if len(val) == 0 {
			return // fasle
		}
		t = val[1] > 0
		return
	})
	return
}

func (b *Bolt) setSafeClosed(t bool) (err error) {
	err = b.b.Update(func(tx *bolt.Tx) (err error) {
		var info *bolt.Bucket
		if info, err = tx.CreateBucketIfNotExists(infoBucket); err != nil {
			return
		}
		if t == true {
			err = info.Put(infoKey, []byte{0xff})
		} else {
			err = info.Put(infoKey, []byte{0x00})
		}
		return
	})
	return
}

//
// Feeds
//

// AddFeed. Adding a feed twice or more times
// does nothing.
func (b *Bolt) AddFeed(pk cipher.PubKey) (err error) {
	err = b.b.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(pk[:])
	})
	return
}

// DelFeed with all heads and Root objects
// unconditionally. If feed doesn't exist
// then the Del returns ErrNoSuchFeed.
func (b *Bolt) DelFeed(pk cipher.PubKey) (err error) {
	err = b.b.Update(func(tx *bolt.Tx) (err error) {
		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}
		return tx.DeleteBucket(pk[:])
	})
	return
}

// IterateFeeds all feeds. Use ErrStopIteration to
// stop iteration. The Iterate passes any error
// returned from given function through. Except
// ErrStopIteration that turns nil. It's possible
// to mutate the IdxDB inside the Iterate
func (b *Bolt) IterateFeeds(iterateFunc IterateFeedsFunc) (err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

// HasFeed returns true if the IdxDB contains
// feed with given public key
func (b *Bolt) HasFeed(pk cipher.PubKey) (ok bool, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

// FeedsLen is number of feeds in DB
func (b *Bolt) FeedsLen() (length int, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

//
// Heads
//

// AddHead new head with given nonce.
// If a head with given nonce already
// exists, then this method does nothing.
func (b *Bolt) AddHead(pk cipher.PubKey, nonce uint64) {
	err = b.b.Update(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

// DelHead deletes head with given nonce and
// all its Root objects. The method returns
// ErrNoSuchHead if a head with given nonce
// doesn't exist.
func (b *Bolt) DelHead(pk cipher.PubKey, nonce uint64) (err error) {
	err = b.b.Update(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

// Has returns true if a head with given
// nonce exits in the DB
func (b *Bolt) HasHead(pk cipher.PubKey, nonce uint64) (ok bool, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

// Iterate over all heads
func (b *Bolt) IterateHeads(pk cipher.PubKey, iterateFunc IterateHeadsFunc) (err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

// HeadsLen is number of heads stored
func (b *Bolt) HeadsLen(pk cipher.PubKey) (length int, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		//
		return
	})
	return
}

//
// Roots
//

// AscendRoots iterates all Root object ascending order.
// Use ErrStopIteration to stop iteration. Any error
// (except the ErrStopIteration) returned by given
// IterateRootsFunc will be passed through. The
// AscendRoots doesn't update access time of a Root.
// See also IterateRootsFunc docs.
func (b *Bolt) AscendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc IterateRootsFunc,
) (err error) {
	//
}

// DescendRoots is the same as the Ascend, but it iterates
// decending order. Use ErrStopIteration to stop
// iteration. The DescendRoots doesn't update access time.
// See also IterateRootsFunc docs.
func (b *Bolt) DescendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc IterateRootsFunc,
) (err error) {
	//
}

// Has the Roots Root with given seq?
func (b *Bolt) HasRoot(pk cipher.PubKey, nonce uint64, seq uint64) (ok bool, err error) {
	//
}

// Len is number of Root objects stored
func (b *Bolt) RootsLen(pk cipher.PubKey, nonce uint64) (length int, err error) {
	//
}

// SetRoot add or touch Root if exists
func (b *Bolt) SetRoot(
	pk cipher.PubKey,
	nonce uint64,
	seq uint64,
	hash cipher.SHA256,
	sig cipher.Sig,
) (root *Root, err error) {
	//
}

// SetNotTouch add Root or do nothing if exists
func (b *Bolt) SetNotTouchRoot(
	pk cipher.PubKey,
	nonce uint64,
	seq uint64,
	hash cipher.SHA256,
	sig cipher.Sig,
) (root *Root, err error) {
	//
}

// GetRoot returns root and touches stored.
func (b *Bolt) GetRoot(pk cipher.PubKey, nonce uint64, seq uint64) (root *Root, err error) {
	//
}

// GetNotTouchRoot returns root.
func (b *Bolt) GetNotTouchRoot(
	pk cipher.PubKey, nonce uint32, seq uint64,
) (root *Root, err error) {
	//
}

// DelRoot deletes Root.
func (b *Bolt) DelRoot(pk cipher.PubKey, nonce uint32, seq uint64) (err error) {
	//
}

// Close IdxDB
func (b *Bolt) Close() error {
	//
}
