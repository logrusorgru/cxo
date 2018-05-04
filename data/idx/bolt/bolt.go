package bolt

import (
	"bytes"
	"encoding/binary"
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

func addOne(b []byte) {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == 0xff {
			continue
		}
		b[i] += 1
		break
	}
	return
}

func utob(u uint64) []byte {
	var t [8]byte
	binary.BigEndian.PutUint64(t[:], u)
	return t[:]
}

func btou(b []byte) (u uint64) {
	u = binary.BigEndian.Uint64(b)
	return
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// A Bolt implements data.IdxDB interface.
// The Bolt based on <github.com/boltdb/bolt>.
type Bolt struct {
	b *bolt.DB

	scanBy       int
	isSafeClosed bool

	closeo sync.Once
}

// NewBolt creates new IdxDB or opens existing.
// The Bolt based on BoltDB <github.com/boltdb/bolt>.
func NewBolt(
	path string, //        :
	mode os.FileMode, //   :
	opts *bolt.Options, // :
	scanBy int, //         :
) (
	b *Bolt, //            :
	err error, //          :
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
		db.Close()
		return
	}

	if err = x.setSafeClosed(false); err != nil {
		db.Close()
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
		t = val[0] > 0
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
		return
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
func (b *Bolt) IterateFeeds(iterateFunc data.IterateFeedsFunc) (err error) {

	var (
		last cipher.PubKey // zeroes
		end  bool

		scan = make([]cipher.PubKey, 0, b.scanBy)
	)

	for end == false {
		b.b.View(func(tx *bolt.Tx) (err error) {
			var (
				c     = tx.Cursor()
				key   []byte
				limit = b.scanBy
			)
			for i := 0; i < limit; i++ {
				if i == 0 {
					key, _ = c.Seek(last[:])
				} else {
					key, _ = c.Next()
				}
				if key == nil {
					end = true
					return
				}
				if bytes.Compare(key, infoBucket) == 0 {
					limit++
					continue
				}
				copy(last[:], key)
				scan = append(scan, last)
			}
			addOne(last[:])
			return
		})

		for _, pk := range scan {
			if err = iterateFunc(pk); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return
			}
		}

		scan = scan[:0]
		// continue
	}
	return
}

// HasFeed returns true if the IdxDB contains
// feed with given public key
func (b *Bolt) HasFeed(pk cipher.PubKey) (ok bool, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		ok = tx.Bucket(pk[:]) != nil
		return
	})
	return
}

// FeedsLen is number of feeds in DB
func (b *Bolt) FeedsLen() (length int, err error) {
	err = b.b.View(func(tx *bolt.Tx) (_ error) {
		var c = tx.Cursor()
		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			if bytes.Compare(key, infoKey) == 0 {
				continue
			}
			length++
		}
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
func (b *Bolt) AddHead(pk cipher.PubKey, nonce uint64) (err error) {
	err = b.b.Update(func(tx *bolt.Tx) (err error) {
		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}
		_, err = feed.CreateBucketIfNotExists(utob(nonce))
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
		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}
		if err = feed.DeleteBucket(utob(nonce)); err == bolt.ErrBucketNotFound {
			err = data.ErrNoSuchHead
		}
		return
	})
	return
}

// HasHead returns true if a head with given
// nonce exits in the DB
func (b *Bolt) HasHead(pk cipher.PubKey, nonce uint64) (ok bool, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}
		ok = feed.Bucket(utob(nonce)) != nil
		return
	})
	return
}

// IterateHeads iterates over all heads
func (b *Bolt) IterateHeads(
	pk cipher.PubKey, //                  :
	iterateFunc data.IterateHeadsFunc, // :
) (err error) {

	var (
		last [8]byte // nonce
		end  bool

		scan = make([]uint64, 0, b.scanBy)
	)

	for end == false {
		err = b.b.View(func(tx *bolt.Tx) (err error) {
			var feed = tx.Bucket(pk[:])
			if feed == nil {
				return data.ErrNoSuchFeed
			}
			var (
				c   = feed.Cursor()
				key []byte
			)
			for i := 0; i < b.scanBy; i++ {
				if i == 0 {
					key, _ = c.Seek(last[:])
				} else {
					key, _ = c.Next()
				}
				if key == nil {
					end = true
					return
				}
				copy(last[:], key)
				scan = append(scan, btou(last[:]))
			}
			addOne(last[:])
			return
		})
		if err != nil {
			return
		}

		for _, nonce := range scan {
			if err = iterateFunc(nonce); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return
			}
		}

		scan = scan[:0]
		// continue
	}
	return
}

// HeadsLen is number of heads stored
func (b *Bolt) HeadsLen(pk cipher.PubKey) (length int, err error) {
	err = b.b.View(func(tx *bolt.Tx) (err error) {
		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}
		var c = feed.Cursor()
		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			length++
		}
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
	pk cipher.PubKey, nonce uint64, iterateFunc data.IterateRootsFunc,
) (err error) {

	var (
		last [8]byte // seq
		end  bool

		scan = make([]uint64, 0, b.scanBy)
	)

	for end == false {
		err = b.b.View(func(tx *bolt.Tx) (err error) {
			var feed = tx.Bucket(pk[:])
			if feed == nil {
				return data.ErrNoSuchFeed
			}
			var head = feed.Bucket(utob(nonce))
			if head == nil {
				return data.ErrNoSuchHead
			}
			var (
				c   = head.Cursor()
				key []byte
			)
			for i := 0; i < b.scanBy; i++ {
				if i == 0 {
					key, _ = c.Seek(last[:])
				} else {
					key, _ = c.Next()
				}
				if key == nil {
					end = true
					return
				}
				copy(last[:], key)
				scan = append(scan, btou(last[:]))
			}
			addOne(last[:])
			return
		})
		if err != nil {
			return
		}

		for _, seq := range scan {
			if err = iterateFunc(seq); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return
			}
		}

		scan = scan[:0]
		// continue
	}
	return
}

// DescendRoots is the same as the Ascend, but it iterates
// decending order. Use ErrStopIteration to stop
// iteration. The DescendRoots doesn't update access time.
// See also IterateRootsFunc docs.
func (b *Bolt) DescendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc data.IterateRootsFunc,
) (err error) {

	var (
		latest = [8]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		last   = latest // seq
		end    bool

		scan = make([]uint64, 0, b.scanBy)
	)

	for end == false {
		err = b.b.View(func(tx *bolt.Tx) (err error) {
			var feed = tx.Bucket(pk[:])
			if feed == nil {
				return data.ErrNoSuchFeed
			}
			var head = feed.Bucket(utob(nonce))
			if head == nil {
				return data.ErrNoSuchHead
			}
			var (
				c   = head.Cursor()
				key []byte
			)
			for i := 0; i < b.scanBy; i++ {
				if i == 0 {
					if last == latest {
						key, _ = c.Last() // last
					} else {
						key, _ = c.Seek(last[:]) // previous View pass
						key, _ = c.Prev()        // previous element
					}
				} else {
					key, _ = c.Prev()
				}
				if key == nil {
					end = true
					return
				}
				copy(last[:], key)
				scan = append(scan, btou(last[:]))
			}
			return
		})
		if err != nil {
			return
		}

		for _, seq := range scan {
			if err = iterateFunc(seq); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return
			}
		}

		scan = scan[:0]
		// continue
	}
	return
}

// Has the Roots Root with given seq?
func (b *Bolt) HasRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (ok bool, err error) {

	err = b.b.View(func(tx *bolt.Tx) (err error) {
		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}
		var head = feed.Bucket(utob(nonce))
		if head == nil {
			return data.ErrNoSuchHead
		}
		ok = head.Get(utob(seq)) != nil
		return
	})
	return
}

// Len is number of Root objects stored
func (b *Bolt) RootsLen(
	pk cipher.PubKey, nonce uint64,
) (length int, err error) {

	err = b.b.View(func(tx *bolt.Tx) (err error) {
		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}
		var head = feed.Bucket(utob(nonce))
		if head == nil {
			return data.ErrNoSuchHead
		}
		var c = head.Cursor()
		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			length++
		}
		return
	})
	return
}

// SetRoot add or touch Root if exists
func (b *Bolt) SetRoot(
	pk cipher.PubKey, //   : feed
	nonce uint64, //       : head
	seq uint64, //         : seq of Root
	hash cipher.SHA256, // : hash of Root
	sig cipher.Sig, //     : signarure of Root
) (root *data.Root, err error) {

	err = b.b.Update(func(tx *bolt.Tx) (err error) {

		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}

		var head = feed.Bucket(utob(nonce))
		if head == nil {
			return data.ErrNoSuchHead
		}

		root = new(data.Root)

		var (
			seqb   = utob(seq)      // encoded seq
			val    = head.Get(seqb) // get existsing
			now    = time.Now()     // now
			access time.Time        // previous access time
		)

		if val != nil {
			must(root.Decode(val)) //
			access = root.Access   // get previous
		} else {
			access = time.Unix(0, 0) // never been
			root.Create = now        //
		}

		root.Hash = hash
		root.Sig = sig
		root.Access = now // touch

		if err = head.Put(seqb, root.Encode()); err != nil {
			root = nil
			return
		}

		root.Access = access // previous (to return)
		return
	})
	return
}

// SetNotTouch add Root or do nothing if exists
func (b *Bolt) SetNotTouchRoot(
	pk cipher.PubKey, //   :
	nonce uint64, //       :
	seq uint64, //         :
	hash cipher.SHA256, // :
	sig cipher.Sig, //     :
) (root *data.Root, err error) {

	err = b.b.Update(func(tx *bolt.Tx) (err error) {

		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}

		var head = feed.Bucket(utob(nonce))
		if head == nil {
			return data.ErrNoSuchHead
		}

		root = new(data.Root)

		var (
			seqb = utob(seq)      // encoded seq
			val  = head.Get(seqb) // get existsing
		)

		if val != nil {
			must(root.Decode(val))
		} else {
			root.Access = time.Unix(0, 0) // never been
			root.Create = time.Now()      //
		}

		root.Hash = hash
		root.Sig = sig

		if err = head.Put(seqb, root.Encode()); err != nil {
			root = nil
		}
		return
	})
	return
}

// GetRoot returns root and touches stored.
func (b *Bolt) GetRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	err = b.b.Update(func(tx *bolt.Tx) (err error) {

		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}

		var head = feed.Bucket(utob(nonce))
		if head == nil {
			return data.ErrNoSuchHead
		}

		root = new(data.Root)

		var (
			seqb   = utob(seq)      // encoded seq
			val    = head.Get(seqb) // get existsing
			access time.Time        // previous access time
		)

		if val == nil {
			return data.ErrNotFound
		}

		must(root.Decode(val))   //
		access = root.Access     // get previous
		root.Access = time.Now() // touch

		if err = head.Put(seqb, root.Encode()); err != nil {
			root = nil
			return
		}

		root.Access = access // previous (to return)
		return
	})
	return
}

// GetNotTouchRoot returns root.
func (b *Bolt) GetNotTouchRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	err = b.b.View(func(tx *bolt.Tx) (err error) {

		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}

		var head = feed.Bucket(utob(nonce))
		if head == nil {
			return data.ErrNoSuchHead
		}

		if val := head.Get(utob(seq)); val != nil {
			root = new(data.Root)
			must(root.Decode(val))
			return
		}

		return data.ErrNotFound
	})
	return
}

// DelRoot deletes Root.
func (b *Bolt) DelRoot(pk cipher.PubKey, nonce uint64, seq uint64) error {

	return b.b.Update(func(tx *bolt.Tx) (err error) {

		var feed = tx.Bucket(pk[:])
		if feed == nil {
			return data.ErrNoSuchFeed
		}

		var head = feed.Bucket(utob(nonce))
		if head == nil {
			return data.ErrNoSuchHead
		}

		var (
			seqb = utob(seq)
			val  = head.Get(seqb)
		)
		if val == nil {
			return data.ErrNotFound
		}

		return head.Delete(seqb)
	})
}

// IsSafeClosed retursn true if last closing
// was successful, and no data lost.
func (b *Bolt) IsSafeClosed() bool { return b.isSafeClosed }

// Close IdxDB
func (b *Bolt) Close() (err error) {
	b.closeo.Do(func() {
		if err = b.setSafeClosed(true); err != nil {
			b.b.Close() // drop error
			return
		}
		err = b.b.Close()
	})
	return
}
