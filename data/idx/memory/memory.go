package memory

import (
	"sync"
	"time"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/skycoin/src/cipher"
)

const ScanBy = 100

type heads map[uint64]*tree

func copyRoot(r *data.Root) (root *data.Root) {
	root = new(data.Root)
	root.Hash = r.Hash
	root.Sig = r.Sig
	root.Access = r.Access
	root.Create = r.Create
	return
}

// A Memory implements data.IdxDB
// interface based on golang maps.
type Memory struct {
	sync.Mutex
	// feeds (pk) -> heads (nonce) -> roots (seq)
	feeds  map[cipher.PubKey]heads
	scanBy int
}

// NewMemory creates new Memory
func NewMemory(scanBy int) (m *Memory) {
	m = new(Memory)
	m.feeds = make(map[cipher.PubKey]heads)
	if scanBy <= 0 {
		m.scanBy = ScanBy
	} else {
		m.scanBy = scanBy
	}
	return
}

// AddFeed. Adding a feed twice or more times
// does nothing.
func (m *Memory) AddFeed(pk cipher.PubKey) (_ error) {
	m.Lock()
	defer m.Unlock()

	var _, ok = m.feeds[pk]
	if ok == false {
		m.feeds[pk] = make(heads)
	}
	return
}

// DelFeed with all heads and Root objects
// unconditionally. If feed doesn't exist
// then the Del returns ErrNoSuchFeed.
func (m *Memory) DelFeed(pk cipher.PubKey) (err error) {
	m.Lock()
	defer m.Unlock()

	var _, ok = m.feeds[pk]
	if ok == false {
		return data.ErrNoSuchFeed
	}
	delete(m.feeds, pk)
	return
}

func (m *Memory) unlockedIterateFeeds(
	pk cipher.PubKey, iterateFunc data.IterateFeedsFunc,
) error {

	m.Unlock()
	defer m.Lock()

	return iterateFunc(pk)
}

// Iterate all feeds. Use ErrStopIteration to
// stop iteration. The Iterate passes any error
// returned from given function through. Except
// ErrStopIteration that turns nil. It's possible
// to mutate the IdxDB inside the Iterate
func (m *Memory) IterateFeeds(iterateFunc data.IterateFeedsFunc) (err error) {
	m.Lock()
	defer m.Unlock()

	for pk := range m.feeds {
		if err = m.unlockedIterateFeeds(pk, iterateFunc); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}
	}
	return
}

// HasFeed returns true if the IdxDB contains
// feed with given public key
func (m *Memory) HasFeed(pk cipher.PubKey) (ok bool, _ error) {
	m.Lock()
	defer m.Unlock()

	_, ok = m.feeds[pk]
	return
}

// FeedsLen is number of feeds in DB
func (m *Memory) FeedsLen() (length int, _ error) {
	m.Lock()
	defer m.Unlock()

	length = len(m.feeds)
	return
}

// AddHead new head with given nonce.
// If a head with given nonce already
// exists, then this method does nothing.
func (m *Memory) AddHead(pk cipher.PubKey, nonce uint64) (err error) {
	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return data.ErrNoSuchFeed
	}

	if _, ok = hs[nonce]; ok == false {
		hs[nonce] = newTree()
	}
	return
}

// DelHead deletes head with given nonce and
// all its Root objects. The method returns
// ErrNoSuchHead if a head with given nonce
// doesn't exist.
func (m *Memory) DelHead(pk cipher.PubKey, nonce uint64) (err error) {
	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return data.ErrNoSuchFeed
	}

	if _, ok = hs[nonce]; ok == false {
		return data.ErrNoSuchHead
	}
	delete(hs, nonce)
	return
}

// Has returns true if a head with given
// nonce exits in the DB
func (m *Memory) HasHead(pk cipher.PubKey, nonce uint64) (ok bool, err error) {
	m.Lock()
	defer m.Unlock()

	var hs heads
	if hs, ok = m.feeds[pk]; ok == false {
		return false, data.ErrNoSuchFeed
	}

	_, ok = hs[nonce]
	return
}

func (m *Memory) unlockedIterateHeads(
	nonce uint64, iterateFunc data.IterateHeadsFunc,
) error {

	m.Unlock()
	defer m.Lock()

	return iterateFunc(nonce)
}

// Iterate over all heads
func (m *Memory) IterateHeads(
	pk cipher.PubKey, iterateFunc data.IterateHeadsFunc,
) (err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return data.ErrNoSuchFeed
	}

	for nonce := range hs {
		if err = m.unlockedIterateHeads(nonce, iterateFunc); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}
	}
	return
}

// HeadsLen is number of heads stored
func (m *Memory) HeadsLen(pk cipher.PubKey) (length int, err error) {
	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return 0, data.ErrNoSuchFeed
	}
	length = len(hs)
	return
}

func (m *Memory) unlockedIterateRoots(
	seq uint64,
	iterateFunc data.IterateRootsFunc,
) error {

	m.Unlock()
	defer m.Lock()

	return iterateFunc(seq)
}

// AscendRoots iterates all Root object ascending order.
// Use ErrStopIteration to stop iteration. Any error
// (except the ErrStopIteration) returned by given
// IterateRootsFunc will be passed through. The
// AscendRoots doesn't update access time of a Root.
// See also IterateRootsFunc docs.
func (m *Memory) AscendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc data.IterateRootsFunc,
) (err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return data.ErrNoSuchHead
	}
	var (
		from uint64   = 0
		scan []uint64 = make([]uint64, 0, m.scanBy)
	)
	for {
		var i int
		err = rs.ascend(from, func(key uint64, _ *data.Root) (err error) {
			scan = append(scan, key)
			if i++; i == cap(scan) {
				return errStop
			}
			return
		})
		if err != nil {
			return
		}
		for _, seq := range scan {
			if err = m.unlockedIterateRoots(seq, iterateFunc); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return
			}
			from = seq
		}
		scan = scan[:0]
		if from != ^uint64(0) {
			from++
		}
		if len(scan) < cap(scan) {
			return
		}
	}
	return
}

// DescendRoots is the same as the Ascend, but it iterates
// decending order. Use ErrStopIteration to stop
// iteration. The DescendRoots doesn't update access time.
// See also IterateRootsFunc docs.
func (m *Memory) DescendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc data.IterateRootsFunc,
) (err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return data.ErrNoSuchHead
	}
	var (
		from          = ^uint64(0)
		scan []uint64 = make([]uint64, 0, m.scanBy)
	)
	for {
		var i int
		err = rs.descend(from, func(key uint64, _ *data.Root) (err error) {
			scan = append(scan, key)
			if i++; i == cap(scan) {
				return errStop
			}
			return
		})
		if err == errStop {
			err = nil
		}
		if err != nil {
			return
		}
		for _, seq := range scan {
			if err = m.unlockedIterateRoots(seq, iterateFunc); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return
			}
			from = seq
		}
		scan = scan[:0]
		if from != 0 {
			from-- // prev
		}
		if len(scan) < cap(scan) {
			return // there are not more elements
		}
	}
	return
}

// HasRoot returns true if Root with given seq exists. The HasRoot
// never updates access time.
func (m *Memory) HasRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (ok bool, err error) {

	m.Lock()
	defer m.Unlock()

	var hs heads
	if hs, ok = m.feeds[pk]; ok == false {
		return false, data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return false, data.ErrNoSuchHead
	}
	ok = rs.exist(seq)
	return
}

// Len is number of Root objects stored
func (m *Memory) RootsLen(
	pk cipher.PubKey, nonce uint64,
) (length int, err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return 0, data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return 0, data.ErrNoSuchHead
	}
	length = rs.length
	return
}

// SetRoot add or touch Root if exists
func (m *Memory) SetRoot(
	pk cipher.PubKey, //   : feed
	nonce uint64, //       : head
	seq uint64, //         : seq of Root
	hash cipher.SHA256, // : hash or Root
	sig cipher.Sig, //     : signature of Root
) (root *data.Root, err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return nil, data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return nil, data.ErrNoSuchHead
	}
	var (
		access time.Time
		now    = time.Now()
	)
	if root = rs.get(seq); root != nil {
		access = root.Access // previous access time

		root.Hash = hash
		root.Sig = sig
		root.Access = now // touch

		root = copyRoot(root)
		root.Access = access
	} else {
		access = time.Unix(0, 0) // never been

		root = new(data.Root)
		root.Hash = hash
		root.Sig = sig
		root.Access = now // touch
		root.Create = now // created

		rs.set(seq, copyRoot(root))
		root.Access = access
	}
	return
}

// SetNotTouch add Root or do nothing if exists
func (m *Memory) SetNotTouchRoot(
	pk cipher.PubKey, //   : feed
	nonce uint64, //       : head
	seq uint64, //         : seq of Root
	hash cipher.SHA256, // : hash of Root
	sig cipher.Sig, //     : signature of Root
) (root *data.Root, err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return nil, data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return nil, data.ErrNoSuchHead
	}
	if root = rs.get(seq); root != nil {
		root.Hash = hash
		root.Sig = sig
		root = copyRoot(root)
	} else {
		root = new(data.Root)
		root.Hash = hash
		root.Sig = sig
		root.Access = time.Unix(0, 0) // never been
		root.Create = time.Now()      // created
		rs.set(seq, copyRoot(root))
	}
	return
}

// GetRoot returns root and touches stored.
func (m *Memory) GetRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return nil, data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return nil, data.ErrNoSuchHead
	}
	var access time.Time
	if root = rs.get(seq); root == nil {
		err = data.ErrNotFound
		return
	}
	access = root.Access     // previous access time
	root.Access = time.Now() // touch
	root = copyRoot(root)
	root.Access = access
	return
}

// GetNotTouchRoot returns root.
func (m *Memory) GetNotTouchRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return nil, data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return nil, data.ErrNoSuchHead
	}
	if root = rs.get(seq); root == nil {
		err = data.ErrNotFound
		return
	}
	root = copyRoot(root)
	return
}

// DelRoot deletes Root.
func (m *Memory) DelRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (err error) {

	m.Lock()
	defer m.Unlock()

	var hs, ok = m.feeds[pk]
	if ok == false {
		return data.ErrNoSuchFeed
	}
	var rs *tree
	if rs, ok = hs[nonce]; ok == false {
		return data.ErrNoSuchHead
	}
	if root := rs.get(seq); root == nil {
		return data.ErrNotFound
	}
	rs.del(seq)
	return
}

// IsSafeClosed retursn true if last closing was successful,
// and no data lost. New DB returns true too, even if it never
// been closed before.
func (m *Memory) IsSafeClosed() bool { return true }

// Close IdxDB
func (m *Memory) Close() error {
	m.feeds = nil
	return nil
}
