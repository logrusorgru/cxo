package memidx

import (
	"sync"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

// in memory DB with ACID transactions
type db struct {
	mx    sync.Mutex
	feeds *feeds
}

func NewIdxDB() (idx data.IdxDB) {
	var d db
	d.feeds = newFeeds()
	return d
}

func (d *db) Tx(txFunc func(feed data.Feeds) error) (err error) {
	d.mx.Lock()
	defer d.mx.Unlock()

	if err = txFunc(d.feeds.begin()); err != nil {
		d.feeds.rollback()
		return
	}

	d.feeds.commit()
	return
}

func (d *db) Close() (_ error) {
	d.mx.Lock()
	defer d.mx.Unlock()

	d.feeds = nil // detach (GC)
	return
}

type feeds struct {
	sync.Mutex
	m map[cipher.PubKey]*feed // stable state
	t map[cipher.PubKey]*feed // transaction
}

func newFeeds() (fs *feeds) {
	fs = new(feeds)
	fs.m = make(map[cipher.PubKey]*feeds)
	return
}

func (f *feeds) begin() *feeds {
	f.t = make(map[cipher.PubKey]*feeds)
	return f
}

func (f *feeds) commit() {
	// merge t and f
}

func (f *feeds) rollback() {
	f.t = nil // clear all changes
}

func (f *feeds) Add(pk cipher.PubKey) (_ error) {

	//	chek out the transaction
	var n, ok = f.t[pk]

	if ok == false { // not exist in the transaction

		// chek out stable storage
		if _, ok := f.f[pk]; ok {
			return // does nothing (alrady exist)
		}

		f.t[pk] = new(feed) // create

	} else if n.del == true { // and ok is true

		f.t[pk] = new(feed) // reborn

	}

	return // already exists
}

func (f *feeds) Del(pk cipher.PubKey) (_ error) {

	// check out transaction
	var n, ok = f.t[pk]

	if ok == false {

		// check out stable storage
		if _, ok := f.f[pk]; ok == false {
			return data.ErrNoSuchFeed // no such feed
		}

		// add "del" record to the transaction
		n = new(feed)
		n.del = true
		f.t[pk] = n
		return
	}

	if n.del == true {
		return data.ErrNoSuchFeed // already deleted in the transaction
	}

	// set 'del' flag to delete the feed on merge
	n.del = true
	return
}

func (f *feeds) Iterate(iterateFunc data.IterateFeedsFunc) (err error) {

	// range over stable storage
	for pk := range f.m {

		// skip feeds deleted in the transaction
		if n, ok := f.t[pk]; ok == false || n.del == false {

			if err = iterateFunc(pk); err != nil {

				// reset service error
				if err == data.ErrStopIteration {
					err = nil
				}

				return
			}

		}

		// skip deleted feeds
	}

	return
}

func (f *feeds) Has(pk cipher.PubKey) (has bool, _ error) {

	//	chek out the transaction
	var n, ok = f.t[pk]

	if ok == false { // not exist in the transaction

		// chek out stable storage
		_, has = f.f[pk]
		return // has or has not the feed

	} else if n.del == true { // and ok is true

		return // false (has deleted feed, e.g. has not the feed)

	}

	return true // has the feed
}

func (f *feeds) Heads(pk cipher.PubKey) (hs data.Heads, err error) {

	//

}

func (f *feeds) Len() (length int) {

	length = len(f.m) // length of stable storage

	for pk, n := range f.t {

		// subtract deleted feeds
		if _, ok := f.m[pk]; ok {
			if n.del == true {
				length--
			}
			continue
		}

		// add feeds of the transaction
		if n.del == false {
			length++
		}

	}

	return
}

type feed struct {
	del bool // delete on merge (has meaning only in transaction)

	// the t is nil for stable storage

	s *heads // link to head of stable storeage
	t *heads // heads of the transaction
}

type heads struct {
	del bool // delete on merge (has meaning only in transaction)

	s *roots // of stable storage
	t *roots // of the transaction
}

// type IterateHeadsFunc func(nonce uint64) (err error)

func (h *heads) Roots(nonce uint64) (rs data.Roots, err error) {
	//
}

func (h *heads) Add(nonce uint64) (rs data.Roots, err error) {
	//
}

func (h *heads) Del(nonce uint64) (err error) {
	//
}

func (h *heads) Has(nonce uint64) (ok bool, err error) {
	//
}

func (h *heads) Iterate(iterateFunc data.IterateHeadsFunc) (err error) {
	//
}

func (h *heads) Len() (length int) {
	//
}

type roots struct {
	del bool // delete on merge (has meaning only in transaction)

	s []*root // of stable
	t []*root // of transaction
}

// type IterateRootsFunc func(r *Root) (err error)

func (r *roots) Ascend(iterateFunc data.IterateRootsFunc) (err error) {
	//
}

func (r *roots) Descend(iterateFunc data.IterateRootsFunc) (err error) {
	//
}

func (r *roots) Set(dr *data.Root) (err error) {
	//
}

func (r *roots) Del(seq uint64) (err error) {
	//
}

func (r *roots) Get(seq uint64) (dr *data.Root, err error) {
	//
}

func (r *roots) Has(seq uint64) (ok bool, err error) {
	//
}

func (r *roots) Len() (length int) {
	//
}

type root struct {
	del bool
	r   *data.Root
}
