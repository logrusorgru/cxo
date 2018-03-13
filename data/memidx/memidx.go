package memidx

import (
	"sync"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

type IterateHeadsFunc func(nonce uint64) (err error)

type Heads interface {
	Roots(nonce uint64) (rs Roots, err error)
	Add(nonce uint64) (rs Roots, err error)
	Del(nonce uint64) (err error)
	Has(nonce uint64) (ok bool, err error)
	Iterate(iterateFunc IterateHeadsFunc) (err error)
	Len() (length int)
}

type IterateRootsFunc func(r *Root) (err error)

type Roots interface {
	Ascend(iterateFunc IterateRootsFunc) (err error)
	Descend(iterateFunc IterateRootsFunc) (err error)
	Set(r *Root) (err error)
	Del(seq uint64) (err error)
	Get(seq uint64) (r *Root, err error)
	Has(seq uint64) (ok bool, err error)
	Len() (length int)
}

type IdxDB interface {
	Tx(func(Feeds) error) error
	Close() error
}

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

	//

}

type Feeds interface {
	Del(pk cipher.PubKey) (err error)
	Iterate(iterateFunc IterateFeedsFunc) (err error)
	Has(pk cipher.PubKey) (ok bool, err error)
	Heads(pk cipher.PubKey) (hs Heads, err error)
	Len() (length int)
}

type feed struct {
	del bool // delete on merge (has meaning only in transaction)
}
