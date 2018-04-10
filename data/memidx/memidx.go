package memidx

import (
	"sync"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

// in memory DB with ACID transactions
type db struct {
	mx sync.Mutex

	s map[cipher.PubKey]*heads // stable storage
	t map[cipher.PubKey]*heads // transaction
}

// NewIdxDB returns new data.IdxDB.
func NewIdxDB() (idx data.IdxDB) {
	var d db
	d.s = make(map[cipher.PubKey]*heads)
	return &d
}

// Tx starts transaction
func (d *db) Tx(txFunc func(feed data.Feeds) error) (err error) {
	d.mx.Lock()
	defer d.mx.Unlock()

	if err = txFunc(d.feeds()); err != nil {
		d.rollback()
	}

	d.commit()
	return
}

func (d *db) feeds() *feeds {
	t.t = make(map[cipher.PubKey]*heads)
	return &feeds{d}
}

func (d *db) rollback() {
	d.t = nil // clear transaction
}

func (d *db) commit() {

	// TODO (kostyarin): merge

	d.t = nil // clear transaction
}

// Close DB
func (d *db) Close() (_ error) {
	d.mx.Lock()
	defer d.mx.Unlock()

	d.s = nil // GC
	return
}

type feeds struct {
	d *db
}

func (f *feeds) Add(pk cipher.PubKey) (err error) {

	// stable storage
	if _, ok := f.d.s[pk]; ok == true {
		// transaction
		if hs, ok := f.d.t[pk]; ok == true {
			// if deleted in transaction
			if hs.del == true {
				f.d.t[pk] = &heads{} // add new (undelete)
			}
			// do nothing if alrady there is in transaction
			return
		} // transaction
		// don't add if there is in the stable storage already
	} // stable storage

	f.d.t[pk] = &heads{} // add new to transaction
	return
}

func (f *feeds) Del(pk cipher.PubKey) (err error) {

	// stable storage
	if _, ok := f.d.s[pk]; ok == true {
		// transaction
		if hs, ok := f.d.t[pk]; ok == true {
			// if alrady deleted in transaction
			if hs.del == true {
				return data.ErrNoSuchFeed
			}
			// if there is in transaction and not deleted
			hs.del = true
			// TODO (kostyarin): clear all other fields of the hs
			return
		}
		// not found in transaction
		f.d.t[pk] = &heads{del: true} // make deleted
		return
	}

	// not found in stable storage and in the transaction
	return data.ErrNoSuchFeed
}

func (f *feeds) Iterate(iterateFunc data.IterateFeedsFunc) (err error) {

	// range over stable storage first
	for pk := range f.d.s {
		// check out transaction
		if _, ok := f.d.t[pk]; ok == true {
			//
		}
		//
	}

	return
}

func (f *feeds) Has(pk cipher.PubKey) (has bool, _ error) {

	//

	return
}

func (f *feeds) Heads(pk cipher.PubKey) (heads data.Heads, err error) {

	//

	return
}

func (f *feeds) Len() (length int) {

	//

	return
}

type heads struct {
	del bool // transaction

	s map[uint64]*roots
}

func (h *heads) Roots(nonce uint64) (rs data.Roots, err error) {

	//

	return
}

func (h *heads) Add(nonce uint64) (rs data.Roots, err error) {

	//

	return
}

func (h *heads) Del(nonce uint64) (err error) {

	//

	return
}

func (h *heads) Has(nonce uint64) (ok bool, err error) {

	//

	return
}

func (h *heads) Iterate(iterateFunc data.IterateHeadsFunc) (err error) {

	//

	return
}

func (h *heads) Len() (length int) {

	//

	return
}

type roots struct {
	del bool // transaction

	s map[uint64]*root
}

type root struct {
	del bool

	dr data.Root
}

func (r *roots) Ascend(iterateFunc data.IterateRootsFunc) (err error) {

	//

	return
}

func (r *roots) Descend(iterateFunc data.IterateRootsFunc) (err error) {

	//

	return
}

func (r *roots) Set(dr *data.Root) (err error) {

	//

	return
}

func (r *roots) Del(seq uint64) (err error) {

	//

	return
}

func (r *roots) Get(seq uint64) (dr *data.Root, err error) {

	//

	return
}

func (r *roots) Has(seq uint64) (ok bool, err error) {

	//

	return
}

func (r *roots) Len() (length int) {

	//

	return
}
