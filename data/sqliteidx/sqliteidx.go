package sqliteidx

import (
	"encoding/hex"
	"io"
	"os"
	"sync"

	"database/sql"
	_ "github.com/mattn/go-sqlite3" // SQLite 3 driver

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

// InMemory is dbPath for NewIdxDB to use
// database in memory
const InMemory string = "file::memory:?cache=shared"

type db struct {
	mx sync.Mutex // one transaction at the same time allowed only
	db *sql.DB
}

// NewIdxDB returns new data.IdxDB based on SQLite3 backend.
// It's possible to use in-memeory DB providing InMemory
// constant. NewIdxDB creates or opens existsing DB.
func NewIdxDB(dbPath string) (idx data.IdxDB, err error) {

	var sq *sql.DB

	// create or open SQLite3 DB
	if sq, err = sql.Open("sqlite3", dbPath); err != nil {
		return
	}

	// terminate SQLite3 engine on error
	defer func() {
		if err != nil {
			sq.Close() // ignore error
		}
	}()

	if err = initializeDatabase(sq); err != nil {
		return
	}

	idx = &db{db: sq}
	return
}

// Tx starts transaction
func (d *db) Tx(txFunc func(feed data.Feeds) error) (err error) {
	d.mx.Lock()
	defer d.mx.Unlock()

	var tx *sql.Tx

	if tx, err = d.db.Begin(); err != nil {
		return
	}

	if err = txFunc(&feeds{tx}); err != nil {
		tx.Rollback()
		return
	}

	return tx.Commit()
}

// Close database
func (d *db) Close() error {
	return d.sql.Close()
}

type feeds struct {
	tx *sql.Tx
}

func (f *feeds) Add(pk cipher.PubKey) (err error) {
	_, err = f.tx.Exec(`INSERT OR IGNORE INTO feed (pubkey) VALUES (?);`,
		pk.Hex())
	return
}

func (f *feeds) Del(pk cipher.PubKey) (err error) {
	_, err = f.tx.Exec(`DELETE FROM feed WHERE pubkey = ?;`, pk.Hex())
	return
}

func pubKeyFromHex(pks string) (pk cipher.PubKey, err error) {
	var b []byte
	if b, err = hex.DecodeString(pks); err != nil {
		return
	}
	if len(b) != len(cipher.PubKey{}) {
		err = errors.New("invalid PubKey length")
	}
	pk = cipher.NewPubKey(b)
	return
}

func (f *feeds) Iterate(iterateFunc data.IterateFeedsFunc) (err error) {

	var (
		pk  cipher.PubKey
		pks string

		rows *sql.Rows
	)

	if rows, err = f.tx.Query(`SELECT pubkey FROM feed;`); err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&pks); err != nil {
			return
		}
		if pk, err = pubKeyFromHex(pks); err != nil {
			return
		}
		if err = iterateFunc(pk); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}
	}

	err = rows.Err()
	return
}

func (f *feeds) Has(pk cipher.PubKey) (has bool, _ error) {
	err = f.tx.QueryRow(`SELECT COUNT(1) FROM feed WHERE pubkey = ?;`,
		pk.Hex()).Scan(&has)
	return
}

func (f *feeds) Heads(pk cipher.PubKey) (heads data.Heads, err error) {
	//
	return
}

func (f *feeds) Len() (length int) {
	f.tx.QueryRow(`SELECT COUNT(*) FROM feed;`, pk.Hex()).Scan(&length)
	return
}

type heads struct {
	id    int64
	nonce uint64

	tx *sql.Tx
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
	sess *xorm.Session
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
