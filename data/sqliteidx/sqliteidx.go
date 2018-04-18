package sqliteidx

import (
	"encoding/hex"
	"io"
	"os"
	"sync"
	"time"

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

	const insert = `INSERT OR IGNORE INTO feed
    (pubkey, updated_at, created_at) VALUES (?, ?, ?);`

	var now = time.Now()
	_, err = f.tx.Exec(insert, pk.Hex(), now, now)
	return
}

func (f *feeds) Del(pk cipher.PubKey) (err error) {

	const del = `DELETE FROM feed WHERE pubkey = ?;`

	var result sql.Result
	if result, err = f.tx.Exec(del, pk.Hex()); err != nil {
		return
	}
	var affected int64
	if affected, err = result.RowsAffected(); err != nil {
		return
	}
	if affected == 0 {
		err = data.ErrNoSuchFeed
	}
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

func (f *feeds) Has(pk cipher.PubKey) (has bool, err error) {
	err = f.tx.QueryRow(`SELECT COUNT(1) FROM feed WHERE pubkey = ?;`,
		pk.Hex()).Scan(&has)
	return
}

func (f *feeds) Heads(pk cipher.PubKey) (hs data.Heads, err error) {

	var (
		row    *sql.Row
		feedID int64
	)

	row = f.tx.QueryRow(`SELECT id FROM feed WHERE pubkey = ?;`, pk.Hex())

	if err = row.Scan(&feedID); err != nil {
		if err == sql.ErrNoRows {
			err = data.ErrNoSuchFeed
		}
		return
	}

	hs = &heads{feedID: feedID, tx: f.tx}
	return
}

func (f *feeds) Len() (length int, err error) {
	err = f.tx.QueryRow(`SELECT COUNT(*) FROM feed;`, pk.Hex()).Scan(&length)
	return
}

type heads struct {
	feedID int64 // fead of the heads
	tx     *sql.Tx
}

func (h *heads) Roots(nonce uint64) (rs data.Roots, err error) {

	var (
		row    *sql.Row
		headID int64
	)

	row = f.tx.QueryRow(`SELECT id FROM head WHERE nonce = ?;`, nonce)

	if err = row.Scan(&headID); err != nil {
		if err == sql.ErrNoRows {
			err = data.ErrNoSuchHead
		}
		return
	}

	rs = &roots{headID: headID, tx: f.tx}
	return
}

func (h *heads) Add(nonce uint64) (rs data.Roots, err error) {

	const insert = `INSERT OR IGNORE INTO head
    (nonce, feed_id, updated_at, created_at) VALUES (?, ?, ?, ?);`

	var now = time.Now()
	_, err = f.tx.Exec(insert, nonce, h.feedID, now, now)
	return

	return
}

func (h *heads) Del(nonce uint64) (err error) {

	const del = `DELETE FROM head WHERE nonce = ? AND feed_id = ?;`

	var result sql.Result
	if result, err = f.tx.Exec(del, nonce, h.feedID); err != nil {
		return
	}
	var affected int64
	if affected, err = result.RowsAffected(); err != nil {
		return
	}
	if affected == 0 {
		err = data.ErrNoSuchHead
	}
	return
}

func (h *heads) Has(nonce uint64) (ok bool, err error) {

	const sel = `SELECT COUNT(1) FROM head
        WHERE nonce = ?
        AND feed_id = ?;`

	err = f.tx.QueryRow(sel, nonce, h.feedID).Scan(&ok)
	return
}

func (h *heads) Iterate(iterateFunc data.IterateHeadsFunc) (err error) {

	const sel = `SELECT nonce FROM head WHERE feed_id = ?;`

	var (
		nonce uint64
		rows  *sql.Rows
	)

	if rows, err = f.tx.Query(sel, h.feedID); err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&nonce); err != nil {
			return
		}
		if err = iterateFunc(nonce); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}
	}

	err = rows.Err()
	return
}

func (h *heads) Len() (length int, err error) {
	const sel = `SELECT COUNT(*) FROM head WHERE feed_id = ?;`
	err = h.tx.QueryRow(sel, h.feedID).Scan(&length)
	return
}

type roots struct {
	headID int64
	tx     *sql.Tx
}

func scanRoot(rows *sql.Rows) (dr *data.Root, err error) {
	var rt root

	err = rows.Scan(
		&rt.Seq,
		&rt.HeadID,
		&rt.AccessTime,
		&rt.Timestamp,
		&rt.Prev,
		&rt.Hash,
		&rt.Sig,
		&rt.CreatedAt,
	)

	if err != nil {
		return
	}

	dr = new(data.Root)

	dr.Create = rt.CreatedAt.UnixNano()
	dr.Access = rt.AccessTime.UnixNano()
	dr.Time = rt.Timestamp.UnixNano()
	dr.Seq = rt.Seq

	if rt.Prev.Valid == true {
		if dr.Prev, err = cipher.SHA256FromHex(rt.Prev.String); err != nil {
			return
		}
	}

	if dr.Hash, err = cipher.SHA256FromHex(rt.Hash); err != nil {
		return
	}

	dr.Sig, err = cipher.SigFromHex(rt.Sig)
	return
}

func (r *roots) iterate(
	dir string,
	iterateFunc data.IterateRootsFunc,
) (
	err error,
) {

	const sel = `SELECT (
		seq,
		head_id,
		access_time,
		timestamp,
		prev,
		hash,
		sig,
		created_at
	)
	FROM root
	WHERE head_id = ?` // + ASC or DESC;

	var (
		nonce uint64
		rows  *sql.Rows
	)

	if rows, err = f.tx.Query(sel+" "+dir+";", r.headID); err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {

		var dr *data.Root
		if dr, err = scanRoot(rows); err != nil {
			return
		}

		if err = iterateFunc(dr); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}

	}

	err = rows.Err()
	return
}

func (r *roots) Ascend(iterateFunc data.IterateRootsFunc) error {
	return r.iterate("ASC", iterateFunc)
}

func (r *roots) Descend(iterateFunc data.IterateRootsFunc) error {
	return r.iterate("DESC", iterateFunc)
}

func (r *roots) Set(dr *data.Root) (err error) {
	//
	return
}

func (r *roots) Del(seq uint64) (err error) {
	const del = `DELETE FROM root WHERE seq = ? AND head_id = ?;`
	_, err = f.tx.Exec(del, seq, r.headID)
	return
}

func (r *roots) Get(seq uint64) (dr *data.Root, err error) {
	//
	return
}

func (r *roots) Has(seq uint64) (ok bool, err error) {

	const sel = `SELECT COUNT(1) FROM root
        WHERE seq = ?
        AND head_id = ?;`

	err = f.tx.QueryRow(sel, seq, r.headID).Scan(&ok)
	return
}

func (r *roots) Len() (length int, err error) {
	const sel = `SELECT COUNT(*) FROM root WHERE head_id = ?;`
	err = h.tx.QueryRow(sel, r.headID).Scan(&length)
	return
}
