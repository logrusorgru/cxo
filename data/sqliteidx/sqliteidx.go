package sqliteidx

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"database/sql"
	"github.com/mattn/go-sqlite3" // SQLite 3 driver

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

func init() {
	const enableForeignKeys = `PRAGMA foreign_keys = ON;`
	sql.Register("sqlite3_with_foreign_keys",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) (err error) {
				logSQL(enableForeignKeys)
				_, err = conn.Exec(enableForeignKeys, nil)
				return
			},
		})
}

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
	if sq, err = sql.Open("sqlite3_with_foreign_keys", dbPath); err != nil {
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

	logSQL("BEGIN;")
	if tx, err = d.db.Begin(); err != nil {
		return
	}

	if err = txFunc(&feeds{tx}); err != nil {
		logSQL("ROLLBACK;")
		tx.Rollback()
		return
	}

	logSQL("COMMIT;")
	return tx.Commit()
}

// Close database
func (d *db) Close() error {
	return d.db.Close()
}

type feeds struct {
	tx *sql.Tx
}

func (f *feeds) Add(pk cipher.PubKey) (hs data.Heads, err error) {

	const sel = `SELECT id
    FROM feed
    WHERE pubkey = ?;`

	const insert = `INSERT INTO feed (
      pubkey,
      updated_at,
      created_at
    ) VALUES (
      ?,
      ?,
      ?
    );`

	var (
		feedID int64
		pkHex  = pk.Hex()
	)

	logSQL(sel, pkHex)
	if err = f.tx.QueryRow(sel, pkHex).Scan(&feedID); err != nil {

		if err == sql.ErrNoRows {

			var (
				result sql.Result
				now    = time.Now()
			)

			logSQL(insert, pk.Hex(), now, now)
			if result, err = f.tx.Exec(insert, pkHex, now, now); err != nil {
				return
			}
			if feedID, err = result.LastInsertId(); err != nil {
				return
			}
			hs = &heads{feedID: feedID, tx: f.tx}
			return
		}

		return
	}

	hs = &heads{feedID: feedID, tx: f.tx}
	return
}

func (f *feeds) Del(pk cipher.PubKey) (err error) {

	const del = `DELETE FROM feed
    WHERE pubkey = ?;`

	var result sql.Result
	logSQL(del, pk.Hex())
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
		return
	}
	pk = cipher.NewPubKey(b)
	return
}

func (f *feeds) Iterate(iterateFunc data.IterateFeedsFunc) (err error) {

	const sel = `SELECT pubkey
    FROM feed;`

	var (
		pk  cipher.PubKey
		pks string

		rows *sql.Rows
	)

	logSQL(sel)
	if rows, err = f.tx.Query(sel); err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() == true {
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

	const sel = `SELECT COUNT(1)
    FROM feed
    WHERE pubkey = ?;`

	logSQL(sel)
	err = f.tx.QueryRow(sel, pk.Hex()).Scan(&has)
	return
}

func (f *feeds) Heads(pk cipher.PubKey) (hs data.Heads, err error) {

	const sel = `SELECT id
    FROM feed
    WHERE pubkey = ?;`

	var feedID int64

	logSQL(sel, pk.Hex())
	if err = f.tx.QueryRow(sel, pk.Hex()).Scan(&feedID); err != nil {
		if err == sql.ErrNoRows {
			err = data.ErrNoSuchFeed
		}
		return
	}

	hs = &heads{feedID: feedID, tx: f.tx}
	return
}

func (f *feeds) Len() (length int, err error) {

	const sel = `SELECT COUNT(*)
    FROM feed;`

	logSQL(sel)
	err = f.tx.QueryRow(sel).Scan(&length)
	return
}

type heads struct {
	feedID int64 // fead of the heads
	tx     *sql.Tx
}

func (h *heads) Roots(nonce uint64) (rs data.Roots, err error) {

	const sel = `SELECT id FROM head
    WHERE feed_id = ?
    AND nonce = ?;`

	var (
		row    *sql.Row
		headID int64
	)

	logSQL(sel, h.feedID, nonce)
	row = h.tx.QueryRow(sel, h.feedID, nonce)

	if err = row.Scan(&headID); err != nil {
		if err == sql.ErrNoRows {
			err = data.ErrNoSuchHead
		}
		return
	}

	rs = &roots{headID: headID, tx: h.tx}
	return
}

func (h *heads) Add(nonce uint64) (rs data.Roots, err error) {

	const sel = `SELECT id
    FROM head
    WHERE feed_id = ?
    AND nonce = ?;`

	const insert = `INSERT INTO head (
      nonce,
      feed_id,
      updated_at,
      created_at
    ) VALUES (
      ?,
      ?,
      ?,
      ?);`

	var (
		result sql.Result
		headID int64

		now = time.Now()
	)

	logSQL(sel, h.feedID, nonce)
	err = h.tx.QueryRow(sel, h.feedID, nonce).Scan(&headID)

	if err != nil {

		if err == sql.ErrNoRows {
			logSQL(insert, nonce, h.feedID, now, now)
			result, err = h.tx.Exec(insert, nonce, h.feedID, now, now)
			if err != nil {
				return
			}
			if headID, err = result.LastInsertId(); err != nil {
				return
			}
			logSQL("after INSERT head_id is:", headID)
			rs = &roots{headID: headID, tx: h.tx}
		}

		return
	}

	rs = &roots{headID: headID, tx: h.tx}
	return
}

func (h *heads) Del(nonce uint64) (err error) {

	const del = `DELETE FROM head
    WHERE feed_id = ?
    AND nonce = ?;`

	var result sql.Result
	logSQL(del, nonce, h.feedID)
	if result, err = h.tx.Exec(del, nonce, h.feedID); err != nil {
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

	const sel = `SELECT COUNT(1)
    FROM head
    WHERE feed_id = ?
    AND nonce = ?;`

	logSQL(sel, nonce, h.feedID)
	err = h.tx.QueryRow(sel, nonce, h.feedID).Scan(&ok)
	return
}

func (h *heads) Iterate(iterateFunc data.IterateHeadsFunc) (err error) {

	const sel = `SELECT nonce
    FROM head
    WHERE feed_id = ?;`

	var (
		nonce uint64
		rows  *sql.Rows
	)

	logSQL(sel, h.feedID)
	if rows, err = h.tx.Query(sel, h.feedID); err != nil {
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

	const sel = `SELECT COUNT(*)
    FROM head
    WHERE feed_id = ?;`

	logSQL(sel, h.feedID)
	err = h.tx.QueryRow(sel, h.feedID).Scan(&length)
	return
}

type roots struct {
	headID int64
	tx     *sql.Tx
}

func (r *roots) iterate(
	dir string,
	iterateFunc data.IterateRootsFunc,
) (
	err error,
) {

	const sel = `SELECT
      seq,
      access_time,
      timestamp,
      prev,
      hash,
      sig,
      created_at
    FROM root
    WHERE head_id = ?
    ORDER BY seq
    ` // + ASC or DESC + ;

	var rows *sql.Rows

	logSQL(sel+dir+";", r.headID)
	if rows, err = r.tx.Query(sel+dir+";", r.headID); err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {

		var (
			r  root
			dr *data.Root
		)

		if err = r.Scan(rows); err != nil {
			return
		}

		if dr, err = r.Root(); err != nil {
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

func (r *roots) insertRoot(dr *data.Root) (err error) {

	const insert = `INSERT INTO root (
      seq,
      head_id,
      access_time,
      timestamp,
      prev,
      hash,
      sig,
      created_at,
      updated_at
    ) VALUES (
      ?,
      ?,
      ?,
      ?,
      ?,
      ?,
      ?,
      ?,
      ?);`

	var now = time.Now()

	dr.Access = now.UnixNano()
	dr.Create = dr.Access

	logSQL(insert, dr.Seq, r.headID, now.UnixNano(), dr.Time, dr.Prev.Hex(),
		dr.Hash.Hex(), dr.Sig.Hex(), now, now)
	_, err = r.tx.Exec(insert,
		dr.Seq,         // seq            (uint64)
		r.headID,       // reference      (int64)
		now.UnixNano(), // aceess_time    (int64)
		dr.Time,        // root timestamp (int64)
		dr.Prev.Hex(),  // prev           (text)
		dr.Hash.Hex(),  // hash           (text)
		dr.Sig.Hex(),   // sig            (text)
		now,            // created_at     (time.Time)
		now,            // updated_at     (time.Time)
	)

	return
}

func (r *roots) updateRoot(rootID int64, dr *data.Root) (err error) {

	const update = `UPDATE root
    SET
      access_time = ?,
      updated_at = ?
    WHERE id = ?;`

	var now = time.Now()

	logSQL(update, now.UnixNano(), now, rootID)
	_, err = r.tx.Exec(update, now.UnixNano(), now, rootID)
	return
}

func (r *roots) Set(dr *data.Root) (err error) {

	if err = dr.Validate(); err != nil {
		return
	}

	const sel = `SELECT
      id,
      access_time,
      created_at
    FROM root
    WHERE head_id = ?
    AND seq = ?;`

	logSQL(sel, r.headID, dr.Seq)

	var (
		rootID     int64
		accessTime int64
		createTime time.Time

		row = r.tx.QueryRow(sel, r.headID, dr.Seq)
	)

	if err = row.Scan(&rootID, &accessTime, &createTime); err != nil {

		if err == sql.ErrNoRows {
			return r.insertRoot(dr)
		}

		return
	}

	dr.Access = accessTime
	dr.Create = createTime.UnixNano()

	return r.updateRoot(rootID, dr)
}

func (r *roots) Del(seq uint64) (err error) {

	const del = `DELETE FROM root
    WHERE seq = ?
    AND head_id = ?;`

	logSQL(del, seq, r.headID)
	_, err = r.tx.Exec(del, seq, r.headID)
	return
}

func (r *roots) Get(seq uint64) (dr *data.Root, err error) {

	const sel = `SELECT
      id,
      access_time,
      timestamp,
      prev,
      hash,
      sig,
      created_at
    FROM root
    WHERE head_id = ?
    AND seq = ?;`

	logSQL(sel, r.headID, seq)
	var (
		row = r.tx.QueryRow(sel, r.headID, seq)

		id         int64
		accessTime int64
		timestamp  int64
		prev       string
		hash       string
		sig        string
		created    time.Time
	)

	err = row.Scan(
		&id,
		&accessTime,
		&timestamp,
		&prev,
		&hash,
		&sig,
		&created,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			err = data.ErrNotFound
		}
		return
	}

	dr = new(data.Root)
	dr.Create = created.UnixNano()
	dr.Access = accessTime
	dr.Time = timestamp
	dr.Seq = seq

	// can panic (fuck it)
	if dr.Prev, err = cipher.SHA256FromHex(prev); err != nil {
		return
	}

	// can panic (fuck it)
	if dr.Hash, err = cipher.SHA256FromHex(hash); err != nil {
		return
	}

	// can panic (fuck it)
	if dr.Sig, err = cipher.SigFromHex(sig); err != nil {
		return
	}

	const update = `UPDATE root
    SET access_time = ?
    WHERE id = ?;`

	logSQL(update, time.Now().UnixNano(), id)
	_, err = r.tx.Exec(update, time.Now().UnixNano(), id)
	return
}

func (r *roots) Has(seq uint64) (ok bool, err error) {

	const sel = `SELECT COUNT(1)
    FROM root
    WHERE seq = ?
    AND head_id = ?;`

	logSQL(sel, seq, r.headID)
	err = r.tx.QueryRow(sel, seq, r.headID).Scan(&ok)
	return
}

func (r *roots) Len() (length int, err error) {

	const sel = `SELECT COUNT(*)
    FROM root
    WHERE head_id = ?;`

	logSQL(sel, r.headID)
	err = r.tx.QueryRow(sel, r.headID).Scan(&length)
	return
}
