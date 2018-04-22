package sqliteidx

import (
	"database/sql"
	"time"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/skycoin/src/cipher"

	"fmt" // tmeporary
)

// tempoary
func logSQL(query string, args ...interface{}) {
	if false {
		fmt.Println(append([]interface{}{("[SQL] " + query)}, args...)...)
	}
}

// --------

type root struct {
	ID int64

	Seq        uint64
	HeadID     int64
	AccessTime int64 // unix nano
	Timestamp  int64 // uinx nano
	Prev       string
	Hash       string
	Sig        string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Scan *sql.Rows those contains 'seq', 'access_time',
// 'timestamp', 'prev', 'hash', 'sig' and 'created_at'
// columns
func (r *root) Scan(rows *sql.Rows) (err error) {
	err = rows.Scan(
		&r.Seq,        // }
		&r.AccessTime, // }
		&r.Timestamp,  // } the data.Root fields
		&r.Prev,       // } --------------------
		&r.Hash,       // }
		&r.Sig,        // }
		&r.CreatedAt,  // }
	)
	return
}

// Root returns data.Root filled from the root
func (r *root) Root() (dr *data.Root, err error) {
	dr = new(data.Root)

	dr.Create = r.CreatedAt.UnixNano()
	dr.Access = r.AccessTime
	dr.Time = r.Timestamp
	dr.Seq = r.Seq

	if dr.Prev, err = cipher.SHA256FromHex(r.Prev); err != nil {
		return
	}

	if dr.Hash, err = cipher.SHA256FromHex(r.Hash); err != nil {
		return
	}

	dr.Sig, err = cipher.SigFromHex(r.Sig)
	return
}

func initializeDatabase(sq *sql.DB) (err error) {

	if err = sq.Ping(); err != nil {
		return
	}

	//
	// feed table
	//

	const feedsTable = `CREATE TABLE feed (

        id          INTEGER
                    PRIMARY KEY
                    AUTOINCREMENT
                    NOT NULL,

        pubkey      VARYING CHARACTER (66)
                    NOT NULL,

        created_at  DATETIME,
        updated_at  DATETIME

    );`

	const feedsPubKeyUniqueIndex = `CREATE UNIQUE INDEX
        idx_feed_pubkey ON feed (pubkey);`

	err = createTableIfNotExist(sq,
		"feed",
		feedsTable,
		feedsPubKeyUniqueIndex)
	if err != nil {
		return
	}

	//
	// head table
	//

	const headsTable = `CREATE TABLE head (

        id          INTEGER
                    PRIMARY KEY
                    AUTOINCREMENT
                    NOT NULL,

        nonce       UNSIGNED BIG INT
                    NOT NULL,
        feed_id     INTEGER
                    NOT NULL,

        created_at  DATETIME,
        updated_at  DATETIME,

        FOREIGN KEY (feed_id) REFERENCES feed (id) ON DELETE CASCADE

    );`

	const headsNonceUniqueIndex = `CREATE UNIQUE INDEX
        idx_head_nonce_feed_id ON head (nonce, feed_id);`

	const headsFeedIdIndex = `CREATE INDEX
        idx_head_feed_id ON head (feed_id);`

	err = createTableIfNotExist(sq,
		"head",
		headsTable,
		headsNonceUniqueIndex,
		headsFeedIdIndex)
	if err != nil {
		return
	}

	//
	// root table
	//

	const rootsTable = `CREATE TABLE root (

        id           INTEGER
                     PRIMARY KEY
                     AUTOINCREMENT
                     NOT NULL,

        seq          UNSIGNED BIG INT,
        head_id      INTEGER
                     NOT NULL,

        access_time  INTEGER
                     NOT NULL,
        timestamp    INTEGER
                     NOT NULL,
        prev         VARYING CHARACTER (64)
                     NOT NULL,
        hash         VARYING CHARACTER (64)
                     NOT NULL,
        sig          VARYING CHARACTER (130)
                     NOT NULL,


        created_at  DATETIME,
        updated_at  DATETIME,

        FOREIGN KEY (head_id) REFERENCES head (id) ON DELETE CASCADE

    );`

	const rootsSeqUniqueIndex = `CREATE UNIQUE INDEX
        idx_root_seq_head_id ON root (seq, head_id);`

	const rootsHeadIdIndex = `CREATE INDEX
        idx_root_head_id ON root (head_id);`

	err = createTableIfNotExist(sq,
		"root",
		rootsTable,
		rootsSeqUniqueIndex,
		rootsHeadIdIndex)
	return
}

func createTableIfNotExist(
	sq *sql.DB, //        :
	name string, //       :
	create string, //     :
	indices ...string, // :
) (
	err error, //         :
) {

	var exist bool

	if exist, err = isTableExist(sq, name); err != nil {
		return
	}

	if exist == false {

		//logSQL(create)
		if _, err = sq.Exec(create); err != nil {
			return
		}

		for _, idx := range indices {
			//logSQL(idx)
			if _, err = sq.Exec(idx); err != nil {
				return
			}
		}

	}

	return
}

func isTableExist(sq *sql.DB, name string) (exist bool, err error) {

	const sel = `SELECT COUNT(*) FROM sqlite_master
    WHERE type = 'table'
    AND name = ?;`

	//logSQL(sel, name)
	err = sq.QueryRow(sel, name).Scan(&exist)
	return
}
