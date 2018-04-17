package sqliteidx

import (
	"database/sql"
	"time"
)

type feed struct {
	ID int64

	PubKey string

	CreatedAt time.Time
	UpdatedAt time.Time
}

type head struct {
	ID int64

	Nonce  uint64
	FeedID int64

	CreatedAt time.Time
	UpdatedAt time.Time
}

type root struct {
	ID int64

	Seq        uint64
	HeadID     int64
	AccessTime time.Time
	Timestamp  time.Time
	Prev       string // can be null
	Hash       string
	Sig        string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func initializeDatabase(sq *sql.DB) (err error) {

	if err = sq.Ping(); err != nil {
		return
	}

	const enableForeignKeys = `PRAGMA foreign_keys = ON;`

	if _, err = sq.Exec(enableForeignKeys); err != nil {
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
        idx_head_nonce ON head (nonce);`

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

        access_time  DATETIME
                     NOT NULL,
        timestamp    UNSIGNED BIG INT
                     NOT NULL,
        prev         VARYING CHARACTER (64),
        hash         VARYING CHARACTER (64)
                     NOT NULL,
    	sig          VARYING CHARACTER (130)
    	             NOT NULL,


        created_at  DATETIME,
        updated_at  DATETIME,

        FOREIGN KEY (head_id) REFERENCES head (id) ON DELETE CASCADE

    );`

	const rootsSeqUniqueIndex = `CREATE UNIQUE INDEX
        idx_root_seq ON root (seq);`

	const rootsHeadIdIndex = `CREATE INDEX
        idx_root_head_id ON head (id);`

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

		if _, err = sql.Exec(create); err != nil {
			return
		}

		for _, idx := range indices {
			if _, err = sql.Exec(idx); err != nil {
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

	err = sq.QueryRow(sel, name).Scan(&exist)
	return
}
