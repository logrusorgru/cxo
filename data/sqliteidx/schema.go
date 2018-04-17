package sqliteidx

import (
	"time"

	"github.com/go-xorm/xorm"
)

type feed struct {
	ID int64 `xorm:"id"`

	PubKey string `xorm:"pubkey"`

	CreatedAt time.Time `xorm:"created 'created_at'"`
	UpdatedAt time.Time `xorm:"updated 'updated_at'"`
}

type head struct {
	ID int64 `xorm:"id"`

	Nonce  uint64 `xorm:"nonce"`
	FeedID int64  `xorm:"feed_id"`

	CreatedAt time.Time `xorm:"created 'created_at'"`
	UpdatedAt time.Time `xorm:"updated 'updated_at'"`
}

type root struct {
	ID int64 `xorm:"id"`

	Seq        uint64    `xorm:"seq"`
	HeadID     int64     `xorm:"head_id"`
	AccessTime time.Time `xorm:"access_time"`
	Timestamp  time.Time `xorm:"timestamp"`
	Prev       string    `xorm:"prev"`
	Hash       string    `xorm:"hash"`
	Sig        string    `xorm:"sig"`

	CreatedAt time.Time `xorm:"created 'created_at'"`
	UpdatedAt time.Time `xorm:"updated 'updated_at'"`
}

func initializeDatabase(sql *xorm.Engine) (err error) {

	if err = sql.Ping(); err != nil {
		return
	}

	const enableForeignKeys = `PRAGMA foreign_keys = ON;`

	if _, err = sql.Exec(enableForeignKeys); err != nil {
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

	err = createTableIfNotExist(sql,
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

	err = createTableIfNotExist(sql,
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

	err = createTableIfNotExist(sql,
		"root",
		rootsTable,
		rootsSeqUniqueIndex,
		rootsHeadIdIndex)
	return
}

func createTableIfNotExist(
	sql *xorm.Engine, //  :
	name string, //       :
	create string, //     :
	indices ...string, // :
) (
	err error, //         :
) {

	var exist bool

	if exist, err = sql.IsTableExist(name); err != nil {
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
