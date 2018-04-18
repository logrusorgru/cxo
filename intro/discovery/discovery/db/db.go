package db

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

// InMemory keeps data base path to use DB in-memory
const InMemory = "file::memory:?cache=shared"

type DB struct {
	db *sql.DB
}

var engine *xorm.Engine

// Init SQLite3 DB for discovery server. Provide
// db path o use InMemory constant
func Init(dbPath string) (db *DB, err error) {

	var sq *sql.DB

	if sq, err = sql.Open("sqlite3", dbPath); err != nil {
		return
	}

	sq.SetMaxIdleConns(30)
	sq.SetMaxOpenConns(30)

	// terminate the SQLite3 DB on error
	defer func() {
		if err != nil {
			sq.Close()
		}
	}()

	if err = createTables(sq); err != nil {
		return
	}

	db = new(DB)
	db.db = sq
	return
}

// Close DB
func (d *DB) Close() error {
	return d.db.Close()
}

func createTables(sq *sql.DB) (err error) {

	//
	// ping
	//

	if err = sq.Ping(); err != nil {
		return
	}

	//
	// foreign keys (disabled by default)
	//

	const enableForeignKeys = `PRAGMA foreign_keys = ON;`

	if _, err = engine.Exec(enableForeignKeys); err != nil {
		return
	}

	//
	// node table
	//

	const nodeTable = `CREATE TABLE node (
        id               INTEGER
                         PRIMARY KEY
                         AUTOINCREMENT
                         NOT NULL,

        [key]            CHAR (66),
        service_address  CHAR (50),
        location         CHAR (100),
        version          TEXT,
        priority         INTEGER,

        created          DATETIME,
        updated          DATETIME
    );`

	// 'key' is SQLite3 keyword
	const nodeIndex = `CREATE UNIQUE INDEX idx_node_key ON node ("key");`

	if err = createTable(sq, "node", nodeTable, nodeIndex); err != nil {
		return
	}

	//
	// service table
	//

	const serviceTable = `CREATE TABLE service (
        id                   INTEGER
                             PRIMARY KEY
                             AUTOINCREMENT
                             NOT NULL,

        [key]                CHAR (66),
        address              CHAR (50),
        hide_from_discovery  INTEGER,
        allow_nodes          TEXT,
        version              CHAR (10),

        created              DATETIME,
        updated              DATETIME,

        node_id              INTEGER,

        FOREIGN KEY (node_id) REFERENCES node (id) ON DELETE CASCADE
    );`

	const serviceIndex = `CREATE UNIQUE INDEX
        idx_service_key ON service ("key");` // 'key' is SQLite3 keyword

	const serviceNodeIdIndex = `CREATE INDEX
        idx_service_node_id ON service (node_id);`

	err = createTable(sq, "service", serviceTable,
		serviceIndex, serviceNodeIdIndex)

	if err != nil {
		return
	}

	//
	// attributes table
	//

	const attributesTable = `CREATE TABLE attributes (
        name        CHAR (20),
        service_id  INTEGER,

        FOREIGN KEY (service_id) REFERENCES  service (id) ON DELETE CASCADE
    );`

	const attributesNameIndex = `CREATE INDEX
        idx_attributes_name ON attributes (name);`

	const attributesServiceIdIndex = `CREATE INDEX
        idx_attributes_service_id ON attributes (service_id);`

	err = createTable(sq, "attributes",
		attributesTable,
		attributesNameIndex,
		attributesServiceIdIndex)

	return
}

func createTable(
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

		if _, err = sq.Exec(create); err != nil {
			return
		}

		for _, idx := range indices {
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

	err = sq.QueryRow(sel, name).Scan(&exist)
	return
}
