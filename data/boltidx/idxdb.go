package idxdb

import (
	"errors"
)

// DefaultDBFileName is default name of DB file.
// The name used by the node package and means:
// IdxDB, CXO API version 5, underying DB is
// BoltDB.
const DefaultDBFileName = "ixd5.bolt"

// common errors
var ErrInvalidSize = errors.New("invalid size of encoded object")
