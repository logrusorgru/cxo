package rediscxds

import (
	"time"

	"github.com/mediocregopher/radix.v3"
	"github.com/skycoin/skycoin/src/cipher"
)

// defaults
const (
	Size int = 10 // default pool size (10 connections)
)

// ExpireFunc has meaning only if expire callback enabled
// by Config. There is a way to set some timeout for every
// value. This way, after last access to a value timeout
// starts and when it expires that callback called (see
// <redis.io> how Redis EXPIRE works to understand real
// behaviour of the EXPIRE). The timeout never removes
// some data from DB. The timeout can be used to turn
// the Redis (of this package) to be LRU cache to use
// the Redis wiht some other DB.
//
// So. If you want to keep lightweight and frequently
// used objects in Redis and other objects in another
// DB (like BoltDB, Badger or SQLite3), then you can wrap
// the Redis and the other DB with some kind of data.CXDS.
// And the wrapper will select DB by size. And the wrapper,
// using the expire callback can move stale objects from
// Redis to other DB.
//
// One more time. The ExpireFunc doesn't mean that obejct
// has been removed from Redis. E.g. it's designed to
// move object from Redis to other DB inside the ExpireFunc.
type ExpireFunc func(key cipher.SHA256)

// A Config represents Redis configurations
type Config struct {
	Size       int             // pool size (max connections)
	Opts       []radix.PoolOpt // pool options
	Expire     time.Duration   // enable expire callback (see ExpireFunc)
	ExpireFunc ExpireFunc      // expire callback (see ExpireFunc)
}

// NewConfig retursn new Config
// filled by defaults.
func NewConfig() (c *Config) {
	c = new(Config)
	c.Size = Size
	return
}
