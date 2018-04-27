package rediscxds

import (
	"fmt"
	"time"

	"github.com/mediocregopher/radix.v3"
	"github.com/skycoin/skycoin/src/cipher"
)

// defaults
const (
	Size        int           = 10               // default pool size
	MinExpire   time.Duration = 10 * time.Second // 10s
	DialTimeout time.Duration = 10 * time.Second // 10s
	ScanCount   int           = 100              // 100 per SCAN
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
	Size int             // pool size (max connections)
	Opts []radix.PoolOpt // pool options

	DialTimeout time.Duration // dial timeout
	Password    string        // redis server password

	// Expire enables expire callback (see ExpireFunc and
	// Config.ExpireFunc field). The Expire will be rounded
	// to nearest second below. If the Expire is zero or can
	// be rounded to zero, then no expire-feature enabled.
	// Also, the Expire can't be less then MinExpire.
	Expire     time.Duration
	ExpireFunc ExpireFunc // expire callback (see ExpireFunc)

	// ScanCount used inside Iterate method, see
	// <https://redis.io/commands/scan#the-count-option>
	// how it works
	ScanCount int
}

// NewConfig retursn new Config
// filled by defaults.
func NewConfig() (c *Config) {
	c = new(Config)
	c.Size = Size
	c.DialTimeout = DialTimeout
	c.ScanCount = ScanCount
	return
}

// Validate configurations. The Validate method adds
// connection function (to dial with timeout and
// to authenticate) to head of the Opts field.
// Validate set Expire to 0 if ExpireFunc is nil.
func (c *Config) Validate() (err error) {

	if c.DialTimeout < 0 {
		return fmt.Errorf("invalid DialTimeout: %s", c.DialTimeout)
	}

	if c.ScanCount <= 0 {
		return fmt.Errorf("invalid ScanCount: %d, want > 0",
			c.ScanCount)
	}

	if c.ExpireFunc == nil {
		c.Expire = 0
	}

	if c.Expire != 0 {
		c.Expire = (c.Expire / time.Second) * time.Second
		if c.Expire < MinExpire {
			return fmt.Errorf("Expire %s is less then min possible (%s)",
				c.Expire, MinExpire)
		}
	}

	if c.DialTimeout > 0 || c.Password != "" {

		var connFunc = func(network, addr string) (conn radix.Conn, err error) {

			if c.DialTimeout == 0 {
				conn, err = radix.Dial(network, addr)
			} else {
				conn, err = radix.DialTimeout(network, addr, c.DialTimeout)
			}

			if err != nil {
				return
			}

			if c.Password != "" {
				err = conn.Do(radix.Cmd(nil, "AUTH", c.Password))
				if err != nil {
					conn.Close()
					return nil, err // don't return closed connection (GC)
				}
			}

			return
		}

		// prepend
		c.Opts = append([]radix.PoolOpt{radix.PoolConnFunc(connFunc)},
			c.Opts...)
	}

	return
}
