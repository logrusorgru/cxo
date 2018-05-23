package redis

import (
	"fmt"
	"time"

	"github.com/mediocregopher/radix.v3"
)

// defaults
const (
	Size        int           = 10               // default pool size
	MinExpire   time.Duration = 10 * time.Second // 10s
	DialTimeout time.Duration = 10 * time.Second // 10s
	ScanCount   int           = 100              // 100 per SCAN
)

// A Config represents Redis configurations
type Config struct {
	Size int             // pool size (max connections)
	Opts []radix.PoolOpt // pool options

	DialTimeout time.Duration // dial timeout
	Password    string        // redis server password

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
