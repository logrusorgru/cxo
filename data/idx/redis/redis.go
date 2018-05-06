package redis

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/skycoin/src/cipher"

	"github.com/mediocregopher/radix.v3"
)

type stat struct {
	all  int64
	used int64
}

// A Redis implments data.IdxDB
// interface over Redis <redis.io>.
type Redis struct {
	pool         *radix.Pool // conenctions pool
	isSafeClosed bool        // current state

	// LRU timeout feature
	expire     int64      // a'la time.Duration in seconds
	expireFunc ExpireFunc //

	// scanning (iterate)
	scanCount int

	// scripts (SHA1)
	touchLua                                               string
	getLua, getIncrLua, getNotTouchLua, getIncrNotTouchLua string
	setIncrLua, setIncrNotTouchLua, setRawLua              string
	incrLua, incrNotTouchLua                               string
	delLua, takeLua                                        string

	closeo sync.Once
}

// NewRedis creates data.CXDS based on Redis <redis.io>
func NewRedis(
	network string, // : "tcp", "tcp4" or "tcp6"
	addr string, //    : address of Redis server
	conf *Config, //   : configurations
) (
	r *Redis, //       : implements data.CXDS
	err error, //      : error if any
) {

	if conf == nil {
		conf = NewConfig() // use defaults
	}

	if err = conf.Validate(); err != nil {
		return
	}

	var pool *radix.Pool
	pool, err = radix.NewPool(network, addr, conf.Size, conf.Opts...)
	if err != nil {
		return
	}

	var rs Redis
	rs.pool = pool
	rs.expire = int64(conf.Expire / time.Second)
	rs.expireFunc = conf.ExpireFunc
	rs.scanCount = conf.ScanCount

	if err = rs.loadScripts(); err != nil {
		pool.Close()
		return
	}

	if err = rs.subscribeExpiredEvents(conf); err != nil {
		pool.Close()
		return
	}

	if err = rs.loadStat(); err != nil {
		pool.Close()
		return
	}

	if rs.isSafeClosed, err = rs.getSafeClosed(); err != nil {
		pool.Close()
		return
	}

	if err = rs.setSafeClosed(false); err != nil {
		pool.Close()
		return
	}

	r = &rs
	return
}

func (r *Redis) setSafeClosed(t bool) (err error) {
	err = r.pool.Do(radix.FlatCmd(nil, "SET", "idx:safe_closed", t))
	return
}

func (r *Redis) getSafeClosed() (safeClosed bool, err error) {
	var exists bool
	err = r.pool.Do(radix.Cmd(&exists, "EXISTS", "idx:safe_closed"))
	if err != nil {
		return
	}
	if exists == false {
		safeClosed = true // fresh DB
		return
	}
	err = r.pool.Do(radix.FlatCmd(&safeClosed, "GET", "idx:safe_closed"))
	return
}

func (r *Redis) loadScripts() (err error) {

	type scriptHash struct {
		script string
		hash   *string
	}

	for _, sh := range []scriptHash{
		{touchLua, &r.touchLua},
		{getLua, &r.getLua},
		{getIncrLua, &r.getIncrLua},
		{getNotTouchLua, &r.getNotTouchLua},
		{getIncrNotTouchLua, &r.getIncrNotTouchLua},
		{setIncrLua, &r.setIncrLua},
		{setIncrNotTouchLua, &r.setIncrNotTouchLua},
		{setRawLua, &r.setRawLua},
		{incrLua, &r.incrLua},
		{incrNotTouchLua, &r.incrNotTouchLua},
		{delLua, &r.delLua},
		{takeLua, &r.takeLua},
	} {
		err = r.pool.Do(radix.Cmd(sh.hash, "SCRIPT", "LOAD", sh.script))
		if err != nil {
			return
		}
	}

	return
}

/*

func (r *Redis) subscribeExpiredEvents(conf *Config) (err error) {

	if conf.Expire == 0 {
		return // don't subscribe (feature disabled)
	}

	// make sure that pool size is enough to wait subscriptions (1 connection)
	if conf.Size < 2 {
		return fmt.Errorf("can't enable Expire feature, small pool size %d",
			conf.Size)
	}

	// enable 'expired' event notifications
	err = r.pool.Do(radix.Cmd(nil, "CONFIG", "SET",
		"notify-keyspace-events", "Ex",
	))

	if err != nil {
		return
	}

	// run subscription in separate goroutine (ignore all errors)
	go r.waitEvents()
	return
}

func (r *Redis) waitEvents() {

	r.pool.Do(radix.WithConn("", func(c radix.Conn) (err error) {
		var psc = radix.PubSub(c)
		defer psc.Close()

		var ch = make(chan radix.PubSubMessage, 10)

		const eventExpired = "__keyevent@0__:expired"

		psc.Subscribe(ch, eventExpired)
		// defer psc.Unsubscribe(ch, eventExpired) // closed connection here

		for msg := range ch {
			if msg.Type != "message" {
				continue
			}
			if msg.Pattern != eventExpired {
				continue
			}
			var ms = string(msg.Message)
			ms = strings.TrimPrefix(ms, ":")

			var hash = cipher.MustSHA256FromHex(ms)
			r.expireFunc(hash) // callback
		}

		return
	}))

}

*/

// prefix
// ------
//
// idx:                          - prefix to use the same Redis server with CXDS
//
//
// service keys
// ------------
//
// idx:safe_closed               - SET
//
//
// feed and heads (presence)
// -------------------------
//
// idx:feed:[hex]                - HSET, HGET {nonce -> 1}, HDEL, HKEYS, DEL
//                                 {feed -> 1}
//
// root
// ----
//
// idx:[hex]:[nonce] seq 1        - ZADD, ZRANGEBYSCORE, ZREMRANGEBYSCORE, DEL
// idx:[hex]:[nonce]:[seq] [...]  - HSET, HDEL, HMSET, HMGET, DEL
//

// AddFeed. Adding a feed twice or more times does nothing.
func (r *Redis) AddFeed(pk cipher.PubKey) (err error) {
	err = r.pool.Do(radix.Cmd(nil, "SET", "idx:feed:"+pk.Hex(), "1"))
	return
}

// DelFeed with all heads and Root objects
// unconditionally. If feed doesn't exist
// then the Del returns ErrNoSuchFeed.
func (r *Redis) DelFeed(pk cipher.PubKey) (err error) {

	err = r.pool.Do(radix.FlatCmd(nil, "EVALSHA", r.delFeed, 2,
		"feed",
		"scan_count",
		key.Hex(),
		r.scanCount,
	))
	return
}

// Iterate all feeds. Use ErrStopIteration to
// stop iteration. The Iterate passes any error
// returned from given function through. Except
// ErrStopIteration that turns nil. It's possible
// to mutate the IdxDB inside the Iterate
func (r *Redis) IterateFeeds(iterateFunc data.IterateFeedsFunc) (err error) {

	var opts = radix.ScanOpts{
		Command: "SCAN",
		Count:   r.scanCount,
		Key:     "",
		Pattern: "idx:feed:*",
	}

	var scan = radix.NewScanner(r.pool, opts)

	// idx:feed:hex

	var (
		key string
		pk  cipher.PubKey
	)

	for scan.Next(&key) == true {

		key = strings.TrimPrefix(key, "idx:feed:")
		pk = cipher.MustPubKeyFromHex(key)

		if err = iterateFunc(pk); err != nil {
			if err == data.ErrStopIteration {
				break // close
			}
			scan.Close() // drop error
			return
		}

	}

	err = scan.Close()
	return
}

// HasFeed returns true if the IdxDB contains
// feed with given public key
func (r *Redis) HasFeed(pk cipher.PubKey) (ok bool, err error) {

	err = r.pool.Do(radix.Cmd(&ok, "EXISTS", "idx:feed:"+pk.Hex()))
	return
}

// FeedsLen is number of feeds in DB
func (r *Redis) FeedsLen() (length int, err error) {

	err = r.pool.Do(radix.FlatCmd(&length, "EVALSHA", r.feedsLen, 1,
		"scan_count",
		r.scanCount,
	))
	return
}

// AddHead new head with given nonce.
// If a head with given nonce already
// exists, then this method does nothing.
func (r *Redis) AddHead(pk cipher.PubKey, nonce uint64) (err error) {

	var hasFeed bool
	err = r.pool.Do(radix.FlatCmd(&hasFeed, "EVALSHA", r.addHead, 2,
		"feed",
		"head",
		pk.Hex(),
		nonce,
	))
	if err != nil {
		return
	}
	if hasFeed == false {
		err = data.ErrNoSuchFeed
	}
	return
}

// DelHead deletes head with given nonce and
// all its Root objects. The method returns
// ErrNoSuchHead if a head with given nonce
// doesn't exist.
func (r *Redis) DelHead(pk cipher.PubKey, nonce uint64) (err error) {

	var hasFeed bool
	err = r.pool.Do(radix.FlatCmd(&hasFeed, "EVALSHA", r.delHead, 3,
		"feed",
		"head",
		"scan_count",
		pk.Hex(),
		nonce,
		r.scanCount,
	))
	if err != nil {
		return
	}
	if hasFeed == false {
		err = data.ErrNoSuchFeed
	}
	return
}

// Has returns true if a head with given
// nonce exits in the DB
func (r *Redis) HasHead(pk cipher.PubKey, nonce uint64) (ok bool, err error) {

	var reply []bool
	err = r.pool.Do(radix.FlatCmd(&hasFeed, "EVALSHA", r.hasHead, 2,
		"feed",
		"head",
		pk.Hex(),
		nonce,
	))
	if err != nil {
		return
	}

	if len(reply) != 2 {
		err = fmt.Errorf("invalid length of reply: %d, want 2", len(reply))
		return
	}

	// has feed
	if reply[0] == false {
		err = data.ErrNoSuchFeed
		return
	}

	ok = reply[1]
	return
}

// IterateHeads iterates over all heads
func (r *Redis) IterateHeads(
	pk cipher.PubKey,
	iterateFunc data.IterateHeadsFunc,
) (err error) {

	//
	return
}

// HeadsLen is number of heads
func (r *Redis) HeadsLen(pk cipher.PubKey) (length int, err error) {

	var reply []int
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.feedsLen, 1,
		"scan_count",
		r.scanCount,
	))

	if err != nil {
		return
	}

	if len(reply) != 2 {
		err = fmt.Errorf("invlaid response length %d, want 2", len(reply))
		return
	}

	if reply[0] == 0 {
		err = data.ErrNoSuchFeed
		return
	}

	length = reply[1]
	return
}

// AscendRoots iterates all Root object ascending order.
// Use ErrStopIteration to stop iteration. Any error
// (except the ErrStopIteration) returned by given
// IterateRootsFunc will be passed through. The
// AscendRoots doesn't update access time of a Root.
// See also IterateRootsFunc docs.
func (r *Redis) AscendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc data.IterateRootsFunc,
) (err error) {

	//
	return
}

// DescendRoots is the same as the Ascend, but it iterates
// decending order. Use ErrStopIteration to stop
// iteration. The DescendRoots doesn't update access time.
// See also IterateRootsFunc docs.
func (r *Redis) DescendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc data.IterateRootsFunc,
) (err error) {

	//
	return
}

// HasRoot returns true if Root with given seq exists. The HasRoot
// never updates access time.
func (r *Redis) HasRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (ok bool, err error) {

	//
	return
}

// Len is number of Root objects stored
func (r *Redis) RootsLen(
	pk cipher.PubKey, nonce uint64,
) (length int, err error) {

	//
	return
}

// SetRoot add or touch Root if exists
func (r *Redis) SetRoot(
	pk cipher.PubKey,
	nonce uint64,
	seq uint64,
	hash cipher.SHA256,
	sig cipher.Sig,
) (root *data.Root, err error) {

	//
	return
}

// SetNotTouch add Root or do nothing if exists
func (r *Redis) SetNotTouchRoot(
	pk cipher.PubKey,
	nonce uint64,
	seq uint64,
	hash cipher.SHA256,
	sig cipher.Sig,
) (root *data.Root, err error) {

	//
	return
}

// GetRoot returns root and touches stored.
func (r *Redis) GetRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	//
	return
}

// GetNotTouchRoot returns root.
func (r *Redis) GetNotTouchRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	//
	return
}

// DelRoot deletes Root.
func (r *Redis) DelRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (err error) {

	//
	return
}

// IsSafeClosed is flag that means that DB has been
// closed successfully last time. If the IsSafeClosed
// returns false, then may be some repair required (it
// depends).
func (r *Redis) IsSafeClosed() (safeClosed bool) {
	return r.isSafeClosed
}

// Pool returns underlying *radix.Pool
func (r *Redis) Pool() *radix.Pool {
	return r.pool
}

// Close the Redis
func (r *Redis) Close() (err error) {

	r.closeo.Do(func() {
		// TODO (kostyarin): unsubscribe 'expired' events handler first

		if err = r.storeStat(); err != nil {
			r.pool.Close() // drop error
			return
		}

		if err = r.setSafeClosed(true); err != nil {
			r.pool.Close() // drop error
			return
		}

		// no way to remove loaded scripts, but it is not neccessary
		// (there is a way to remove all scripts)

		// dump to disk synchronously
		err = r.pool.Do(radix.Cmd(nil, "SAVE"))
		if err != nil {
			r.pool.Close() // drop error
			return
		}

		err = r.pool.Close()
	})

	return
}
