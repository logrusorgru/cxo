package redis

import (
	"fmt"
	"strconv"
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

	// scanning (iterate)
	scanCount int

	// scripts (SHA1)
	addHead                   string
	delFeed, delHead, delRoot string
	takeRoot                  string
	feedsLen, headsLen        string
	rootsLen                  string
	getRoot, getRootNotTouch  string
	hasHead, hasRoot          string
	rangeRoots                string
	setRoot, setRootNotTouch  string

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
	rs.scanCount = conf.ScanCount

	if err = rs.loadScripts(); err != nil {
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
		{addHeadLua, &r.addHead},
		{delFeedLua, &r.delFeed},
		{delHeadLua, &r.delHead},
		{delRootLua, &r.delRoot},
		{takeRootLua, &r.takeRoot},
		{feedsLenLua, &r.feedsLen},
		{headsLenLua, &r.headsLen},
		{rootsLenLua, &r.rootsLen},
		{getRootLua, &r.getRoot},
		{getRootNotTouchLua, &r.getRootNotTouch},
		{hasHeadLua, &r.hasHead},
		{hasRootLua, &r.hasRoot},
		{rangeRootsLua, &r.rangeRoots},
		{setRootLua, &r.setRoot},
		{setRootNotTouchLua, &r.setRootNotTouch},
	} {
		err = r.pool.Do(radix.Cmd(sh.hash, "SCRIPT", "LOAD", sh.script))
		if err != nil {
			return
		}
	}

	return
}

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
// idx:feed:[hex]                - HSET, HGET {nonce -> h}, HDEL, HKEYS, DEL
//                                 {feed -> 1}
//
// root
// ----
//
// idx:[hex]:[nonce] seq seq      - ZADD, ZRANGE, ZREVRANGE, ZREM, DEL
// idx:[hex]:[nonce]:[seq] [...]  - HSET, HDEL, HMSET, HMGET, DEL
//

// AddFeed. Adding a feed twice or more times does nothing.
func (r *Redis) AddFeed(pk cipher.PubKey) (err error) {
	err = r.pool.Do(radix.Cmd(nil, "HSET", "idx:feed:"+pk.Hex(), "feed", "1"))
	return
}

// DelFeed with all heads and Root objects
// unconditionally. If feed doesn't exist
// then the Del returns ErrNoSuchFeed.
func (r *Redis) DelFeed(pk cipher.PubKey) (err error) {

	var hasFeed boolReply
	err = r.pool.Do(radix.FlatCmd(&hasFeed, "EVALSHA", r.delFeed, 1,
		"feed",
		pk.Hex(),
	))

	if err != nil {
		return
	}

	if hasFeed == false {
		err = data.ErrNoSuchFeed
	}

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

	err = r.pool.Do(radix.FlatCmd(&length, "EVALSHA", r.feedsLen, 0))
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

	var reply delHeadReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.delHead, 2,
		"feed",
		"head",
		pk.Hex(),
		nonce,
	))
	if err != nil {
		return
	}
	if reply.HasFeed == false {
		return data.ErrNoSuchFeed
	}
	if reply.HasHead == false {
		return data.ErrNoSuchHead
	}
	return // nil
}

// Has returns true if a head with given
// nonce exits in the DB
func (r *Redis) HasHead(pk cipher.PubKey, nonce uint64) (ok bool, err error) {

	var reply []bool
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.hasHead, 2,
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

	var hasFeed bool
	err = r.pool.Do(radix.Cmd(&hasFeed, "EXISTS",
		"idx:feed:"+pk.Hex(),
	))
	if err != nil {
		return
	}

	if hasFeed == false {
		err = data.ErrNoSuchFeed
		return
	}

	var opts = radix.ScanOpts{
		Command: "HSCAN",                //
		Count:   r.scanCount,            //
		Key:     "idx:feed:" + pk.Hex(), //
		Pattern: "[^f]*",                // except 'feed'
	}

	var (
		scan = radix.NewScanner(r.pool, opts)

		elem  string
		nonce uint64

		i int
	)

	for scan.Next(&elem) == true {

		if i++; i%2 == 0 {
			continue // skip value
		}

		if nonce, err = strconv.ParseUint(elem, 10, 64); err != nil {
			panic(err) // must not happens
		}

		if err = iterateFunc(nonce); err != nil {
			if err == data.ErrStopIteration {
				err = nil
				break // go to the scan.Close()
			}
			scan.Close() // drop error
			return
		}

	}

	err = scan.Close()
	return
}

// HeadsLen is number of heads
func (r *Redis) HeadsLen(pk cipher.PubKey) (length int, err error) {

	var reply headsLenReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.headsLen, 1,
		"feed",
		pk.Hex(),
	))
	if err != nil {
		return
	}

	if reply.HasFeed == false {
		err = data.ErrNoSuchFeed
		return
	}

	length = reply.Length
	return
}

func (r *Redis) RangeRoots(
	pk cipher.PubKey,
	nonce uint64,
	iterateFunc data.IterateRootsFunc,
	reverse bool,
) (
	err error,
) {

	// in:  feed, head, start, scan_by, reverse
	// out: has_feed, has_head, {seqs}

	var (
		feed = pk.Hex()
		head = strconv.FormatUint(nonce, 10)

		startSeq uint64
		start    string = "-inf"
	)

	if reverse == true {
		start = "+inf"
	}

	for i := 0; ; i++ {

		if i > 0 {
			start = strconv.FormatUint(startSeq, 10)
		}

		var reply rangeRootsReply
		err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.rangeRoots, 5,
			"feed",
			"head",
			"start",
			"scan_by",
			"reverse",
			feed,
			head,
			start,
			r.scanCount,
			reverse,
		))

		if err != nil {
			return
		}

		if reply.HasFeed == false {
			return data.ErrNoSuchFeed
		}

		if reply.HasHead == false {
			return data.ErrNoSuchHead
		}

		for _, seq := range reply.Seqs {

			if err = iterateFunc(seq); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return // stop
			}

			startSeq = seq
		}

		// from next
		if reverse == false {
			startSeq++ // direct
		} else {
			startSeq-- // reverse
		}

		if len(reply.Seqs) < r.scanCount {
			break // end
		}

	}

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

	return r.RangeRoots(pk, nonce, iterateFunc, false)
}

// DescendRoots is the same as the Ascend, but it iterates
// decending order. Use ErrStopIteration to stop
// iteration. The DescendRoots doesn't update access time.
// See also IterateRootsFunc docs.
func (r *Redis) DescendRoots(
	pk cipher.PubKey, nonce uint64, iterateFunc data.IterateRootsFunc,
) (err error) {

	return r.RangeRoots(pk, nonce, iterateFunc, true)
}

// HasRoot returns true if Root with given seq exists. The HasRoot
// never updates access time.
func (r *Redis) HasRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (ok bool, err error) {

	var reply []int
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.hasRoot, 3,
		"feed",
		"head",
		"seq",
		pk.Hex(),
		nonce,
		seq,
	))

	if err != nil {
		return
	}

	if len(reply) != 3 {
		err = fmt.Errorf("invalid response length %d, want 3", len(reply))
	} else if reply[0] == 0 {
		err = data.ErrNoSuchFeed
	} else if reply[1] == 0 {
		err = data.ErrNoSuchHead
	} else {
		ok = reply[2] == 1
	}

	return
}

// RootsLen is number of Root objects stored
func (r *Redis) RootsLen(
	pk cipher.PubKey, nonce uint64,
) (length int, err error) {

	var reply []int64
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.rootsLen, 2,
		"feed",
		"head",
		pk.Hex(),
		nonce,
	))

	if err != nil {
		return
	}

	if len(reply) != 3 {
		err = fmt.Errorf("invalid response length %d, want 3", len(reply))
	} else if reply[0] == 0 {
		err = data.ErrNoSuchFeed
	} else if reply[1] == 0 {
		err = data.ErrNoSuchHead
	} else {
		length = int(reply[2])
	}

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

	var reply setRootReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.setRoot, 6,
		"feed",
		"head",
		"seq",
		"hash",
		"sig",
		"now",
		pk.Hex(),
		nonce,
		seq,
		hash[:],
		sig[:],
		time.Now().UnixNano(),
	))
	if err != nil {
		return
	}

	if reply.HasFeed == false {
		return nil, data.ErrNoSuchFeed
	} else if reply.HasHead == false {
		return nil, data.ErrNoSuchHead
	}

	root = new(data.Root)
	root.Hash = hash
	root.Sig = sig
	root.Access = reply.Access // last access time
	root.Create = reply.Create
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

	var reply setRootReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.setRootNotTouch, 6,
		"feed",
		"head",
		"seq",
		"hash",
		"sig",
		"now",
		pk.Hex(),
		nonce,
		seq,
		hash[:],
		sig[:],
		time.Now().UnixNano(),
	))
	if err != nil {
		return
	}

	if reply.HasFeed == false {
		return nil, data.ErrNoSuchFeed
	} else if reply.HasHead == false {
		return nil, data.ErrNoSuchHead
	}

	root = new(data.Root)
	root.Hash = hash
	root.Sig = sig
	root.Access = reply.Access // last access time
	root.Create = reply.Create
	return
}

// GetRoot returns root and touches stored.
func (r *Redis) GetRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	var reply getRootReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.getRoot, 4,
		"feed",
		"head",
		"seq",
		"now",
		pk.Hex(),
		nonce,
		seq,
		time.Now().UnixNano(),
	))
	if err != nil {
		return
	}

	if reply.HasFeed == false {
		return nil, data.ErrNoSuchFeed
	} else if reply.HasHead == false {
		return nil, data.ErrNoSuchHead
	}

	if reply.Hash == (cipher.SHA256{}) {
		return nil, data.ErrNotFound
	}

	root = new(data.Root)
	root.Hash = reply.Hash
	root.Sig = reply.Sig
	root.Access = reply.Access // last access time
	root.Create = reply.Create
	return
}

// GetNotTouchRoot returns root.
func (r *Redis) GetNotTouchRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	var reply getRootReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.getRootNotTouch, 3,
		"feed",
		"head",
		"seq",
		pk.Hex(),
		nonce,
		seq,
	))
	if err != nil {
		return
	}

	if reply.HasFeed == false {
		return nil, data.ErrNoSuchFeed
	} else if reply.HasHead == false {
		return nil, data.ErrNoSuchHead
	}

	if reply.Hash == (cipher.SHA256{}) {
		return nil, data.ErrNotFound
	}

	root = new(data.Root)
	root.Hash = reply.Hash
	root.Sig = reply.Sig
	root.Access = reply.Access // last access time
	root.Create = reply.Create
	return
}

// TakeRoot deletes Root returinig it.
func (r *Redis) TakeRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (root *data.Root, err error) {

	var reply getRootReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.takeRoot, 3,
		"feed",
		"head",
		"seq",
		pk.Hex(),
		nonce,
		seq,
	))
	if err != nil {
		return
	}

	if reply.HasFeed == false {
		return nil, data.ErrNoSuchFeed
	} else if reply.HasHead == false {
		return nil, data.ErrNoSuchHead
	}

	if reply.Hash == (cipher.SHA256{}) {
		return nil, data.ErrNotFound
	}

	root = new(data.Root)
	root.Hash = reply.Hash
	root.Sig = reply.Sig
	root.Access = reply.Access // last access time
	root.Create = reply.Create
	return
}

// DelRoot deletes Root.
func (r *Redis) DelRoot(
	pk cipher.PubKey, nonce uint64, seq uint64,
) (err error) {

	var reply []int
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.delRoot, 3,
		"feed",
		"head",
		"seq",
		pk.Hex(),
		nonce,
		seq,
	))
	if err != nil {
		return
	}

	if len(reply) != 3 {
		err = fmt.Errorf("invalid response length: %d, want 3", len(reply))
	} else if reply[0] == 0 {
		err = data.ErrNoSuchFeed
	} else if reply[1] == 0 {
		err = data.ErrNoSuchHead
	} else if reply[2] == 0 {
		err = data.ErrNotFound
	}

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
