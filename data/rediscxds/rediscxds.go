package rediscxds

import (
	"bufio"
	"fmt"
	"strings"
	"time"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/skycoin/src/cipher"

	"github.com/mediocregopher/radix.v3"
	"github.com/mediocregopher/radix.v3/resp"
)

// Convention
//   - if 'create' field is zero (that is equal to time.Time{})
//     then object doesn't exist

// A Redis implments data.CXDS
// interface over Redis database.
type Redis struct {
	pool         *radix.Pool // conenctions pool
	isSafeClosed bool        // current state

	// LRU timeout feature
	expire     time.Duration //
	expireFunc ExpireFunc    //

	data.Hooks // hooks

	// scripts (SHA1)
	touchLua                                  string
	getLua, getIncrLua, getIncrNotTouchLua    string
	setIncrLua, setIncrNotTouchLua, setRawLua string
	incrLua, incrNotTouchLua                  string
	delLua, takeLua                           string
}

// NewCXDS creates CXDS based on Redis
func NewCXDS(
	network string, // : "tcp", "tcp4" or "tcp6"
	addr string, //    : address of Redis server
	conf *Config, //   : configurations
) (
	ds data.CXDS, //   : the data.CXDS
	err error, //      : error if any
) {

	if err = conf.Validate(); err != nil {
		return
	}

	var pool *radix.Pool
	pool, err = radix.NewPool(network, addr, conf.Size, conf.Opts...)
	if err != nil {
		return
	}

	var rc Redis
	rc.pool = pool
	rc.expire = conf.Expire / time.Second
	rc.expireFunc = conf.ExpireFunc

	if err = rc.loadScripts(); err != nil {
		pool.Close()
		return
	}

	if err = rc.subscribeExpiredEvents(conf); err != nil {
		pool.Close()
		return
	}

	if rc.isSafeClosed, err = rc.getSafeClosed(); err != nil {
		pool.Close()
		return
	}

	if err = rc.setSafeClosed(false); err != nil {
		pool.Close()
		return
	}

	ds = &rc
	return
}

func (r *Redis) setSafeClosed(t bool) (err error) {
	err = r.pool.Do(radix.FlatCmd(nil, "SET", "safeClosed", t))
	return
}

func (r *Redis) getSafeClosed() (safeClosed bool, err error) {
	err = r.pool.Do(radix.FlatCmd(&safeClosed, "GET", "safeClosed"))
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
		{getIncrNotTouchLua, &r.getIncrNotTouchLua},
		{setIncrLua, &r.setIncrLua},
		{setIncrNotTouchLua, &r.setIncrNotTouchLua},
		{setRawLua, &r.setRawLua},
		{incrLua, &r.incrLua},
		{incrNotTouchLua, &r.incrNotTouchLua},
		{delLua, &r.delLua},
		{takeLua, &r.takeLua},
	} {

		var reply resp.BulkString

		err = r.pool.Do(radix.Cmd(&reply, "SCRIPT LOAD", sh.script))
		if err != nil {
			return
		}

		*sh.hash = reply.S

	}

	return
}

func (r *Redis) subscribeExpiredEvents(conf *Config) (err error) {

	if conf.Expire == 0 || conf.ExpireFunc == nil {
		return // don't subscribe (feature disabled)
	}

	// make sure that pool size is enough to wait subscriptions (1 connection)
	if conf.Size < 2 {
		return fmt.Errorf("can't enable Expire feature, small pool size %d",
			conf.Size)
	}

	// enable 'expired' event notifications
	err = r.pool.Do(radix.Cmd(nil, "CONFIG SET",
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

	r.pool.Do(radix.WithConn("", func(c *radix.Conn) (err error) {
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
			ms = strings.TrimSuffix(ms, ".ex")

			var pk, err = cipher.PubKeyFromHex(ms)
			if err != nil {
				panic(err)
			}

			r.expireFunc(pk) // callback
		}

	}))

}

//
// Touch
//

func (r *Redis) beforeTouchHooks(key cipher.SHA256) (err error) {
	defer r.BeforeTouchHooksClose()
	for _, hook := range r.BeforeTouchHooks(key) {
		if _, err = hook(key); err != nil { // ignore the meta (_)
			return
		}
	}
	return
}

func (r *Redis) Touch(key cipher.SHA256) (access time.Time, err error) {

	if err = r.beforeTouchHooks(key); err != nil {
		return
	}

	var reply []int64 // exists, access

	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.touchLua, 3,
		"expire",
		"hex",
		"now",
		r.expire,
		key.Hex(),
		time.Now().UnixNano(),
	))

	if err != nil {
		r.CallAfterTouchHooks(key, access, err)
		return
	}

	if len(reply) != 2 {
		err = fmt.Errorf("invalid reply length %d, want 2", len(reply))
		r.CallAfterTouchHooks(key, access, err)
		return
	}

	// exists (reply[0])
	if reply[0] == 0 {
		r.CallAfterTouchHooks(key, access, data.ErrNotFound)
		return
	}

	access = time.Unix(0, reply[1])
	r.CallAfterTouchHooks(key, access, nil)
	return
}

//
// Get
//

func (r *Redis) Get(key cipher.SHA256) (obj *Object, err error) {
	//
	return
}

func (r *Redis) GetIncr(
	key cipher.SHA256,
	incrBy int64,
) (
	obj *Object,
	err error,
) {
	//
	return
}

func (r *Redis) GetNotTouch(key cipher.SHA256) (obj *Object, err error) {
	//
	return
}

func (r *Redis) GetIncrNotTouch(
	key cipher.SHA256,
	incrBy int64,
) (
	obj *Object,
	err error,
) {
	//
	return
}

func (r *Redis) Set(key cipher.SHA256, val []byte) (obj *Object, err error) {
	//
	return
}

func (r *Redis) SetIncr(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	obj *Object, //       : object with new RC and previous last access time
	err error, //         : error if any
) {
	//
	return
}

func (r *Redis) SetNotTouch(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
) (
	obj *Object, //       : object with new RC and previous last access time
	err error, //         : error if any
) {
	//
	return
}

func (r *Redis) SetIncrNotTouch(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	obj *Object, //       : object with new RC and previous last access time
	err error, //         : error if any
) {
	//
	return
}

func (r *Redis) SetRaw(key cipher.SHA256, obj *Object) (err error) {
	//
	return
}

func (r *Redis) Incr(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	rc int64, //          : new RC
	access time.Time, //  : previous last access time
	err error, //         : error if any
) {
	//
	return
}

func (r *Redis) IncrNotTouch(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	rc int64, //          : new RC
	access time.Time, //  : previous last access time
	err error, //         : error if any
) {
	//
	return
}

func (r *Redis) Take(key cipher.SHA256) (obj *Object, err error) {
	//
	return
}

func (r *Redis) Del(key cipher.SHA256) (err error) {
	//
	return
}

func (r *Redis) Iterate(iterateFunc IterateObjectsFunc) (err error) {
	//
	return
}

func (r *Redis) IterateDel(iterateFunc IterateObjectsDelFunc) (err error) {
	//
	return
}

func (r *Redis) Amount() (all, used int64) {
	//
	return
}

func (r *Redis) Volume() (all, used int64) {
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

// Close the CXDS
func (r *Redis) Close() (err error) {

	err = r.client.Do(radix.FlatCmd(nil, "SET", "safeClosed", true))
	if err != nil {
		r.client.Close() // drop error
		return
	}

	return r.client.Close()
}
