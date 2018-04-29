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

// A Redis implments data.CXDS
// interface over Redis <redis.io>.
type Redis struct {
	pool         *radix.Pool // conenctions pool
	isSafeClosed bool        // current state

	// LRU timeout feature
	expire     int64      // a'la time.Duration in seconds
	expireFunc ExpireFunc //

	// scanning (iterate)
	scanCount int

	// amount and volume
	statMutex sync.Mutex
	amount    stat
	volume    stat

	data.HooksKeepper // hooks

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
	err = r.pool.Do(radix.FlatCmd(nil, "SET", ":safe_closed", t))
	return
}

func (r *Redis) getSafeClosed() (safeClosed bool, err error) {
	var exists bool
	err = r.pool.Do(radix.Cmd(&exists, "EXISTS", ":safe_closed"))
	if err != nil {
		return
	}
	if exists == false {
		safeClosed = true // fresh DB
		return
	}
	err = r.pool.Do(radix.FlatCmd(&safeClosed, "GET", ":safe_closed"))
	return
}

func (r *Redis) storeStat() (err error) {
	r.statMutex.Lock()
	defer r.statMutex.Unlock()

	err = r.pool.Do(radix.FlatCmd(nil, "HMSET", ":stat",
		"amount_all", r.amount.all,
		"amount_used", r.amount.used,
		"volume_all", r.volume.all,
		"volume_used", r.volume.used,
	))
	return
}

func (r *Redis) loadStat() (err error) {
	r.statMutex.Lock()
	defer r.statMutex.Unlock()

	var reply statReply
	err = r.pool.Do(radix.FlatCmd(&reply, "HMGET", ":stat",
		"amount_all",
		"amount_used",
		"volume_all",
		"volume_used",
	))
	if err != nil {
		return
	}
	r.amount.all = reply.AmountAll
	r.amount.used = reply.AmountUsed
	r.volume.all = reply.VolumeAll
	r.volume.used = reply.VolumeUsed
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

//
// Hooks
//

// Hooks returns object to access hooks of the Redis
func (r *Redis) Hooks() (hooks data.Hooks) {
	return r
}

//
// Touch
//

func (r *Redis) beforeTouchHooks(key cipher.SHA256) (err error) {
	defer r.BeforeTouchHooksClose()
	for _, hook := range r.BeforeTouchHooks() {
		if _, err = hook(key); err != nil { // ignore the meta (_)
			return
		}
	}
	return
}

// Touch updates access time of object by key. The Touch returns
// data.ErrNotFound if object doesn't exist.
func (r *Redis) Touch(key cipher.SHA256) (access time.Time, err error) {

	if err = r.beforeTouchHooks(key); err != nil {
		return
	}

	var reply touchReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.touchLua, 3,
		"expire",
		"hex",
		"now",
		r.expire,
		key.Hex(),
		time.Now().UnixNano(),
	))

	if err != nil {
		r.CallAfterTouchHooks(key, time.Time{}, err)
		return
	}

	// if not exist
	if reply.Exists == false {
		err = data.ErrNotFound
		r.CallAfterTouchHooks(key, time.Time{}, err)
		return
	}

	access = reply.Access
	r.CallAfterTouchHooks(key, access, nil)
	return
}

//
// Get
//

func (r *Redis) beforeGetHooks(key cipher.SHA256, incrBy int64) (err error) {
	defer r.BeforeGetHooksClose()
	for _, hook := range r.BeforeGetHooks() {
		if _, err = hook(key, incrBy); err != nil { // ignore the meta (_)
			return
		}
	}
	return
}

func (r *Redis) changeStatAfter(created bool, rc, incrBy, volume int64) {
	r.statMutex.Lock()
	defer r.statMutex.Unlock()

	// set methods
	if created == true {
		r.amount.all++
		r.volume.all += volume
		if rc > 0 {
			r.amount.used++
			r.volume.used += volume
		}
		return
	}

	if incrBy == 0 {
		return // no changes
	}
	if rc <= 0 {
		if rc-incrBy > 0 {
			r.amount.used--                // } one of objects,
			r.volume.used -= int64(volume) // }  turns to be not used
		}
		return
	}
	// rc > 0
	if rc-incrBy <= 0 {
		r.amount.used++                // } reborn
		r.volume.used += int64(volume) // }
	}
	return
}

func (r *Redis) get(
	reply *getReply,
	action radix.CmdAction,
	key cipher.SHA256,
	incrBy int64,
) (
	obj *data.Object,
	err error,
) {

	if err = r.beforeGetHooks(key, incrBy); err != nil {
		return
	}

	if err = r.pool.Do(action); err != nil {
		r.CallAfterGetHooks(key, nil, err)
		return
	}

	// exists (is nil)
	if obj = reply.Object(); obj == nil {
		err = data.ErrNotFound
		r.CallAfterGetHooks(key, nil, err)
		return
	}

	r.changeStatAfter(false, obj.RC, incrBy, int64(len(obj.Val)))
	r.CallAfterGetHooks(key, obj, err)
	return
}

// Get object by key, updating access time, but returning
// object with previous access time. The Get method returns
// data.ErrNotFound if obejct doesn't exist.
func (r *Redis) Get(key cipher.SHA256) (*data.Object, error) {
	var reply getReply
	return r.get(&reply, radix.FlatCmd(&reply, "EVALSHA", r.getLua, 3,
		"expire",
		"hex",
		"now",
		r.expire,
		key.Hex(),
		time.Now().UnixNano(),
	), key, 0)
}

// GetIncr is the same as the Get. The GetIncr method
// allows to change RC of object.
func (r *Redis) GetIncr(key cipher.SHA256, incrBy int64) (*data.Object, error) {
	var reply getReply
	return r.get(&reply, radix.FlatCmd(&reply, "EVALSHA", r.getIncrLua, 4,
		"expire",
		"hex",
		"incr",
		"now",
		r.expire,
		key.Hex(),
		incrBy,
		time.Now().UnixNano(),
	), key, incrBy)
}

// GetNotTouch is the same as the Get, but the GetNotTouch method never
// updates access time.
func (r *Redis) GetNotTouch(key cipher.SHA256) (obj *data.Object, err error) {
	var reply getReply
	return r.get(&reply, radix.FlatCmd(&reply, "EVALSHA", r.getNotTouchLua, 2,
		"expire",
		"hex",
		r.expire,
		key.Hex(),
	), key, 0)
}

// GetIncrNotTouch is the same as the GetNotTouch, but it allows to
// change RC of object.
func (r *Redis) GetIncrNotTouch(
	key cipher.SHA256, incrBy int64,
) (*data.Object, error) {

	var reply getReply
	return r.get(&reply,
		radix.FlatCmd(&reply, "EVALSHA", r.getIncrNotTouchLua, 3,
			"expire",
			"hex",
			"incr",
			r.expire,
			key.Hex(),
			incrBy,
		), key, incrBy)
}

func (r *Redis) beforeSetHooks(
	key cipher.SHA256,
	val []byte,
	incrBy int64,
) (
	err error,
) {

	defer r.BeforeSetHooksClose()
	for _, hook := range r.BeforeSetHooks() {
		if _, err = hook(key, val, incrBy); err != nil { // ignore the meta (_)
			return
		}
	}
	return
}

func (r *Redis) set(
	reply *setReply,
	action radix.CmdAction,
	key cipher.SHA256,
	val []byte,
	incrBy int64,
) (
	obj *data.Object,
	err error,
) {

	if err = r.beforeSetHooks(key, val, incrBy); err != nil {
		return
	}

	if err = r.pool.Do(action); err != nil {
		r.CallAfterSetHooks(key, nil, err)
		return
	}

	obj = new(data.Object)
	obj.Val = val             //
	obj.RC = reply.RC         // new RC
	obj.Access = reply.Access //
	obj.Create = reply.Create //

	r.changeStatAfter(reply.Created, obj.RC, incrBy, int64(len(val)))
	r.CallAfterSetHooks(key, obj, err)
	return
}

// Set or update object incrementing RC by 1 and updating access time
func (r *Redis) Set(key cipher.SHA256, val []byte) (*data.Object, error) {
	return r.SetIncr(key, val, 1)
}

// SetIncr sets or updates object incrementing RC by given value and
// updating access time
func (r *Redis) SetIncr(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	obj *data.Object, //  : object with new RC and previous last access time
	err error, //         : error if any
) {
	var reply setReply
	return r.set(&reply, radix.FlatCmd(&reply, "EVALSHA", r.setIncrLua, 5,
		"expire",
		"hex",
		"val",
		"incr",
		"now",
		r.expire,
		key.Hex(),
		val,
		incrBy,
		time.Now().UnixNano(),
	), key, val, incrBy)
}

// SetNotTouch is the same as the Set, but it doesn't update access time.
// But, if it creates new object, then the access time is set to now.
// Since, the access time can't be less then create time.
func (r *Redis) SetNotTouch(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
) (
	obj *data.Object, //  : object with new RC and previous last access time
	err error, //         : error if any
) {
	return r.SetIncrNotTouch(key, val, 1)
}

// SetIncrNotTouch is the same as the SetNotTouch,
// that uses given value to increment RC
func (r *Redis) SetIncrNotTouch(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	obj *data.Object, //  : object with new RC and previous last access time
	err error, //         : error if any
) {
	var reply setReply
	return r.set(&reply,
		radix.FlatCmd(&reply, "EVALSHA", r.setIncrNotTouchLua, 5,
			"expire",
			"hex",
			"val",
			"incr",
			"now",
			r.expire,
			key.Hex(),
			val,
			incrBy,
			time.Now().UnixNano(),
		), key, val, incrBy)
}

func (r *Redis) changeStatAfterSetRaw(
	overwritten bool, // :
	pvol, prc int64, //  : previous
	vol, rc int64, //    : new
) {
	r.statMutex.Lock()
	defer r.statMutex.Unlock()

	// regards to collisons or use of blank value for some developer reasons

	if overwritten == true {
		r.volume.all += (vol - pvol) // diff
		if prc <= 0 {                // was dead
			if rc > 0 { // reborn
				r.amount.used++
				r.volume.used += vol
			}
			// else -> still dead (do nothing)
		} else { // was alive
			if rc <= 0 { // kill
				r.amount.used--
				r.volume.used -= pvol
			} else { // still alive
				r.volume.used += (vol - pvol) // diff
			}
		}
	} else { // new object created
		r.volume.all += vol
		r.amount.all++
		if rc > 0 { // alive object
			r.volume.used += vol
			r.amount.used++
		}
	}

}

// SetRaw object, overwriting existing if exists
func (r *Redis) SetRaw(key cipher.SHA256, obj *data.Object) (err error) {

	if err = r.beforeSetHooks(key, obj.Val, 0); err != nil {
		return
	}

	var reply setRawReply // overwritten, prev_vol, prev_rc
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.setRawLua, 6,
		"expire",
		"hex",
		"val",
		"rc",
		"access",
		"create",
		r.expire,
		key.Hex(),
		obj.Val,
		obj.RC,
		obj.Access.UnixNano(),
		obj.Create.UnixNano(),
	))

	if err != nil {
		r.CallAfterSetHooks(key, nil, err)
		return
	}

	r.changeStatAfterSetRaw(
		reply.Overwritten,   // : overwritten
		reply.PrevVol,       // : prev_vol
		reply.PrevRC,        // : prev_rc
		int64(len(obj.Val)), // : vol
		obj.RC,              // : rc
	)
	r.CallAfterSetHooks(key, obj, err)
	return
}

func (r *Redis) beforeIncrHooks(key cipher.SHA256, incrBy int64) (err error) {
	defer r.BeforeIncrHooksClose()
	for _, hook := range r.BeforeIncrHooks() {
		if _, err = hook(key, incrBy); err != nil {
			return
		}
	}
	return
}

func (r *Redis) incr(
	reply *incrReply, //       :
	action radix.CmdAction, // :
	key cipher.SHA256, //      :
	incrBy int64, //           :
) (
	rc int64, //               :
	access time.Time, //       :
	err error, //              :
) {

	if err = r.beforeIncrHooks(key, incrBy); err != nil {
		return
	}

	if err = r.pool.Do(action); err != nil {
		r.CallAfterIncrHooks(key, rc, access, err) // key, 0, time.Time{}, err
		return
	}

	if reply.Exists == false {
		err = data.ErrNotFound
		r.CallAfterIncrHooks(key, 0, time.Time{}, err)
		return
	}

	rc = reply.RC
	access = reply.Access

	r.changeStatAfter(false, rc, incrBy, reply.Vol)
	r.CallAfterIncrHooks(key, rc, access, err)
	return
}

// Incr changes RC of object by key
func (r *Redis) Incr(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	int64, //             : new RC
	time.Time, //         : previous last access time
	error, //             : error if any
) {
	var reply incrReply
	return r.incr(&reply, radix.FlatCmd(&reply, "EVALSHA", r.incrLua, 4,
		"expire",
		"hex",
		"incr",
		"now",
		r.expire,
		key.Hex(),
		incrBy,
		time.Now().UnixNano(),
	), key, incrBy)
}

// IncrNotTouch is like the Incr but doesn't update access time
func (r *Redis) IncrNotTouch(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	int64, //             : new RC
	time.Time, //         : previous last access time
	error, //             : error if any
) {
	var reply incrReply
	return r.incr(&reply, radix.FlatCmd(&reply, "EVALSHA", r.incrNotTouchLua, 3,
		"expire",
		"hex",
		"incr",
		r.expire,
		key.Hex(),
		incrBy,
	), key, incrBy)
}

func (r *Redis) beforeDelHooks(key cipher.SHA256) (err error) {
	defer r.BeforeDelHooksClose()
	for _, hook := range r.BeforeDelHooks() {
		if _, err = hook(key); err != nil {
			return
		}
	}
	return
}

func (r *Redis) changeStatAfterDel(rc, vol int64) {
	r.statMutex.Lock()
	defer r.statMutex.Unlock()

	r.amount.all--
	r.volume.all -= vol

	if rc > 0 {
		r.amount.used--
		r.volume.used -= vol
	}
}

// Take is get and delete
func (r *Redis) Take(key cipher.SHA256) (obj *data.Object, err error) {

	if err = r.beforeDelHooks(key); err != nil {
		return
	}

	var reply getReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.takeLua, 2,
		"expire",
		"hex",
		r.expire,
		key.Hex(),
	))
	if err != nil {
		r.CallAfterDelHooks(key, nil, err)
		return
	}

	if obj = reply.Object(); obj == nil {
		err = data.ErrNotFound
		r.CallAfterDelHooks(key, nil, err)
		return
	}

	r.changeStatAfterDel(obj.RC, int64(len(obj.Val)))
	r.CallAfterIncrHooks(key, obj.RC, obj.Access, err)
	return
}

// Del by key
func (r *Redis) Del(key cipher.SHA256) (err error) {

	if err = r.beforeDelHooks(key); err != nil {
		return
	}

	var reply delReply
	err = r.pool.Do(radix.FlatCmd(&reply, "EVALSHA", r.delLua, 2,
		"expire",
		"hex",
		r.expire,
		key.Hex(),
	))
	if err != nil {
		r.CallAfterDelHooks(key, nil, err)
		return
	}

	if reply.Deleted == false {
		err = data.ErrNotFound
		r.CallAfterDelHooks(key, nil, err)
		return
	}

	r.changeStatAfterDel(reply.RC, reply.Vol)
	r.CallAfterDelHooks(key, nil, nil)
	return
}

// Iterate keys
func (r *Redis) Iterate(iterateFunc data.IterateKeysFunc) (err error) {

	var scan = radix.NewScanner(r.pool, radix.ScanOpts{
		Command: "SCAN",
		Pattern: "[^:]*", // not start from ':'
		Count:   r.scanCount,
	})

	var (
		hex string
		key cipher.SHA256
	)

	for scan.Next(&hex) == true {
		key = cipher.MustSHA256FromHex(hex)
		if err = iterateFunc(key); err != nil {
			if err == data.ErrStopIteration {
				break // brak the loop
			}
			scan.Close() // drop this error
			return
		}
	}

	err = scan.Close()
	return
}

// Amount of objects
func (r *Redis) Amount() (all, used int64) {
	r.statMutex.Lock()
	defer r.statMutex.Unlock()

	return r.amount.all, r.amount.used
}

// Volume of object (payload only)
func (r *Redis) Volume() (all, used int64) {
	r.statMutex.Lock()
	defer r.statMutex.Unlock()

	return r.volume.all, r.volume.used
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
