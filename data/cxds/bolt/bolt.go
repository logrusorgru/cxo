package bolt

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

// ScanBy is default
const ScanBy = 100

var (
	objsBucket = []byte("o") // objects bucket
	infoBucket = []byte("i") // information bucket
	infoKey    = infoBucket  // information key
)

func appendInt(p []byte, i int64) []byte {
	var t [8]byte
	binary.BigEndian.PutUint64(t[:], uint64(i))
	return append(p, t[:]...)
}

func appendBool(p []byte, t bool) []byte {
	if t {
		return append(p, 0xff)
	}
	return append(p, 0x00)
}

func getInt(p []byte) int64 {
	return int64(binary.BigEndian.Uint64(p))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func addOne(b []byte) {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == 0xff {
			continue
		}
		b[i] += 1
		break
	}
	return
}

type stat struct {
	all, used int64
}

type metaInfo struct {
	amount, volume stat
	isSafeClosed   bool
}

func (m *metaInfo) encode() (p []byte) {
	p = make([]byte, 0, 17)

	p = appendInt(p, m.amount.all)  // 8
	p = appendInt(p, m.amount.used) // 16

	p = appendInt(p, m.volume.all)  // 24
	p = appendInt(p, m.volume.used) // 32

	p = appendBool(p, m.isSafeClosed) // 33
	return
}

func (m *metaInfo) decode(p []byte) (err error) {
	if len(p) != 33 {
		return fmt.Errorf("invalid length of encoded metaInfo %d", len(p))
	}
	m.amount.all = getInt(p)
	m.amount.used = getInt(p[8:])
	m.volume.all = getInt(p[16:])
	m.volume.used = getInt(p[24:])
	m.isSafeClosed = p[32] > 0
	return
}

func vol(val []byte) int64 {
	return int64(len(val))
}

type Bolt struct {
	b *bolt.DB

	scanBy int

	sync.Mutex // lock stat
	metaInfo
	closeo sync.Once
}

// NewBolt creates new DB or opens existsing.
// The scanBy argument used by Iterate method.
// If the scanBy is zero, then default value
// used. Arguments path, mode and boltOptions
// relate to bolt db.
func NewBolt(
	path string, //               : path to DB file
	mode os.FileMode, //          : file mode
	boltOptions *bolt.Options, // : bolt DB options (can be nil)
	scanBy int, //                : elements in Iterate loop (zero is de)
) (b *Bolt, err error) {

	var db *bolt.DB
	if db, err = bolt.Open(path, mode, boltOptions); err != nil {
		return
	}

	var x = new(Bolt)
	x.b = db

	if scanBy <= 0 {
		x.scanBy = ScanBy
	} else {
		x.scanBy = scanBy
	}

	if err = x.createBucketsIfNotExists(); err != nil {
		x.b.Close()
		return
	}

	if err = x.getInfo(); err != nil {
		x.b.Close()
		return
	}

	var sc = x.isSafeClosed // temporary
	x.isSafeClosed = false
	if err = x.setInfo(); err != nil {
		x.b.Close()
		return
	}

	x.isSafeClosed = sc // restore
	return x, nil
}

func (b *Bolt) createBucketsIfNotExists() error {
	return b.b.Update(func(t *bolt.Tx) (err error) {
		if _, err = t.CreateBucketIfNotExists(infoBucket); err != nil {
			return
		}
		_, err = t.CreateBucketIfNotExists(objsBucket)
		return
	})
}

func (b *Bolt) getInfo() (err error) {
	err = b.b.View(func(t *bolt.Tx) (err error) {
		var val = t.Bucket(infoBucket).Get(infoKey)
		if len(val) == 0 {
			b.isSafeClosed = true // fresh DB
			return
		}
		err = b.decode(val)
		return
	})
	return
}

func (b *Bolt) setInfo() (err error) {
	err = b.b.Update(func(t *bolt.Tx) error {
		return t.Bucket(infoBucket).Put(infoKey, b.encode())
	})
	return
}

func (b *Bolt) do(doFunc func(b *bolt.Bucket) error) error {
	return b.b.Update(func(t *bolt.Tx) error {
		return doFunc(t.Bucket(objsBucket))
	})
}

func getObject(
	objs *bolt.Bucket, key cipher.SHA256,
) (obj *data.Object, err error) {

	var val = objs.Get(key[:])
	if val == nil {
		return nil, data.ErrNotFound
	}
	obj = new(data.Object)
	must(obj.Decode(val))
	return
}

func setObject(objs *bolt.Bucket, key cipher.SHA256, obj *data.Object) error {
	return objs.Put(key[:], obj.Encode())
}

// Hooks retursn nil (not implemented)
func (b *Bolt) Hooks() (hooks data.Hooks) {
	return // nil
}

func (b *Bolt) changeStatAfter(created bool, rc, incrBy, volume int64) {
	b.Lock()
	defer b.Unlock()

	// set methods
	if created == true {
		b.amount.all++
		b.volume.all += volume
		if rc > 0 {
			b.amount.used++
			b.volume.used += volume
		}
		return
	}

	if incrBy == 0 {
		return // no changes
	}
	if rc <= 0 {
		if rc-incrBy > 0 {
			b.amount.used--         // } one of objects,
			b.volume.used -= volume // }  turns to be not used
		}
		return
	}
	// rc > 0
	if rc-incrBy <= 0 {
		b.amount.used++         // } reborn
		b.volume.used += volume // }
	}
	return
}

// Touch object by its key updating its last access time.
// The Touch method returns ErrNotFound if object doesn't
// exist. The Touch returns previous last access time.
func (b *Bolt) Touch(key cipher.SHA256) (access time.Time, err error) {
	err = b.do(func(objs *bolt.Bucket) (err error) {
		var obj *data.Object
		if obj, err = getObject(objs, key); err != nil {
			return
		}
		access = obj.Access     // <- get last access time
		obj.Access = time.Now() // < and touch
		return setObject(objs, key, obj)
	})
	return
}

//
// Get* methods.
//
//
// Get* methods used to obtain an object. If object dosn't
// exist, then the Get* methods return ErrNotFound.
//
// Get Object by key updating its last access time.
func (b *Bolt) Get(key cipher.SHA256) (*data.Object, error) {
	return b.GetIncr(key, 0)
}

// GetIncr is the same as the Get but it changes
// RC using provided argument. The argument can
// be zero, actually.
func (b *Bolt) GetIncr(
	key cipher.SHA256, incrBy int64,
) (obj *data.Object, err error) {

	err = b.do(func(objs *bolt.Bucket) (err error) {
		if obj, err = getObject(objs, key); err != nil {
			return
		}

		var access = obj.Access // <- get last access time
		obj.Access = time.Now() // < and touch
		obj.RC += incrBy        // <- incr by

		if err = setObject(objs, key, obj); err != nil {
			return
		}
		obj.Access = access // previous
		b.changeStatAfter(false, obj.RC, incrBy, vol(obj.Val))
		return
	})
	return
}

// GetNotTouch is the same as the Get but it
// doesn't update last access time.
func (b *Bolt) GetNotTouch(key cipher.SHA256) (*data.Object, error) {
	return b.GetIncrNotTouch(key, 0)
}

// GetIncNotTouch is the same as the GetIncr but
// it doesn't update last access time.
func (b *Bolt) GetIncrNotTouch(
	key cipher.SHA256, incrBy int64,
) (obj *data.Object, err error) {

	err = b.do(func(objs *bolt.Bucket) (err error) {
		if obj, err = getObject(objs, key); err != nil {
			return
		}

		obj.RC += incrBy // <- incr by

		if err = setObject(objs, key, obj); err != nil {
			return
		}
		b.changeStatAfter(false, obj.RC, incrBy, vol(obj.Val))
		return
	})
	return
}

// Set* methods used to create new object. If obejct is
// alredy exists, then Set* method updates access time,
// and increments RC. The Set and SetNotTouch methods
// increments the RC by +1. The SetIcnr and
// SetIncrNotTouch methods use provided value.
//
// Set creates new object or updates existsing. The Set
// method equal to the SetIncr method with `incrBy = 1`.
func (b *Bolt) Set(key cipher.SHA256, val []byte) (*data.Object, error) {
	return b.SetIncr(key, val, 1)
}

// SetIncr uses provided inrBy argument to change
// RC of object. If object already exists, then
// no auto +1 added. The SetIncr with `incrBy = 1`
// is the same as the Set.
func (b *Bolt) SetIncr(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	obj *data.Object, //  : object with new RC and previous last access time
	err error, //         : error if any
) {

	err = b.do(func(objs *bolt.Bucket) (err error) {

		var (
			now     = time.Now()
			created bool
			access  time.Time
		)

		if obj, err = getObject(objs, key); err != nil {
			if err != data.ErrNotFound {
				return // DB failure
			}

			created = true
			obj = new(data.Object)
			obj.Create = now
			obj.Access = time.Unix(0, 0)
		}

		access = obj.Access // last access or 0 nano since epoch

		obj.RC += incrBy
		obj.Val = val
		obj.Access = now

		if err = setObject(objs, key, obj); err != nil {
			return
		}

		obj.Access = access
		b.changeStatAfter(created, obj.RC, incrBy, vol(val))
		return
	})
	return
}

// SetNotTouch is the same as the Set but it
// doesn't update last access time.
func (b *Bolt) SetNotTouch(
	key cipher.SHA256, val []byte,
) (*data.Object, error) {
	return b.SetIncrNotTouch(key, val, 1)
}

// SetIncrNotTouch is the same as the SetIncr but
// it doesn't update last access time.
func (b *Bolt) SetIncrNotTouch(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	obj *data.Object, //  : object with new RC and previous last access time
	err error, //         : error if any
) {

	err = b.do(func(objs *bolt.Bucket) (err error) {

		var (
			now     = time.Now()
			created bool
			access  time.Time
		)

		if obj, err = getObject(objs, key); err != nil {
			if err != data.ErrNotFound {
				return // DB failure
			}

			created = true
			obj = new(data.Object)
			obj.Create = now
			obj.Access = time.Unix(0, 0)
		}

		access = obj.Access // last access or 0 nano since epoch

		obj.RC += incrBy
		obj.Val = val

		if created {
			obj.Access = now // set to now only if created
		}

		if err = setObject(objs, key, obj); err != nil {
			return
		}

		obj.Access = access
		b.changeStatAfter(created, obj.RC, incrBy, vol(val))
		return
	})
	return
}

// call under lock
func (b *Bolt) changeStatAfterSetRaw(
	overwritten bool, // : is overwritten
	pvol, prc int64, //  : previous
	vol, rc int64, //    : new
) {
	b.Lock()
	defer b.Unlock()

	// regards to collisons or use of blank value for some developer reasons

	if overwritten == true {
		b.volume.all += (vol - pvol) // diff
		if prc <= 0 {                // was dead
			if rc > 0 { // reborn
				b.amount.used++
				b.volume.used += vol
			}
			// else -> still dead (do nothing)
		} else { // was alive
			if rc <= 0 { // kill
				b.amount.used--
				b.volume.used -= pvol
			} else { // still alive
				b.volume.used += (vol - pvol) // diff
			}
		}
	} else { // new object created
		b.volume.all += vol
		b.amount.all++
		if rc > 0 { // alive object
			b.volume.used += vol
			b.amount.used++
		}
	}

}

// SetRaw sets given object as is. If object alreday exists,
// then the SetRaw method overwrites existing one.
func (b *Bolt) SetRaw(key cipher.SHA256, obj *data.Object) (err error) {

	err = b.do(func(objs *bolt.Bucket) (err error) {

		var (
			o               *data.Object
			overwritten     bool
			prevVol, prevRC int64
		)

		if o, err = getObject(objs, key); err != nil {
			if err != data.ErrNotFound {
				return // DB failure
			}
		} else {
			overwritten = true
			prevVol, prevRC = vol(o.Val), o.RC
		}
		if err = setObject(objs, key, obj); err != nil {
			return
		}

		b.changeStatAfterSetRaw(overwritten, prevVol, prevRC,
			vol(obj.Val), obj.RC)
		return
	})
	return
}

// Incr methods used to change RC of an object
// returning new RC or error if any. If object
// doesn't exist then Incr methods return
// ErrNotFound error.
//
// Incr inc- or decrements RC of object with given
// key using provided value. The Incr returns new
// RC or error if any.
func (b *Bolt) Incr(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	rc int64, //          : new RC
	access time.Time, //  : previous last access time
	err error, //         : error if any
) {

	err = b.do(func(objs *bolt.Bucket) (err error) {

		var obj *data.Object
		if obj, err = getObject(objs, key); err != nil {
			return
		}

		access = obj.Access
		obj.Access = time.Now() // touch

		obj.RC += incrBy
		rc = obj.RC

		b.changeStatAfter(false, rc, incrBy, vol(obj.Val))
		return setObject(objs, key, obj)
	})
	return
}

// IncrNotTouch is the same as the Incr but it
// doesn't update last access time.
func (b *Bolt) IncrNotTouch(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	rc int64, //          : new RC
	access time.Time, //  : previous last access time
	err error, //         : error if any
) {

	err = b.do(func(objs *bolt.Bucket) (err error) {

		var obj *data.Object
		if obj, err = getObject(objs, key); err != nil {
			return
		}

		access = obj.Access

		obj.RC += incrBy
		rc = obj.RC

		b.changeStatAfter(false, rc, incrBy, vol(obj.Val))
		return setObject(objs, key, obj)
	})
	return
}

func (b *Bolt) changeStatAfterDel(rc, vol int64) {
	b.amount.all--
	b.volume.all -= vol

	if rc > 0 {
		b.amount.used--
		b.volume.used -= vol
	}
}

// Take deletes an object unconditionally returinig:
// (1) deleted object, (2) ErrNotFound if object
// doesn't exist (3) any other error (DB failure,
// for exmple).
func (b *Bolt) Take(key cipher.SHA256) (obj *data.Object, err error) {
	err = b.do(func(objs *bolt.Bucket) (err error) {
		if obj, err = getObject(objs, key); err != nil {
			return
		}
		b.changeStatAfterDel(obj.RC, vol(obj.Val))
		return objs.Delete(key[:])
	})
	return
}

// Del deletes an object unconditionally. The Del
// method returns ErrNotFound error if object doens't
// exist in DB.
func (b *Bolt) Del(key cipher.SHA256) (err error) {
	err = b.do(func(objs *bolt.Bucket) (err error) {
		var obj *data.Object
		if obj, err = getObject(objs, key); err != nil {
			return
		}
		b.changeStatAfterDel(obj.RC, vol(obj.Val))
		return objs.Delete(key[:])
	})
	return
}

// Iterate all keys in CXDS. Use ErrStopIteration to stop
// an iteration. The Iterate method never lock DB and any
// parallel Get-/Set-/Incr-/Del/etc call can be performed
// with call of the Iterate at the same time. But, the
// Iterate can lock DB for time of the IterateKeysFunc
// call.
//
// Iterate never updates last access time.
//
// Iterate can skip new objects, and use deleted objects.
func (b *Bolt) Iterate(iterateFunc data.IterateKeysFunc) (err error) {

	var (
		last cipher.SHA256
		end  bool

		scan = make([]cipher.SHA256, 0, b.scanBy)
	)

	for end == false {
		b.do(func(objs *bolt.Bucket) (_ error) {
			var c = objs.Cursor()
			for i := 0; i < b.scanBy; i++ {
				var key []byte
				if key, _ = c.Seek(last[:]); key == nil {
					end = true
					return
				}
				copy(last[:], key)
				scan = append(scan, last)
				addOne(last[:])
			}
			return
		})

		for _, key := range scan {
			if err = iterateFunc(key); err != nil {
				if err == data.ErrStopIteration {
					err = nil
				}
				return
			}
		}

		scan = scan[:0]
		// continue
	}
	return
}

// Amount of objects. The 'all' means amount of all objects
// and the 'used' is amount of objects with RC greater then
// zero.
func (b *Bolt) Amount() (all, used int64) {
	b.Lock()
	defer b.Unlock()

	return b.amount.all, b.amount.used
}

// Volume of objects. The volume measured
// in bytes. The volume consist of payload
// only and not includes keys and any other
// meta information like references counter
// etc. The 'all' is volume of all objects,
// and the 'used' is volume of objects with
// RC greater then zero.
func (b *Bolt) Volume() (all, used int64) {
	b.Lock()
	defer b.Unlock()

	return b.volume.all, b.volume.used
}

// IsSafeClosed is flag that means that DB has been
// closed successfully last time. If the IsSafeClosed
// returns false, then may be some repair required (it
// depends).
func (b *Bolt) IsSafeClosed() bool {
	return b.isSafeClosed // no locks needed
}

// Close the Bolt
func (b *Bolt) Close() (err error) {
	b.closeo.Do(func() {
		if err = b.setInfo(); err != nil {
			b.b.Close() // drop error
			return
		}
		err = b.b.Close()
	})
	return
}
