package memory

import (
	"sync"
	"time"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

type stat struct {
	all, used int64
}

type Memory struct {
	sync.Mutex
	kvs            map[cipher.SHA256]*data.Object
	amount, volume stat
	clsoeo         sync.Once
}

// NewMemory creates CXDS-databse in
// memory. The DB based on golang map
func NewMemory() data.CXDS {
	return &Memory{kvs: make(map[cipher.SHA256]*data.Object)}
}

func copyObject(o *data.Object) (obj *data.Object) {
	obj = new(data.Object)
	obj.Val = make([]byte, len(o.Val))
	copy(obj.Val, o.Val)
	obj.RC = o.RC
	obj.Access = o.Access
	obj.Create = o.Create
	return
}

func (m *Memory) changeStatAfter(created bool, rc, incrBy, volume int64) {
	// under lock

	// set methods
	if created == true {
		m.amount.all++
		m.volume.all += volume
		if rc > 0 {
			m.amount.used++
			m.volume.used += volume
		}
		return
	}

	if incrBy == 0 {
		return // no changes
	}
	if rc <= 0 {
		if rc-incrBy > 0 {
			m.amount.used--                // } one of objects,
			m.volume.used -= int64(volume) // }  turns to be not used
		}
		return
	}
	// rc > 0
	if rc-incrBy <= 0 {
		m.amount.used++                // } reborn
		m.volume.used += int64(volume) // }
	}
	return
}

func (m *Memory) Hooks() (hooks data.Hooks) {
	return // nil
}

func (m *Memory) Touch(key cipher.SHA256) (access time.Time, err error) {
	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		err = data.ErrNotFound
		return
	}
	access = obj.Access
	obj.Access = time.Now()
	return
}

func (m *Memory) Get(key cipher.SHA256) (*data.Object, error) {
	return m.GetIncr(key, 0)
}

func (m *Memory) GetIncr(
	key cipher.SHA256, incrBy int64,
) (*data.Object, error) {

	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		return nil, data.ErrNotFound
	}
	obj.RC += incrBy

	var cp = copyObject(obj)
	obj.Access = time.Now()

	m.changeStatAfter(false, obj.RC, incrBy, int64(len(obj.Val)))
	return cp, nil
}

func (m *Memory) GetNotTouch(key cipher.SHA256) (*data.Object, error) {
	return m.GetIncrNotTouch(key, 0)
}

//
func (m *Memory) GetIncrNotTouch(
	key cipher.SHA256, incrBy int64,
) (*data.Object, error) {

	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		return nil, data.ErrNotFound
	}

	obj.RC += incrBy
	m.changeStatAfter(false, obj.RC, incrBy, int64(len(obj.Val)))
	return copyObject(obj), nil
}

// Set is SetIncr(key, val, 1)
func (m *Memory) Set(key cipher.SHA256, val []byte) (*data.Object, error) {
	return m.SetIncr(key, val, 1)
}

// SetIncr ... blah
func (m *Memory) SetIncr(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	*data.Object, //      : object with new RC and previous last access time
	error, //             : error if any
) {
	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		obj = new(data.Object)
		m.kvs[key] = obj
	}

	obj.Val = val
	obj.RC += incrBy

	var cp = copyObject(obj)
	obj.Access = time.Now()

	if ok == false {
		obj.Create = obj.Access

		cp.Access = obj.Access
		cp.Create = obj.Access
	}

	cp.Val, obj.Val = obj.Val, cp.Val // swap (copy in DB, argument in reply)

	m.changeStatAfter(false, obj.RC, incrBy, int64(len(val)))
	return cp, nil
}

// SetNotTouch ... balh
func (m *Memory) SetNotTouch(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
) (
	*data.Object, //      : object with new RC and previous last access time
	error, //             : error if any
) {
	return m.SetIncrNotTouch(key, val, 1)
}

// SetIncrNotTouch ... blah
func (m *Memory) SetIncrNotTouch(
	key cipher.SHA256, // : hash of the object
	val []byte, //        : encoded object
	incrBy int64, //      : inc- or decrement RC by this value
) (
	*data.Object, //      : object with new RC and previous last access time
	error, //             : error if any
) {
	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		obj = new(data.Object)
		obj.Val = val
		obj.RC = incrBy
		obj.Access = time.Now() // access time can't be less then create time
		obj.Create = obj.Access
		m.kvs[key] = obj
		m.changeStatAfter(true, obj.RC, incrBy, int64(len(val)))
	} else {
		obj.Val = val
		obj.RC += incrBy
		m.changeStatAfter(false, obj.RC, incrBy, int64(len(val)))
	}

	var cp = copyObject(obj)
	cp.Val, obj.Val = obj.Val, cp.Val // swap (copy in DB, argument in reply)
	return cp, nil
}

// call under lock
func (m *Memory) changeStatAfterSetRaw(
	overwritten bool, // : is overwritten
	pvol, prc int64, //  : previous
	vol, rc int64, //    : new
) {

	// regards to collisons or use of blank value for some developer reasons

	if overwritten == true {
		m.volume.all += (vol - pvol) // diff
		if prc <= 0 {                // was dead
			if rc > 0 { // reborn
				m.amount.used++
				m.volume.used += vol
			}
			// else -> still dead (do nothing)
		} else { // was alive
			if rc <= 0 { // kill
				m.amount.used--
				m.volume.used -= pvol
			} else { // still alive
				m.volume.used += (vol - pvol) // diff
			}
		}
	} else { // new object created
		m.volume.all += vol
		m.amount.all++
		if rc > 0 { // alive object
			m.volume.used += vol
			m.amount.used++
		}
	}

}

// SetRaw .. .blah
func (m *Memory) SetRaw(key cipher.SHA256, obj *data.Object) (err error) {
	m.Lock()
	defer m.Unlock()

	var o, ok = m.kvs[key]
	if ok == false {
		m.kvs[key] = copyObject(obj)
		m.changeStatAfterSetRaw(false, 0, 0, int64(len(obj.Val)), obj.RC)
		return
	}
	m.kvs[key] = copyObject(obj)
	m.changeStatAfterSetRaw(true, int64(len(o.Val)), o.RC,
		int64(len(obj.Val)), obj.RC)
	return
}

// Incr .. blah
func (m *Memory) Incr(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	rc int64, //          : new RC
	access time.Time, //  : previous last access time
	err error, //         : error if any
) {
	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		err = data.ErrNotFound
		return
	}

	obj.RC += incrBy
	m.changeStatAfter(false, obj.RC, incrBy, int64(len(obj.Val)))

	rc = obj.RC
	access = obj.Access
	obj.Access = time.Now()
	return
}

// IncrNotTouch ... blah
func (m *Memory) IncrNotTouch(
	key cipher.SHA256, // : hash of the object
	incrBy int64, //      : inr- or decrement by
) (
	rc int64, //          : new RC
	access time.Time, //  : previous last access time
	err error, //         : error if any
) {
	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		err = data.ErrNotFound
		return
	}

	obj.RC += incrBy
	m.changeStatAfter(false, obj.RC, incrBy, int64(len(obj.Val)))

	rc = obj.RC
	access = obj.Access
	return
}

func (m *Memory) changeStatAfterDel(rc, vol int64) {
	m.amount.all--
	m.volume.all -= vol

	if rc > 0 {
		m.amount.used--
		m.volume.used -= vol
	}
}

// Take gets value from DB and deletes. Short words, the Take is
// the same as Get and Del.
func (m *Memory) Take(key cipher.SHA256) (obj *data.Object, err error) {
	m.Lock()
	defer m.Unlock()

	var ok bool
	if obj, ok = m.kvs[key]; ok == false {
		err = data.ErrNotFound
		return
	}
	m.changeStatAfterDel(obj.RC, int64(len(obj.Val)))
	return
}

// Del value by key. If value doesn't exists, then
// the Del retusn data.ErrNotFound
func (m *Memory) Del(key cipher.SHA256) (err error) {
	m.Lock()
	defer m.Unlock()

	var obj, ok = m.kvs[key]
	if ok == false {
		return data.ErrNotFound
	}
	delete(m.kvs, key)
	m.changeStatAfterDel(obj.RC, int64(len(obj.Val)))
	return
}

func (m *Memory) unlockedIterate(
	key cipher.SHA256,
	iterateFunc data.IterateKeysFunc,
) (
	err error,
) {
	m.Unlock()
	defer m.Lock()

	return iterateFunc(key)
}

// Iterate over all keys.
func (m *Memory) Iterate(iterateFunc data.IterateKeysFunc) (err error) {

	m.Lock()
	defer m.Unlock()

	for key := range m.kvs {
		if err = m.unlockedIterate(key, iterateFunc); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}
	}

	return
}

// Amount of objects
func (m *Memory) Amount() (all, used int64) {
	m.Lock()
	defer m.Unlock()

	return m.amount.all, m.amount.used
}

// Volume of objects. Payload only.
func (m *Memory) Volume() (all, used int64) {
	m.Lock()
	defer m.Unlock()

	return m.volume.all, m.volume.used
}

// IsSafeClosed returns true allways.
func (*Memory) IsSafeClosed() bool {
	return true
}

// Map returns underlying map. Use with Lock/Unlock to
// protect DB against concurent use
func (m *Memory) Map() map[cipher.SHA256]*data.Object {
	return m.kvs
}

// Close DB. After closing DB can't be used.
func (m *Memory) Close() (_ error) {
	m.Lock()
	defer m.Unlock()

	m.clsoeo.Do(func() {
		m.kvs = nil // clear
	})
	return
}
