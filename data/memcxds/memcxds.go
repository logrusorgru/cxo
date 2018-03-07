package memcxds

import (
	"sync"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

// object stored in memory
type memoryObject struct {
	hash cipher.SHA256 // key

	rc  uint32 // rc
	val []byte // value

	next *memoryObject // next element
}

type memoryCXDS struct {
	mx sync.RWMutex

	length int
	list   *memoryObject

	amountAll  int
	amountUsed int

	voluemAll  int
	volumeUsed int
}

// under lock
func (m *memoryCXDS) set(mo *memoryObject) {

	if m.length == 0 {
		m.length++
		m.list = mo
		return
	}

	for el := m.list; el != nil; el = el.next {
		if el.hash == mo.hash {
			//
		}
	}

}

// NewCXDS creates CXDS-databse in memory. The
// DB based on ordered doubly-linked list.
func NewCXDS() data.CXDS {
	return &memoryCXDS{}
}

func (m *memoryCXDS) av(rc, nrc uint32, vol int) {

	if rc == 0 { // was dead
		if nrc > 0 { // resurrected
			m.amountUsed++
			m.volumeUsed += vol
		}
		return // else -> as is
	}

	// rc > 0 (was alive)

	if nrc == 0 { // killed
		m.amountUsed--
		m.volumeUsed -= vol
	}

}

func (m *memoryCXDS) incr(
	key cipher.SHA256,
	mo *memoryObject,
	rc uint32,
	inc int,
) (
	nrc uint32,
) {

	switch {
	case inc == 0:
		nrc = rc // no changes
		return
	case inc < 0:
		inc = -inc // change the sign

		if uinc := uint32(inc); uinc >= rc {
			nrc = 0
		} else {
			nrc = rc - uinc
		}
	case inc > 0:
		nrc = rc + uint32(inc)
	}

	mo.rc = nrc
	m.kvs.Replace(key, mo) // m.kvs[key] = mo

	m.av(rc, nrc, len(mo.val))
	return

}

// Get value and change rc
func (m *memoryCXDS) Get(
	key cipher.SHA256,
	inc int,
) (
	val []byte,
	rc uint32,
	err error,
) {

	if inc == 0 { // read only
		m.mx.RLock()
		defer m.mx.RUnlock()
	} else { // read-write
		m.mx.Lock()
		defer m.mx.Unlock()
	}

	var moi, ok = m.kvs.Get(key)

	if ok == true {
		var mo = moi.(memoryObject)
		val, rc = mo.val, mo.rc
		rc = m.incr(key, mo, rc, inc)
		return
	}

	// if mo, ok := m.kvs[key]; ok {
	// 	val, rc = mo.val, mo.rc
	// 	rc = m.incr(key, mo, rc, inc)
	// 	return
	// }

	err = data.ErrNotFound
	return
}

// Set value and change rc
func (m *memoryCXDS) Set(
	key cipher.SHA256,
	val []byte,
	inc int,
) (
	rc uint32,
	err error,
) {

	if inc <= 0 {
		panicf("negative inc argument in Set: %d", inc)
	}

	if len(val) == 0 {
		err = ErrEmptyValue
		return
	}

	m.mx.Lock()
	defer m.mx.Unlock()

	var moi, ok = m.kvs.Get(key)

	if ok == true {
		var mo = moi.(memoryObject)
		rc = m.incr(key, mo, mo.rc, inc)
		return
	}

	// if mo, ok := m.kvs[key]; ok {
	// 	rc = m.incr(key, mo, mo.rc, inc)
	// 	return
	// }

	// created

	m.amountAll++
	m.voluemAll += len(val)

	m.amountUsed++
	m.volumeUsed += len(val)

	rc = uint32(inc)
	m.kvs.Insert(key, memoryObject{rc, val})
	// m.kvs[key] = memoryObject{rc, val}

	return
}

// Inc changes rc
func (m *memoryCXDS) Inc(
	key cipher.SHA256,
	inc int,
) (
	rc uint32,
	err error,
) {

	if inc == 0 { // presence check
		m.mx.RLock()
		defer m.mx.RUnlock()
	} else { // changes
		m.mx.Lock()
		defer m.mx.Unlock()
	}

	var moi, ok = m.kvs.Get(key)

	if ok == true {
		var mo = moi.(memoryObject)
		rc = m.incr(key, mo, mo.rc, inc)
		return
	}

	// if mo, ok := m.kvs[key]; ok {
	// 	rc = m.incr(key, mo, mo.rc, inc)
	// 	return
	// }

	err = data.ErrNotFound
	return
}

// Del deletes value unconditionally
func (m *memoryCXDS) Del(key cipher.SHA256) (_ error) {
	m.mx.Lock()
	defer m.mx.Unlock()

	var moi, ok = m.kvs.Get(key)
	// var mo, ok = m.kvs[key]

	if ok == false {
		return // not found
	}

	var mo = moi.(memoryObject)

	if mo.rc > 0 {
		m.amountUsed--
		m.volumeUsed -= len(mo.val)
	}

	m.amountAll--
	m.voluemAll -= len(mo.val)

	m.kvs.Delete(key)
	return
}

func isErrNoValues(err error) bool {
	return err.Error() ==
		"No values found that were equal to or within the given bounds."
}

// 8x4 = 32, the lastHash is cipher.SHA256 every bit of which set to 1
const lastHash = cipher.SHA256{
	0xff, 0xff, 0xff, 0xff, // 1
	0xff, 0xff, 0xff, 0xff, // 2
	0xff, 0xff, 0xff, 0xff, // 3
	0xff, 0xff, 0xff, 0xff, // 4
	0xff, 0xff, 0xff, 0xff, // 5
	0xff, 0xff, 0xff, 0xff, // 6
	0xff, 0xff, 0xff, 0xff, // 7
	0xff, 0xff, 0xff, 0xff, // 8
}

// decrement since
func decSince(b []byte) {

	var zero cipher.SHA256

	if bytes.Compare(b, zero[:]) == 0 {
		copy(b, lastHash[:])
		return
	}

	// from tail
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == 0 {
			continue // decrement next byte
		}
		b[i]--
		return
	}
}

// Iterate all keys
func (m *memoryCXDS) Iterate(
	since cipher.SHA256, //                 : starting from
	iterateFunc data.IterateObjectsFunc, // : iterator
) (
	err error, //                           : an error
) {

	m.mx.Lock()
	defer m.mx.Unlock()

	var smIter = func(rec sortedmap.Record) (cont bool) {

		var (
			key = rec.Key.(cipher.SHA256)
			mo  = rec.Val.(memoryObject)
		)

		err = iterateFunc(key, mo.rc, mo.val)

		// continue if the err is nil
		return err != nil
	}

	// (1) from the since to the end (all inclusive)
	// (2) from the beginning to the since (exclusive the since)

	// (1)

	defer func() {
		if err == data.ErrStopIteration {
			err = nil // reset the service error
		}
	}()

	var smErr = m.kvs.BoundedIterFunc(false, since, nil, smIter)

	// sortedmap error, ignore it, if it is "no values" error
	if smErr != nil && isErrNoValues(smErr) == false {
		return smErr
	}

	if err != nil {
		return // erro returned by iterateFunc
	}

	// (2)

	decSince(since[:]) // exclude the since from the pass
	smErr = m.kvs.BoundedIterFunc(false, nil, since, smIter)

	// sortedmap error, ignore it if it is "no values" error
	if smErr != nil && isErrNoValues(smErr) == false {
		return smErr
	}

	// for k, mo := range m.kvs {
	// 	if err = iterateFunc(k, mo.rc, mo.val); err != nil {
	// 		if err == data.ErrStopIteration {
	// 			err = nil
	// 		}
	// 		return
	// 	}
	// }

	return
}

// IterateDel all keys deleting
func (m *memoryCXDS) IterateDel(
	iterateFunc data.IterateObjectsDelFunc,
) (
	err error,
) {

	m.mx.Lock()
	defer m.mx.Unlock()

	var del bool

	for k, mo := range m.kvs {
		if del, err = iterateFunc(k, mo.rc, mo.val); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}
		if del == true {
			delete(m.kvs, k)
			if mo.rc > 0 {
				m.amountUsed--
				m.volumeUsed -= len(mo.val)
			}
			m.amountAll--
			m.voluemAll -= len(mo.val)
		}
	}

	return
}

// amount of objects
func (m *memoryCXDS) Amount() (all, used int) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	return m.amountAll, m.amountUsed
}

// Volume of objects
func (m *memoryCXDS) Volume() (all, used int) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	return m.voluemAll, m.volumeUsed
}

// Version returns API version
func (*memoryCXDS) Version() int {
	return Version
}

// Close DB
func (m *memoryCXDS) Close() (_ error) {
	m.mx.Lock()
	defer m.mx.Unlock()

	m.kvs = nil // clear
	return
}
