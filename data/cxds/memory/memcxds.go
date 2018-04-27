package memory

import (
	"errors"
	"fmt"
	"sync"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

var ErrEmptyValue = errors.New("empty value")

type Memory struct {
	mx  sync.RWMutex
	kvs map[cipher.SHA256]memoryObject

	amountAll  int
	amountUsed int

	voluemAll  int
	volumeUsed int
}

// object stored in memory
type memoryObject struct {
	rc  uint32
	val []byte
}

// NewCXDS creates CXDS-databse in
// memory. The DB based on golang map
func NewCXDS() data.CXDS {
	return &Memory{kvs: make(map[cipher.SHA256]memoryObject)}
}

func (m *Memory) av(rc, nrc uint32, vol int) {

	if rc == 0 { // was dead
		if nrc > 0 { // an be resurrected
			m.amountUsed++
			m.volumeUsed += vol
		}
		return // else -> as is
	}

	// rc > 0 (was alive)

	if nrc == 0 { // an be killed
		m.amountUsed--
		m.volumeUsed -= vol
	}

}

func (m *Memory) incr(
	key cipher.SHA256,
	mo memoryObject,
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
	m.kvs[key] = mo

	m.av(rc, nrc, len(mo.val))
	return

}

// Get value and change rc
func (m *Memory) Get(
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

	if mo, ok := m.kvs[key]; ok {
		val, rc = mo.val, mo.rc
		rc = m.incr(key, mo, rc, inc)
		return
	}
	err = data.ErrNotFound
	return
}

// Set value and change rc
func (m *Memory) Set(
	key cipher.SHA256,
	val []byte,
	inc int,
) (
	rc uint32,
	err error,
) {

	if inc <= 0 {
		panicf("invalid inc argument is Set: %d", inc)
	}

	if len(val) == 0 {
		err = ErrEmptyValue
		return
	}

	m.mx.Lock()
	defer m.mx.Unlock()

	if mo, ok := m.kvs[key]; ok {
		rc = m.incr(key, mo, mo.rc, inc)
		return
	}

	// created

	m.amountAll++
	m.voluemAll += len(val)

	m.amountUsed++
	m.volumeUsed += len(val)

	rc = uint32(inc)
	m.kvs[key] = memoryObject{rc, val}

	return
}

// Inc changes rc
func (m *Memory) Inc(
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

	if mo, ok := m.kvs[key]; ok {
		rc = m.incr(key, mo, mo.rc, inc)
		return
	}

	err = data.ErrNotFound
	return
}

// Del deletes value unconditionally
func (m *Memory) Del(key cipher.SHA256) (_ error) {
	m.mx.Lock()
	defer m.mx.Unlock()

	var mo, ok = m.kvs[key]

	if ok == false {
		return // not found
	}

	if mo.rc > 0 {
		m.amountUsed--
		m.volumeUsed -= len(mo.val)
	}

	m.amountAll--
	m.voluemAll -= len(mo.val)

	return
}

func (m *Memory) unlockedIterate(
	k cipher.SHA256,
	rc uint32,
	v []byte,
	iterateFunc data.IterateObjectsFunc,
) (
	err error,
) {

	m.mx.Unlock()
	defer m.mx.Lock()

	return iterateFunc(k, rc, v)
}

// Iterate all keys
func (m *Memory) Iterate(iterateFunc data.IterateObjectsFunc) (err error) {

	m.mx.Lock()
	defer m.mx.Unlock()

	for k, mo := range m.kvs {
		if err = m.unlockedIterate(k, mo.rc, mo.val, iterateFunc); err != nil {
			if err == data.ErrStopIteration {
				err = nil
			}
			return
		}
	}

	return
}

func (m *Memory) unlockedIterateDel(
	k cipher.SHA256,
	rc uint32,
	v []byte,
	iterateFunc data.IterateObjectsDelFunc,
) (
	del bool,
	err error,
) {

	m.mx.Unlock()
	defer m.mx.Lock()

	return iterateFunc(k, rc, v)
}

// IterateDel all keys deleting
func (m *Memory) IterateDel(
	iterateFunc data.IterateObjectsDelFunc,
) (
	err error,
) {

	m.mx.Lock()
	defer m.mx.Unlock()

	var del bool

	for k, mo := range m.kvs {
		del, err = m.unlockedIterateDel(k, mo.rc, mo.val, iterateFunc)
		if err != nil {
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
func (m *Memory) Amount() (all, used int) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	return m.amountAll, m.amountUsed
}

// Volume of objects
func (m *Memory) Volume() (all, used int) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	return m.voluemAll, m.volumeUsed
}

// IsSafeClosed allways returns true, because
// the DB placed in memory and destroyed after
// closing.
func (*Memory) IsSafeClosed() bool {
	return true
}

// Close DB
func (m *Memory) Close() (_ error) {
	m.mx.Lock()
	defer m.mx.Unlock()

	m.kvs = nil // clear
	return
}

func panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}
