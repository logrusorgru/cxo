package cxds

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
)

var (
	objsBucket = []byte("o") // objects bucket
	infoBucket = []byte("i") // information bucket
	infoKey    = infoBucket  // information key
)

// pasue iterators and resume them later
type pauseResume struct {
	p chan struct{} // not buffered (pause)
	r chan struct{} // not buffered (resume)
}

func newPauseResume() (pr *pauseResume) {
	pr = new(pauseResume)
	pr.p = make(chan struct{})
	pr.r = make(chan struct{})
	return
}

// force an iterator to pause
func (p *pauseResume) pause() {
	p.p <- struct{}{}
}

// resume a paused iterator
func (p *pauseResume) resume() {
	<-p.r
}

// chek the need to pause
func (p *pauseResume) needPause() (need bool) {
	select {
	case <-p.p:
		return true
	default:
	}
	return // false
}

// waiting for resume
func (p *pauseResume) waitResume() {
	p.r <- struct{}{}
}

// release if paused
func (p *pauseResume) release() {
	select {
	case <-p.p:
		<-p.r
	default:
	}
}

type driveCXDS struct {
	mx sync.Mutex // lock amounts, volumes, etc

	amountAll  int // amount of all objects
	amountUsed int // amount of used objects

	volumeAll  int // volume of all objects
	volumeUsed int // volume of used objects

	isSafeClosed bool // last time closing status

	// iterators must not lock DB and must allow
	// concurent Get/Set/Inc and Del calls; the
	// maps below contains only iterators that
	// have started their transactions
	ix      sync.Mutex                // lock iterators
	roIters map[*pauseResume]struct{} // read-only iterators
	rwIters map[*pauseResume]struct{} // read-write iterators

	// underlying DB
	b *bolt.DB
}

func (d *driveCXDS) addReadOnlyIterator(pr *pauseResume) {
	d.ix.Lock()
	defer d.ix.Unlock()

	d.roIters[pr] = struct{}{}
}

func (d *driveCXDS) delReadOnlyIterator(pr *pauseResume) {
	d.ix.Lock()
	defer d.ix.Unlock()

	delete(d.roIters, pr)
}

func (d *driveCXDS) addReadWriteIterator(pr *pauseResume) {
	d.ix.Lock()
	defer d.ix.Unlock()

	d.rwIters[pr] = struct{}{}
}

func (d *driveCXDS) delReadWriteIterator(pr *pauseResume) {
	d.ix.Lock()
	defer d.ix.Unlock()

	delete(d.rwIters, pr)
}

func (d *driveCXDS) pauseReadWriteIterators() {
	d.ix.Lock()
	defer d.ix.Unlock()

	for iter := range d.rwIters {
		iter.pause()
	}
}

func (d *driveCXDS) resumeReadWriteIterators() {
	d.ix.Lock()
	defer d.ix.Unlock()

	for iter := range d.rwIters {
		iter.pause()
	}
}

func (d *driveCXDS) pauseAllIterators() {
	d.ix.Lock()
	defer d.ix.Unlock()

	for iter := range d.roIters {
		iter.pause()
	}

	for iter := range d.rwIters {
		iter.pause()
	}
}

func (d *driveCXDS) resumeAllIterators() {
	d.ix.Lock()
	defer d.ix.Unlock()

	for iter := range d.roIters {
		iter.resume()
	}

	for iter := range d.rwIters {
		iter.resume()
	}
}

// NewCXDS opens existing CXDS-database
// or creates new by given file name. Underlying
// database is boltdb (github.com/boltdb/bolt).
// E.g. this stores data in filesystem
func NewCXDS(
	fileName string, //    : DB file path
	mode os.FileMode, //   : file mode
	opts *bolt.Options, // : BoltDB options
) (
	ds data.CXDS, //       : CXDS or
	err error, //          : an error
) {

	var created bool // true if the file does not exist

	_, err = os.Stat(fileName)
	created = os.IsNotExist(err)

	if opts == nil {
		opts = &bolt.Options{
			Timeout: time.Millisecond * 500,
		}
	}

	var b *bolt.DB
	b, err = bolt.Open(fileName, mode, opts)

	if err != nil {
		return
	}

	defer func() {

		if err != nil {
			b.Close() // close
			if created == true {
				os.Remove(fileName) // clean up
			}
		}

	}()

	var dr = &driveCXDS{b: b} // wrap

	err = b.Update(func(tx *bolt.Tx) (err error) {

		// first of all, take a look the info bucket

		// if the file has not been created, then
		// version of this DB file seems outdated
		if info := tx.Bucket(infoBucket); info == nil {

			if created == false {
				return errors.New(
					"missing info-bucket (may be old version of file)")
			}

			if info, err = tx.CreateBucket(infoBucket); err != nil {
				return
			}

		}

		_, err = tx.CreateBucketIfNotExists(objsBucket)
		return

	})

	if err != nil {
		return
	}

	if created == true {
		dr.isSafeClosed = true // ok for fresh DB
	} else {
		if err = dr.loadStat(); err != nil {
			return
		}
	}

	ds = dr
	return
}

func (d *driveCXDS) loadStat() (err error) {

	d.mx.Lock()
	defer d.mx.Unlock()

	return d.b.Update(func(tx *bolt.Tx) (err error) {

		var (
			meta metaInfo

			info  = tx.Bucket(infoBucket)
			infob = info.Get(infoKey)
		)

		if len(infob) == 0 {
			return errors.New("missing meta info data")
		}

		if err = meta.decode(infob); err != nil {
			return fmt.Errorf("error decoding meta info: %v", err)
		}

		d.isSafeClosed = meta.IsSafeClosed
		d.amountAll = int(meta.AmountAll)
		d.amountUsed = int(meta.AmountUsed)
		d.volumeAll = int(meta.VolumeAll)
		d.volumeUsed = int(meta.VolumeUsed)

		// and clear the IsSafeClosed flag

		meta.IsSafeClosed = false
		return info.Put(infoKey, meta.encode())
	})
}

func (d *driveCXDS) saveStat() (err error) {

	d.mx.Lock()
	defer d.mx.Unlock()

	var meta metaInfo

	meta.AmountAll = uint32(d.amountAll)
	meta.AmountUsed = uint32(d.amountUsed)
	meta.VolumeAll = uint32(d.volumeAll)
	meta.VolumeUsed = uint32(d.volumeUsed)
	meta.IsSafeClosed = true

	return d.b.Update(func(tx *bolt.Tx) (err error) {
		var info = tx.Bucket(infoBucket)
		return info.Put(infoKey, meta.encode())
	})

}

func (d *driveCXDS) av(rc, nrc uint32, vol int) {

	d.mx.Lock()
	defer d.mx.Unlock()

	if rc == 0 { // was dead
		if nrc > 0 { // and be resurrected
			d.amountUsed++
			d.volumeUsed += vol
		}
		return // else -> as is
	}

	// rc > 0 (was alive)

	if nrc == 0 { // and be killed
		d.amountUsed--
		d.volumeUsed -= vol
	}

}

func (d *driveCXDS) incr(
	o *bolt.Bucket, // : objects
	key []byte, //     : key[:]
	val []byte, //     : value without leading rc (4 bytes)
	rc uint32, //      : existing rc
	inc int, //        : change the rc
) (
	nrc uint32, //     : new rc
	err error, //      : an error
) {

	switch {
	case inc == 0:
		nrc = rc // all done (no changes)
		return
	case inc < 0:
		inc = -inc // change its sign
		if uinc := uint32(inc); uinc >= rc {
			nrc = 0 // zero
		} else {
			nrc = rc - uinc // reduce (rc > 0)
		}
	case inc > 0:
		nrc = rc + uint32(inc) // increase the rc
	}

	var repl = make([]byte, 4, 4+len(val))
	setRefsCount(repl, nrc)
	repl = append(repl, val...)
	err = o.Put(key[:], repl)

	if rc != nrc {
		d.av(rc, nrc, len(val))
	}

	return
}

// priority View (pause iterators and perform the View transaction)
func (d *driveCXDS) pView(txFunc func(*bolt.Tx) error) (err error) {
	d.pauseReadWriteIterators()
	defer d.resumeReadWriteIterators()

	return d.b.View(txFunc)
}

// priority Update (puse all iterators and perform Update transaction)
func (d *driveCXDS) pUpdate(txFunc func(*bolt.Tx) error) (err error) {
	d.pauseAllIterators()
	defer d.resumeAllIterators()

	return d.b.Update(txFunc)
}

//

// Get value by key changing or
// leaving as is references counter
func (d *driveCXDS) Get(
	key cipher.SHA256, // :
	inc int, //           :
) (
	val []byte, //        :
	rc uint32, //         :
	err error, //         :
) {

	var tx = func(tx *bolt.Tx) (err error) {

		var (
			o   = tx.Bucket(objsBucket)
			got = o.Get(key[:])
		)

		if len(got) == 0 {
			return data.ErrNotFound // pass through
		}

		rc = getRefsCount(got)
		val = make([]byte, len(got)-4)
		copy(val, got[4:])

		rc, err = d.incr(o, key[:], val, rc, inc)
		return
	}

	if inc == 0 {
		err = d.pView(tx) // lookup only
	} else {
		err = d.pUpdate(tx) // some changes
	}

	return
}

func panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func (d *driveCXDS) addAll(vol int) {
	d.mx.Lock()
	defer d.mx.Unlock()

	d.amountAll++
	d.volumeAll += vol
}

// Set value and its references counter
func (d *driveCXDS) Set(
	key cipher.SHA256,
	val []byte,
	inc int,
) (
	rc uint32,
	err error,
) {

	if inc <= 0 {
		panicf("invalid inc argument in CXDS.Set: %d", inc)
	}

	if len(val) == 0 {
		err = ErrEmptyValue
		return
	}

	err = d.pUpdate(func(tx *bolt.Tx) (err error) {

		var (
			o   = tx.Bucket(objsBucket)
			got = o.Get(key[:])
		)

		if len(got) == 0 {

			// created
			d.addAll(len(val))

			rc, err = d.incr(o, key[:], val, 0, 1)
			return
		}

		rc, err = d.incr(o, key[:], got[4:], getRefsCount(got), inc)
		return
	})

	return
}

// Inc changes references counter
func (d *driveCXDS) Inc(
	key cipher.SHA256,
	inc int,
) (
	rc uint32,
	err error,
) {

	var tx = func(tx *bolt.Tx) (_ error) {

		var (
			o   = tx.Bucket(objsBucket)
			got = o.Get(key[:])
		)

		if len(got) == 0 {
			return data.ErrNotFound
		}

		rc = getRefsCount(got)

		if inc == 0 {
			return // done
		}

		rc, err = d.incr(o, key[:], got[4:], rc, inc)
		return
	}

	if inc == 0 {
		err = d.pView(tx) // lookup only
	} else {
		err = d.pUpdate(tx) // changes required
	}

	return
}

func (d *driveCXDS) del(rc uint32, vol int) {

	d.mx.Lock()
	defer d.mx.Unlock()

	if rc > 0 {
		d.amountUsed--
		d.volumeUsed -= vol
	}

	d.amountAll--
	d.volumeAll -= vol
}

// Del deletes value unconditionally
func (d *driveCXDS) Del(
	key cipher.SHA256,
) (
	err error,
) {

	err = d.pUpdate(func(tx *bolt.Tx) (err error) {

		var (
			o   = tx.Bucket(objsBucket)
			got = o.Get(key[:])
		)

		if len(got) == 0 {
			return // not found
		}

		if err = o.Delete(key[:]); err != nil {
			return
		}

		d.del(getRefsCount(got), len(got)-4)
		return // nil
	})

	return
}

// Iterate all keys read-only without lock.
func (d *driveCXDS) Iterate(iterateFunc data.IterateObjectsFunc) (err error) {

	var (
		pr = newPauseResume()

		since cipher.SHA256 // object to start from (zero)
		done  bool          //
	)

	defer pr.release() // release the pauseResume if pause needed

	d.addReadOnlyIterator(pr)
	defer d.delReadOnlyIterator(pr)

	for {

		err = d.b.View(func(tx *bolt.Tx) (err error) {

			var c = tx.Bucket(objsBucket).Cursor()

			for k, v := c.Seek(since[:]); k != nil; k, v = c.Next() {

				copy(since[:], k)

				if pr.needPause() == true {
					return // stop the transaction and wait for resume
				}

				err = iterateFunc(since, getRefsCount(v), v[4:])

				if err != nil {
					if err == data.ErrStopIteration {
						err = nil
					}
					return
				}

			}

			done = true // done

			return

		}) // func(tx *bolt.Tx) error

		if err != nil {
			return // break by the error
		}

		if done == false {
			pr.waitResume() // continue the for loop
		} else {
			break // break the for loop (the done is true)
		}

	} // for

	return
}

// IterateDel all keys deleting
func (d *driveCXDS) IterateDel(
	iterateFunc data.IterateObjectsDelFunc,
) (
	err error,
) {

	var (
		pr = newPauseResume()

		since cipher.SHA256 // object to start from (zero)
		done  bool          //
	)

	defer pr.release() // release the pauseResume if pause needed

	d.addReadWriteIterator(pr)
	defer d.delReadWriteIterator(pr)

	for {

		err = d.b.Update(func(tx *bolt.Tx) (err error) {

			var (
				rc  uint32
				c   = tx.Bucket(objsBucket).Cursor()
				del bool
			)

			// Seek instead of the Next, because we allows modifications
			// and the BoltDB requires Seek after mutating

			for k, v := c.Seek(since[:]); k != nil; k, v = c.Seek(since[:]) {

				copy(since[:], k)

				rc = getRefsCount(v)

				if del, err = iterateFunc(since, rc, v[4:]); err != nil {
					if err == data.ErrStopIteration {
						err = nil
					}
					return
				}

				if del == true {
					if err = c.Delete(); err != nil {
						return
					}

					d.del(rc, len(v)-4) // stat
				}

				incSlice(since[:]) // next

				if pr.needPause() == true {
					return // break transaction
				}
			}

			done = true
			return
		}) // func(tx *bolt.Tx) error

		if err != nil {
			return // an error
		}

		if done == false {
			pr.waitResume() // wait for resume
		} else {
			break // break for loop (done)
		}

	} // for

	return
}

// Amount of objects
func (d *driveCXDS) Amount() (all, used int) {
	d.mx.Lock()
	defer d.mx.Unlock()

	return d.amountAll, d.amountUsed
}

// Volume of objects (only values)
func (d *driveCXDS) Volume() (all, used int) {
	d.mx.Lock()
	defer d.mx.Unlock()

	return d.volumeAll, d.volumeUsed
}

// IsSafeClosed returns true if the DB was
// closed successfully last time
func (d *driveCXDS) IsSafeClosed() bool {
	return d.isSafeClosed
}

func isNotOpen(err error) bool {
	return err == bolt.ErrDatabaseNotOpen
}

// Close DB
func (d *driveCXDS) Close() (err error) {

	if err = d.b.Sync(); err != nil {
		if isNotOpen(err) == true {
			return
		}
		d.b.Close() // drop error
		return
	}

	// save stat (amounts and volumes) and set safe-closing flag to true
	if err = d.saveStat(); err != nil {
		if isNotOpen(err) == true {
			return
		}
		d.b.Close() // drop error
		return
	}

	return d.b.Close()
}

func copySlice(in []byte) (got []byte) {
	got = make([]byte, len(in))
	copy(got, in)
	return
}
