package skyobject

import (
	"sync"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/cxo/skyobject/registry"
)

// A Filler implemnets registry.Splitter interface
// and used for filling.
type Filler struct {
	c *Container
	r *registry.Root

	reg *registry.Registry

	rq chan<- cipher.SHA256

	mx   sync.Mutex
	incs map[cipher.SHA256]int

	limit chan struct{} // max

	errq chan error

	await sync.WaitGroup

	closeq chan struct{}
	closeo sync.Once
}

//
// methods of the registry.Splitter
//

func (f *Filler) Registry() (reg *registry.Registry) {
	return f.reg
}

func (f *Filler) Get(key cipher.SHA256) (val []byte, rc uint32, err error) {

	// try to get from DB first
	val, rc, err = f.c.Get(key, 1) // incrementing the rc to hold the object

	if err == nil {
		f.inc(key)
		return
	}

	if err != data.ErrNotFound {
		fatal("DB failure:", err) // fatality
	}

	// not found
	var gc = make(chan Object, 1) // wait for the object

	f.c.Want(key, gc)
	defer f.c.Unwant(key, gc) // to be memory safe

	// requset the object using the rq channel
	if f.requset(key) == false {
		return
	}

	select {
	case obj := <-gc:
		f.inc(key) // increment it first for the realRC
		val, rc = obj.Val, obj.RC
	case <-f.closeq:
		err = ErrTerminated
	}

	return
}

func (f *Filler) Fail(err error) {
	select {
	case f.errq <- err:
	case <-f.closeq:
	}
}

//
// internal methods
//

func (f *Filler) inc(key cipher.SHA256) {
	f.mx.Lock()
	defer f.mx.Unlock()

	f.incs[key]++
}

func (f *Filler) requset(key cipher.SHA256) (ok bool) {
	select {
	case f.rq <- key:
		ok = true
	case <-f.closeq:
	}
	return
}

// Clsoe terminates the Split walking and waits for
// goroutines the split creates
func (f *Filler) Close() {
	f.closeo.Do(func() {
		close(f.closeq)
		f.await.Wait()
	})
}

// Fill given Root returns Filler that fills given
// Root obejct. To request objects, the DB doesn't
// have, given rq channel used. The Fill used by
// the node package to fill Root obejcts. The filler
// must be closed after using
func (c *Container) Fill(
	r *registry.Root, //        : the Root to fill
	rq chan<- cipher.SHA256, // : request object from peers
	maxParall int, //           : max subtrees processing at the same time
) (
	f *Filler, //               : the Filler
) {

	f = new(Filler)

	f.c = c
	f.r = r

	f.rq = rq
	f.incs = make(map[cipher.SHA256]int)

	if maxParall > 0 {
		f.limit = make(chan struct{}, maxParall)
	}

	f.errq = make(chan error, 1)
	f.closeq = make(chan struct{})

	return
}

func (f *Filler) remove() {
	for key, inc := range f.incs {
		f.c.db.CXDS().Inc(key, -inc) // ignore error

		// TOOD (kostyarin): handle error
	}
}

// Acquire is like (sync.WaitGroup).Add(1), but the
// Acquire blocks if goroutines limit reached, and
// the Acquire returns false if the Filler closed
func (f *Filler) Acquire() (ok bool) {

	if f.limit == nil {
		ok = true
		f.await.Add(1)
		return
	}

	select {
	case f.limit <- struct{}{}:
		ok = true
		f.await.Add(1)
	case <-f.closeq:
	}

	return

}

// Release is like (sync.WaitGroup).Done
func (f *Filler) Release() {

	if f.limit == nil {
		f.await.Done()
		return
	}

	<-f.limit
	f.await.Done()

}

// Run the Filler. The Run method blocks
// until finish or first error
func (f *Filler) Run() (err error) {

	if err = f.getRegistry(); err != nil {
		f.remove()
		return
	}

	for _, dr := range f.r.Refs {

		if f.Acquire() == false {
			break
		}

		go dr.Split(f)
	}

	var done = make(chan struct{})

	go func() {
		f.await.Wait() // wait group
		close(done)
	}()

	select {
	case err = <-f.errq:
		f.Close()
		f.remove()
	case <-done:
	}

	return
}

func (f *Filler) getRegistry() (err error) {

	var reg *registry.Registry

	if reg, err = f.c.Registry(f.r.Reg); err != nil {

		if err != data.ErrNotFound {
			return // DB failure or malformed encoded Registry
		}

		if _, _, err = f.Get(cipher.SHA256(f.r.Reg)); err != nil {
			return
		}

		reg, err = f.c.Registry(f.r.Reg)
	}

	f.reg = reg

	return

}
