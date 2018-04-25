package rediscxds

import (
	"github.com/mediocregopher/radix.v3"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/skycoin/src/cipher"
)

type redisCXDS struct {
	client       *radix.Pool // conenctions pool
	isSafeClosed bool        // current state
}

// NewCXDS creates CXDS based on Redis
func NewCXDS(
	network string,
	addr string,
	size int,
	opts ...radix.PoolOpt,
) (
	ds data.CXDS,
	err error,
) {

	var pool *radix.Pool
	if pool, err = radix.NewPool(network, addr, size, opts...); err != nil {
		return
	}

	var rc redisCXDS
	rc.client = pool

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

func (r *redisCXDS) setSafeClosed(t bool) (err error) {
	err = r.client.Do(radix.FlatCmd(nil, "SET", "safeClosed", t))
	return
}

func (r *redisCXDS) getSafeClosed() (safeClosed bool, err error) {
	err = r.client.Do(radix.FlatCmd(&safeClosed, "GET", "safeClosed"))
	return
}

// read-only Get
func (r *redisCXDS) get(key cipher.SHA256) (val []byte, rc uint32, err error) {
	//
	err = r.client.Do(radix.FlatCmd(rcv, "HMGET", key.Hex()))
	return
}

func (r *redisCXDS) Get(
	key cipher.SHA256,
	inc int,
) (
	val []byte,
	rc uint32,
	err error,
) {

	if inc == 0 {
		//
	} else {
		//
	}

	return
}

func (r *redisCXDS) Set(
	key cipher.SHA256,
	val []byte,
	inc int,
) (
	rc uint32,
	err error,
) {
	//
	return
}

func (r *redisCXDS) Inc(key cipher.SHA256, inc int) (rc uint32, err error) {
	//
	return
}

func (r *redisCXDS) Iterate(iterateFunc IterateObjectsFunc) (err error) {
	//
	return
}

func (r *redisCXDS) IterateDel(iterateFunc IterateObjectsDelFunc) error {
	//
	return
}

func (r *redisCXDS) Del(key cipher.SHA256) (err error) {
	//
	return
}

func (r *redisCXDS) Amount() (all, used int) {
	//
	return
}

func (r *redisCXDS) Volume() (all, used int) {
	//
	return
}

// IsSafeClosed last time
func (r *redisCXDS) IsSafeClosed() (safeClosed bool) {
	return r.isSafeClosed
}

// Close the CXDS
func (r *redisCXDS) Close() (err error) {

	err = r.client.Do(radix.FlatCmd(nil, "SET", "safeClosed", true))
	if err != nil {
		r.client.Close() // drop error
		return
	}

	return r.client.Close()
}
