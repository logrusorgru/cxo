package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/mediocregopher/radix.v3"
	"github.com/mediocregopher/radix.v3/resp"
	"github.com/skycoin/skycoin/src/cipher"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	var pool, err = radix.NewPool("tcp", "127.0.0.1:6379", 10)
	if err != nil {
		log.Print(err)
		return
	}
	defer pool.Close()

	fmt.Println("getSafeClosed not exist")
	var safeClosed bool
	if safeClosed, err = getSafeClosed(pool); err != nil {
		log.Print(err)
		return
	}
	fmt.Println("safeClosed:", safeClosed)

	for _, t := range []bool{false, true} {

		fmt.Println("setSafeClosed to", t)
		if err = setSafeClosed(pool, t); err != nil {
			log.Print(err)
			return
		}
		if safeClosed, err = getSafeClosed(pool); err != nil {
			log.Print(err)
			return
		}
		fmt.Println("safeClosed:", safeClosed, "want", t)

	}

	var (
		value = []byte("something useful")
		key   = cipher.SumSHA256(value)
	)

	fmt.Println("getReadOnly")
	var obj *Object
	if obj, err = getReadOnly(pool, key); err != nil {
		log.Print(err)
		return
	}
	fmt.Printf("getReadOnly: %#v\n", obj)

}

//go:generate msgp
type Object struct {
	Value      []byte    // encoded object
	RC         uint32    // references counter
	AccessTime time.Time //
	CreateTime time.Time //
}

func (o *Object) UnmarshalRESP(r *bufio.Reader) (err error) {
	fmt.Println(r.ReadString('\n'))
	fmt.Println(r.ReadString('\n'))
	fmt.Println(r.ReadString('\n'))
	fmt.Println(r.ReadString('\n'))
	fmt.Println(r.ReadString('\n'))
	return
}

func setSafeClosed(pool *radix.Pool, safeClosed bool) (err error) {
	err = pool.Do(radix.FlatCmd(nil, "SET", "safeClosed", safeClosed))
	return
}

func getSafeClosed(pool *radix.Pool) (safeClosed bool, err error) {
	err = pool.Do(radix.FlatCmd(&safeClosed, "GET", "safeClosed"))
	return
}

// read-only Get
func getObjectReadOnly(
	pool *radix.Pool,
	key cipher.SHA256,
) (
	obj *Object,
	err error,
) {

	obj = new(Object)
	err = pool.Do(radix.FlatCmd(obj, "HMGET", key.Hex(),
		"val", "rc", "access", "create"))
	return
}

type redisTime struct {
	T time.Time
}

func (r *redisTime) MarshalRESP(w io.Writer) (err error) {
	var unixNano resp.Int
	unixNano.I = r.T.UnixNano()
	return unixNano.MarshalRESP(w)
}

func (r *redisTime) UnmarshalRESP(b *bufio.Reader) (err error) {

	var unixNano resp.Int
	if err = unixNano.UnmarshalRESP(b); err != nil {
		return
	}

	r.T = time.Unix(0, unixNano.I)
	return
}

type redisTwoTimes struct {
	Access redisTime
	Create redisTime
}

func (r *redisTwoTimes) UnmarshalRESP(b *bufio.Reader) (err error) {

	var ah resp.ArrayHeader

	if err = ah.UnmarshalRESP(b); err != nil {
		return
	}

	if ah.N != 2 {
		return errors.New("invalid response")
	}

	if err = r.Access.UnmarshalRESP(b); err != nil {
		return
	}

	err = r.Create.UnmarshalRESP(b)
	return
}

func setObject(
	pool *radix.Pool,
	key cipher.SHA256,
	val []byte,
	inc int,
) (
	obj *Object,
	err error,
) {

	var (
		keyHex = key.Hex()
		now    = time.Now()

		tt       redisTwoTimes
		redisInc resp.Int
	)

	err = pool.Do(radix.Pipeline(
		radix.Cmd(nil, "MULTI"),
		radix.FlatCmd(&tt, "HMGET", keyHex, "access", "create"),
		radix.Cmd(nil, "HMSET", keyHex, "val", val, "access", now.UnixNano()),
		radix.Cmd(nil, "HSETNX", keyHex, "create", now.UnixNano()),
		radix.Cmd(&redisInc, "HINCRBY", keyHex, "inc", inc), // + or -
		radix.Cmd(nil, "EXEC"),
	))

	if err != nil {
		return
	}

	obj = new(Object)
	obj.Value = val              // as it was
	obj.RC = redisInc.I          // new value
	obj.AccessTime = tt.Access.T // last access time
	obj.CreateTime = tt.Create.T // created at

	if obj.CreateTime.IsZero() == true {
		obj.CreateTime = now // just now
	}

	return
}

/*



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

*/
