package redis

import (
	"bufio"
	"fmt"
	"strconv"
	"time"

	"github.com/mediocregopher/radix.v3/resp"

	"github.com/skycoin/cxo/data"
)

type statReply struct {
	amountAll  int64
	amountUsed int64
	volumeAll  int64
	volumeUsed int64
}

func (s *statReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var ah resp.ArrayHeader
	if err = ah.UnmarshalRESP(r); err != nil {
		return
	}
	if ah.N != 4 {
		err = fmt.Errorf("invalid response length %d, wnat 4", ah.N)
	}

	var fields = [4]*int64{
		&s.amountAll,
		&s.amountUsed,
		&s.volumeAll,
		&s.volumeUsed,
	}
	for i := 0; i < 4; i++ {
		var s resp.BulkString
		if err = s.UnmarshalRESP(r); err != nil {
			return
		}
		if s.S == "" {
			*(fields[i]) = 0
			continue
		}
		var i64 int64
		if i64, err = strconv.ParseInt(s.S, 10, 64); err != nil {
			return
		}
		*(fields[i]) = i64
	}
	return
}

type touchReply struct {
	Exists resp.Int
	Access resp.Int
}

func (t *touchReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	// for {
	// 	s, err := r.ReadString('\n')
	// 	if err != nil {
	// 		println("error:", err.Error())
	// 	}
	// 	println("string:", s)
	// }
	var ah resp.ArrayHeader
	if err = ah.UnmarshalRESP(r); err != nil {
		return
	}
	if ah.N != 2 {
		return fmt.Errorf("invalid response length %d, want 2", ah.N)
	}
	if err = t.Exists.UnmarshalRESP(r); err != nil {
		return
	}
	err = t.Access.UnmarshalRESP(r)
	return
}

type object struct {
	Exists resp.Int             // bool
	Val    resp.BulkStringBytes // []byte
	RC     resp.Int             // int64
	Access resp.Int             // int64
	Create resp.Int             // int64
}

func (o *object) UnmarshalRESP(r *bufio.Reader) (err error) {

	var ah resp.ArrayHeader
	if err = ah.UnmarshalRESP(r); err != nil {
		return
	}

	if ah.N != 5 {
		return fmt.Errorf("invalid resposne length %d, want 5", ah.N)
	}

	if err = o.Exists.UnmarshalRESP(r); err != nil {
		return
	}
	if err = o.Val.UnmarshalRESP(r); err != nil {
		return
	}
	if err = o.RC.UnmarshalRESP(r); err != nil {
		return
	}
	if err = o.Access.UnmarshalRESP(r); err != nil {
		return
	}
	err = o.Create.UnmarshalRESP(r)
	return

}

// return data.Object or nil if not exist
func (o *object) Object() (obj *data.Object) {

	if o.Exists.I == 0 {
		return
	}

	obj = new(data.Object)
	obj.Val = o.Val.B
	obj.RC = o.RC.I
	obj.Access = time.Unix(0, o.Access.I)
	obj.Create = time.Unix(0, o.Create.I)
	return
}

type setReply struct {
	rc     int64 // overwritten
	access int64 // prev_vol
	create int64 // prev_rc
}

func (s *setReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var ah resp.ArrayHeader
	if err = ah.UnmarshalRESP(r); err != nil {
		return
	}
	if ah.N != 3 {
		return fmt.Errorf("invalid response length: %d, want 3", ah.N)
	}
	for _, field := range []*int64{&s.rc, &s.access, &s.create} {
		var i resp.Int
		if err = i.UnmarshalRESP(r); err != nil {
			return
		}
		*field = i.I // set
	}
	return
}

func (s *setReply) RC() int64 {
	return s.rc
}

func (s *setReply) Access() time.Time {
	return time.Unix(0, s.access)
}

func (s *setReply) Create() time.Time {
	return time.Unix(0, s.create)
}

// SetRaw reply
func (s *setReply) overwritten() bool {
	return s.rc == 1
}

// SetRaw reply
func (s *setReply) prevVol() int64 {
	return s.access
}

// SetRaw reply
func (s *setReply) prevRC() int64 {
	return s.create
}
