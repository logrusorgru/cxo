package redis

import (
	"bufio"
	"fmt"
	"strconv"
	"time"

	"github.com/mediocregopher/radix.v3/resp"

	"github.com/skycoin/cxo/data"
)

func parseArrayHeader(r *bufio.Reader) (n int, err error) {
	var ah resp.ArrayHeader
	if err = ah.UnmarshalRESP(r); err != nil {
		return
	}
	n = ah.N
	return
}

func parseBool(r *bufio.Reader) (t bool, err error) {
	var val resp.Int
	if err = val.UnmarshalRESP(r); err != nil {
		return
	}
	t = (val.I != 0)
	return
}

func parseInt64(r *bufio.Reader) (n int64, err error) {
	var val resp.BulkString
	if err = val.UnmarshalRESP(r); err != nil {
		return
	}
	if val.S == "" {
		return // (0, nil)
	}
	n, err = strconv.ParseInt(val.S, 10, 64)
	return
}

func parseTime(r *bufio.Reader) (tm time.Time, err error) {
	var n int64
	if n, err = parseInt64(r); err != nil {
		return
	}
	tm = time.Unix(0, n)
	return
}

// getStat
type statReply struct {
	AmountAll  int64
	AmountUsed int64
	VolumeAll  int64
	VolumeUsed int64
}

func (s *statReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var n int
	if n, err = parseArrayHeader(r); err != nil {
		return
	}
	if n != 4 {
		err = fmt.Errorf("invalid response length %d, wnat 4", n)
	}
	for _, field := range [4]*int64{
		&s.AmountAll,
		&s.AmountUsed,
		&s.VolumeAll,
		&s.VolumeUsed,
	} {
		var i64 int64
		if i64, err = parseInt64(r); err != nil {
			return
		}
		*field = i64
	}
	return
}

// Touch
type touchReply struct {
	Exists bool
	Access time.Time
}

func (t *touchReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var n int
	if n, err = parseArrayHeader(r); err != nil {
		return
	}
	if n != 2 {
		return fmt.Errorf("invalid response length %d, want 2", n)
	}
	if t.Exists, err = parseBool(r); err != nil {
		return
	}
	t.Access, err = parseTime(r)
	return
}

// Get, GetIncr, GetNotTouch, GetIncrNotTouch
type getReply struct {
	Exists bool
	Val    []byte
	RC     int64
	Access time.Time
	Create time.Time
}

func (g *getReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var n int
	if n, err = parseArrayHeader(r); err != nil {
		return
	}
	if n != 5 {
		return fmt.Errorf("invalid resposne length %d, want 5", n)
	}
	if g.Exists, err = parseBool(r); err != nil {
		return
	}
	var val resp.BulkStringBytes
	if err = val.UnmarshalRESP(r); err != nil {
		return
	}
	g.Val = val.B
	if g.RC, err = parseInt64(r); err != nil {
		return
	}
	if g.Access, err = parseTime(r); err != nil {
		return
	}
	g.Create, err = parseTime(r)
	return
}

// return data.Object or nil if not exist
func (g *getReply) Object() (obj *data.Object) {
	if g.Exists == false {
		return
	}
	obj = new(data.Object)
	obj.Val = g.Val
	obj.RC = g.RC
	obj.Access = g.Access
	obj.Create = g.Create
	return
}

// Set, SetIncr, SetNotTouch, SetIncrNotTouch
type setReply struct {
	Created bool
	RC      int64
	Access  time.Time
	Create  time.Time
}

func (s *setReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var n int
	if n, err = parseArrayHeader(r); err != nil {
		return
	}
	if n != 4 {
		return fmt.Errorf("invalid response length: %d, want 3", n)
	}
	if s.Created, err = parseBool(r); err != nil {
		return
	}
	if s.RC, err = parseInt64(r); err != nil {
		return
	}
	if s.Access, err = parseTime(r); err != nil {
		return
	}
	s.Create, err = parseTime(r)
	return
}

// SetRaw
type setRawReply struct {
	Overwritten bool
	PrevVol     int64
	PrevRC      int64
}

func (s *setRawReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var n int
	if n, err = parseArrayHeader(r); err != nil {
		return
	}
	if n != 3 {
		return fmt.Errorf("invalid response length: %d, want 3", n)
	}
	if s.Overwritten, err = parseBool(r); err != nil {
		return
	}
	if s.PrevVol, err = parseInt64(r); err != nil {
		return
	}
	s.PrevRC, err = parseInt64(r)
	return
}

type incrReply struct {
	Exists bool
	Vol    int64 // int52
	RC     int64
	Access time.Time
}

func (i *incrReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var n int
	if n, err = parseArrayHeader(r); err != nil {
		return
	}
	if n != 4 {
		return fmt.Errorf("invalid response length: %d, want 4", n)
	}
	if i.Exists, err = parseBool(r); err != nil {
		return
	}
	if i.Vol, err = parseInt64(r); err != nil {
		return
	}
	if i.RC, err = parseInt64(r); err != nil {
		return
	}
	i.Access, err = parseTime(r)
	return
}

type delReply struct {
	Deleted bool
	Vol     int64
	RC      int64
}

func (d *delReply) UnmarshalRESP(r *bufio.Reader) (err error) {
	var n int
	if n, err = parseArrayHeader(r); err != nil {
		return
	}
	if n != 3 {
		return fmt.Errorf("invalid response length: %d, want 3", n)
	}
	if d.Deleted, err = parseBool(r); err != nil {
		return
	}
	if d.Vol, err = parseInt64(r); err != nil {
		return
	}
	d.RC, err = parseInt64(r)
	return
}
