package redis

import (
	"bufio"
	"fmt"
	"strconv"
	"time"

	"github.com/skycoin/cxo/data"

	"github.com/mediocregopher/radix.v3/resp"
)

func respArrayHead(b *bufio.Reader) (n int, err error) {
	var ah resp.ArrayHeader
	if err = ah.UnmarshalRESP(b); err != nil {
		return
	}
	n = int(ah.N)
	return
}

func respBool(b *bufio.Reader) (t bool, err error) {
	var i resp.Int
	if err = i.UnmarshalRESP(b); err != nil {
		return
	}
	t = (i != 0)
	return
}

func respTime(b *bufio.Reader) (t time.Time, err error) {
	var s resp.BulkString
	if err = s.UnmarshalRESP(b); err != nil {
		return
	}
	var i int64
	if i, err = strconv.ParseInt(s, 10, 64); err != nil {
		panic(err) // must not happen
	}
	t = time.Unix(0, i)
	return
}

type rangeRootsReply struct {
	HasFeed bool
	HasHead bool
	Seqs    []uint64
}

func (r *rangeRootsReply) UnmarshalRESP(b *bufio.Reader) error {

	if n, err := respArrayHead(b); err != nil {
		return err
	} else if n != 3 {
		return fmt.Errorf("invalid response length %d, want 3", n)
	}

	for _, bp := range []*bool{
		&r.HasFeed,
		&r.HasHead,
	} {
		if *bp, err = respBool(b); err != nil {
			return
		}
	}

	var seqs = resp.Any{
		I: []uint64{},
	}

	if err = seqs.UnmarshalRESP(b); err != nil {
		return
	}

	r.Seqs = seqs
	return
}

type setRootReply struct {
	HasFeed bool
	HasHead bool
	Created bool
	Access  time.Time
	Create  time.Time
}

func (s *setRootReply) UnmarshalRESP(b *bufio.Reader) error {

	if n, err := respArrayHead(b); err != nil {
		return err
	} else if n != 5 {
		return fmt.Errorf("invalid response length %d, wnat 5", n)
	}

	for _, bp := range []*bool{
		&s.HasFeed,
		&s.HasHead,
		&s.Created,
	} {
		if *bp, err = respBool(b); err != nil {
			return
		}
	}

	for _, tp := range []*time.Time{
		&s.Access,
		&s.Create,
	} {
		if *tp, err = respTime(b); err != nil {
			return
		}
	}

	return
}
