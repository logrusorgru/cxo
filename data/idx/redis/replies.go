package redis

import (
	"bufio"
	"fmt"
	"strconv"
	"time"

	"github.com/skycoin/skycoin/src/cipher"

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
	t = (i.I != 0)
	return
}

func respTime(b *bufio.Reader) (t time.Time, err error) {
	var s resp.BulkString
	if err = s.UnmarshalRESP(b); err != nil {
		return
	}
	var i int64
	if i, err = strconv.ParseInt(s.S, 10, 64); err != nil {
		panic(err) // must not happen
	}
	t = time.Unix(0, i)
	return
}

func respBytes(b *bufio.Reader) (p []byte, err error) {
	var s resp.BulkStringBytes
	if err = s.UnmarshalRESP(b); err != nil {
		return
	}
	p = s.B
	return
}

func respSkipBulkString(b *bufio.Reader, times int) (err error) {
	var bs resp.BulkString
	for i := 0; i < times; i++ {
		if err = bs.UnmarshalRESP(b); err != nil {
			return
		}
	}
	return
}

type rangeRootsReply struct {
	HasFeed bool
	HasHead bool
	Seqs    []uint64
}

func (r *rangeRootsReply) UnmarshalRESP(b *bufio.Reader) (err error) {

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

	var (
		seqs []uint64
		any  = resp.Any{
			I: &seqs,
		}
	)

	if err = any.UnmarshalRESP(b); err != nil {
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

func (s *setRootReply) UnmarshalRESP(b *bufio.Reader) (err error) {

	var n int
	if n, err = respArrayHead(b); err != nil {
		return
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

type getRootReply struct {
	HasFeed bool
	HasHead bool
	Hash    cipher.SHA256
	Sig     cipher.Sig
	Access  time.Time
	Create  time.Time
}

func (g *getRootReply) UnmarshalRESP(b *bufio.Reader) (err error) {

	if n, err := respArrayHead(b); err != nil {
		return err
	} else if n != 6 {
		return fmt.Errorf("invalid response length %d, wnat 6", n)
	}

	for _, bp := range []*bool{
		&g.HasFeed,
		&g.HasHead,
	} {
		if *bp, err = respBool(b); err != nil {
			return
		}
	}

	// hash
	var p []byte
	if p, err = respBytes(b); err != nil {
		return
	} else if len(p) == 0 {
		g.Hash = cipher.SHA256{}        // clear
		g.Sig = cipher.Sig{}            // clear
		g.Create = time.Unix(0, 0)      // clear
		g.Access = g.Create             // clear
		return respSkipBulkString(b, 3) // skip them
	} else if len(p) != len(cipher.SHA256{}) {
		panic(fmt.Errorf("invalid (idx/redis) root#hash length %d", len(p)))
	} else {
		copy(g.Hash[:], p)
	}

	// sig
	if p, err = respBytes(b); err != nil {
		return
	} else if len(p) != len(cipher.Sig{}) {
		panic(fmt.Errorf("invalid (idx/redis) root#sig length %d", len(p)))
	} else {
		copy(g.Sig[:], p)
	}

	for _, tp := range []*time.Time{
		&g.Access,
		&g.Create,
	} {
		if *tp, err = respTime(b); err != nil {
			return
		}
	}

	return
}

type headsLenReply struct {
	HasFeed bool
	Length  int
}

func (h *headsLenReply) UnmarshalRESP(b *bufio.Reader) (err error) {
	var n int
	if n, err = respArrayHead(b); err != nil {
		return
	} else if n != 2 {
		return fmt.Errorf("invalid resposne length %d, want 2", n)
	}
	if h.HasFeed, err = respBool(b); err != nil {
		return
	}
	var length resp.Int
	if err = length.UnmarshalRESP(b); err != nil {
		return
	}
	h.Length = int(length.I)
	return
}

type boolReply bool

func (b *boolReply) UnmarshalRESP(br *bufio.Reader) (err error) {
	var t bool
	if t, err = respBool(br); err != nil {
		return
	}
	*b = boolReply(t)
	return
}

type delHeadReply struct {
	HasFeed bool
	HasHead bool
}

func (h *delHeadReply) UnmarshalRESP(b *bufio.Reader) (err error) {
	var n int
	if n, err = respArrayHead(b); err != nil {
		return
	} else if n != 2 {
		return fmt.Errorf("invalid resposne length %d, want 2", n)
	}
	if h.HasFeed, err = respBool(b); err != nil {
		return
	}
	h.HasHead, err = respBool(b)
	return
}
