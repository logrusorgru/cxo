package idx

import (
	"errors"
	"testing"
	"time"

	"github.com/skycoin/cxo/data"
	"github.com/skycoin/skycoin/src/cipher"
)

// hash and sig generated using given string and sk
func newRootByString(
	s string, sk cipher.SecKey,
) (hash cipher.SHA256, sig cipher.Sig) {
	hash = cipher.SumSHA256([]byte(s))
	sig = cipher.SignHash(hash, sk)
	return
}

//
// Feeds
//

func AddFeed(t *testing.T, idx data.IdxDB) {
	// AddFeed. Adding a feed twice or more times does nothing.

	var pk, _ = cipher.GenerateKeyPair()

	if err := idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if ok, err := idx.HasFeed(pk); err != nil {
		t.Error(err)
	} else if ok == false {
		t.Error("not exist")
	}

	// twice
	if err := idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if ok, err := idx.HasFeed(pk); err != nil {
		t.Error(err)
	} else if ok == false {
		t.Error("not exist")
	}

	return
}

func DelFeed(t *testing.T, idx data.IdxDB) {
	// DelFeed with all heads and Root objects
	// unconditionally. If feed doesn't exist
	// then the Del returns ErrNoSuchFeed.

	var pk, sk = cipher.GenerateKeyPair()

	// not exist
	if err := idx.DelFeed(pk); err == nil {
		t.Error("missing error")
	} else if err != data.ErrNoSuchFeed {
		t.Error(err)
	}

	// add
	if err := idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}

	// exist
	if err := idx.DelFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if ok, err := idx.HasFeed(pk); err != nil {
		t.Error(err)
	} else if ok == true {
		t.Error("not deleted")
	}

	// with head and root
	// (heads and roots must be removed)
	if err := idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}

	var nonce uint64 = 1090

	if err := idx.AddHead(pk, 1090); err != nil {
		t.Error(err)
		return
	}

	if ln, err := idx.HeadsLen(pk); err != nil {
		t.Error(err)
		return
	} else if ln != 1 {
		t.Errorf("unexpected heads length: %d, want 1", ln)
	}

	var (
		hash = cipher.SumSHA256([]byte("stub"))
		sig  = cipher.SignHash(hash, sk)
	)

	if _, err := idx.SetRoot(pk, nonce, 0, hash, sig); err != nil {
		t.Error(err)
		return
	}

	if ln, err := idx.RootsLen(pk, nonce); err != nil {
		t.Error(err)
		return
	} else if ln != 1 {
		t.Errorf("unexpected roots length %d, want 1", ln)
	}

	idx.DelFeed(pk)

	return
}

func IterateFeeds(t *testing.T, idx data.IdxDB) {
	// Iterate all feeds. Use ErrStopIteration to
	// stop iteration. The Iterate passes any error
	// returned from given function through. Except
	// ErrStopIteration that turns nil. It's possible
	// to mutate the IdxDB inside the Iterate

	var (
		called int
		err    error
	)

	err = idx.IterateFeeds(func(pk cipher.PubKey) (err error) {
		called++
		return
	})

	if err != nil {
		t.Error(err)
		return
	}

	if called != 0 {
		t.Error("wrong times called", called)
		return
	}

	// one
	var pk1, _ = cipher.GenerateKeyPair()
	if err = idx.AddFeed(pk1); err != nil {
		t.Error(err)
		return
	}

	called = 0
	err = idx.IterateFeeds(func(pk cipher.PubKey) (err error) {
		called++
		if pk != pk1 {
			t.Error("wrong pk")
		}
		return
	})

	if err != nil {
		t.Error(err)
	}

	if called != 1 {
		t.Error("wrong times called", called)
	}

	// two
	var pk2, _ = cipher.GenerateKeyPair()
	if err = idx.AddFeed(pk2); err != nil {
		t.Error(err)
		return
	}

	called = 0
	var pks = map[cipher.PubKey]struct{}{pk1: {}, pk2: {}}
	err = idx.IterateFeeds(func(pk cipher.PubKey) (err error) {
		called++
		if _, ok := pks[pk]; ok == false {
			t.Error("unexpected pk:", pk.Hex()[:7])
		}
		delete(pks, pk)
		return
	})

	if err != nil {
		t.Error(err)
	}

	if called != 2 {
		t.Error("wrong times called", called)
	}

	// data.ErrStopIteration
	called = 0
	pks = map[cipher.PubKey]struct{}{pk1: {}, pk2: {}}
	err = idx.IterateFeeds(func(pk cipher.PubKey) (err error) {
		called++
		if _, ok := pks[pk]; ok == false {
			t.Error("unexpected pk:", pk.Hex()[:7])
		}
		delete(pks, pk)
		return data.ErrStopIteration
	})

	if err != nil {
		t.Error(err)
	}

	if called != 1 {
		t.Error("wrong times called", called)
	}

	// pass error through
	var errBreaking = errors.New("breaking error")
	called = 0
	pks = map[cipher.PubKey]struct{}{pk1: {}, pk2: {}}
	err = idx.IterateFeeds(func(pk cipher.PubKey) (err error) {
		called++
		if _, ok := pks[pk]; ok == false {
			t.Error("unexpected pk:", pk.Hex()[:7])
		}
		delete(pks, pk)
		return errBreaking
	})

	if err != errBreaking {
		t.Error("wrong error", err)
	}

	if called != 1 {
		t.Error("wrong times called", called)
	}

	return
}

func HasFeed(t *testing.T, idx data.IdxDB) {
	// HasFeed returns true if the IdxDB contains
	// feed with given public key

	var pk, _ = cipher.GenerateKeyPair()

	// has not
	var ok, err = idx.HasFeed(pk)
	if err != nil {
		t.Error(err)
	}

	if ok == true {
		t.Error("has")
	}

	// has
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}

	if ok, err = idx.HasFeed(pk); err != nil {
		t.Error(err)
	}

	if ok == false {
		t.Error("has not")
	}

	return
}

func FeedsLen(t *testing.T, idx data.IdxDB) {
	// FeedsLen is number of feeds in DB

	var (
		l   int
		err error
		pk  cipher.PubKey
	)

	for i := 0; i < 3; i++ {
		if l, err = idx.FeedsLen(); err != nil {
			t.Error(err)
		}
		if l != i {
			t.Errorf("wrong feeds length %d, wnat %d", l, i)
		}
		pk, _ = cipher.GenerateKeyPair()
		if err = idx.AddFeed(pk); err != nil {
			t.Error(err)
		}
	}

}

//
// Heads
//

func AddHead(t *testing.T, idx data.IdxDB) {
	// AddHead new head with given nonce.
	// If a head with given nonce already
	// exists, then this method does nothing.

	// ErrNoSouchFeed
	var (
		pk, _        = cipher.GenerateKeyPair()
		nonce uint64 = 1050

		ok  bool
		err error
	)

	// ErrNoSuchFeed
	if err = idx.AddHead(pk, nonce); err != data.ErrNoSuchFeed {
		t.Error("missing or wrong error:", err)
	}

	// has
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
	}
	if ok, err = idx.HasHead(pk, nonce); err != nil {
		t.Error(err)
	} else if ok == false {
		t.Error("has not")
	}

	// twice
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
	}
	if ok, err = idx.HasHead(pk, nonce); err != nil {
		t.Error(err)
	} else if ok == false {
		t.Error("has not")
	}

	return
}

func DelHead(t *testing.T, idx data.IdxDB) {
	// DelHead deletes head with given nonce and
	// all its Root objects. The method returns
	// ErrNoSuchHead if a head with given nonce
	// doesn't exist.

	// ErrNoSouchFeed
	var (
		pk, _        = cipher.GenerateKeyPair()
		nonce uint64 = 1050

		ok  bool
		err error
	)

	if err = idx.DelHead(pk, nonce); err != data.ErrNoSuchFeed {
		t.Error("missing or wrong error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}

	if err = idx.DelHead(pk, nonce); err != data.ErrNoSuchHead {
		t.Error("missing or wrong error:", err)
	}

	// delete
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
	}

	if err = idx.DelHead(pk, nonce); err != nil {
		t.Error(err)
	}

	if ok, err = idx.HasHead(pk, nonce); err != nil {
		t.Error(err)
	} else if ok == true {
		t.Error("not deleted")
	}

	return
}

func HasHead(t *testing.T, idx data.IdxDB) {
	// HasHead returns true if a head with given
	// nonce exits in the DB

	var (
		pk, _        = cipher.GenerateKeyPair()
		nonce uint64 = 1050

		ok  bool
		err error
	)

	// ErrNoSuchFeed
	if _, err = idx.HasHead(pk, nonce); err != data.ErrNoSuchFeed {
		t.Error("missing or wrong error:", err)
	}

	// has not
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}
	if ok, err = idx.HasHead(pk, nonce); err != nil {
		t.Error(err)
	} else if ok == true {
		t.Error("has")
	}

	// has
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
	}
	if ok, err = idx.HasHead(pk, nonce); err != nil {
		t.Error(err)
	} else if ok == false {
		t.Error("has not")
	}

	return
}

func IterateHeads(t *testing.T, idx data.IdxDB) {
	// IterateHeads over all heads

	var (
		pk, _ = cipher.GenerateKeyPair()

		called int
		err    error
	)

	// ErrNoSuchFeed
	err = idx.IterateHeads(pk, func(uint64) (err error) {
		called++
		return
	})
	if err != data.ErrNoSuchFeed {
		t.Error("missing or wrong error:", err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// no heads
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}
	err = idx.IterateHeads(pk, func(uint64) (err error) {
		called++
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// one
	var n1 uint64 = 1050
	if err = idx.AddHead(pk, n1); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.IterateHeads(pk, func(nonce uint64) (err error) {
		called++
		if nonce != n1 {
			t.Error("wrong nonce", nonce)
		}
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}

	// two
	var n2 uint64 = 1090
	if err = idx.AddHead(pk, n2); err != nil {
		t.Error(err)
	}
	var ns = map[uint64]struct{}{n1: {}, n2: {}}
	called = 0
	err = idx.IterateHeads(pk, func(nonce uint64) (err error) {
		called++
		if _, ok := ns[nonce]; ok == false {
			t.Error("unexpected head:", nonce)
		}
		delete(ns, nonce)
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 2 {
		t.Error("wrong times called:", called)
	}

	// stop iteration
	ns = map[uint64]struct{}{n1: {}, n2: {}}
	called = 0
	err = idx.IterateHeads(pk, func(nonce uint64) (err error) {
		called++
		if _, ok := ns[nonce]; ok == false {
			t.Error("unexpected head:", nonce)
		}
		delete(ns, nonce)
		return data.ErrStopIteration
	})
	if err != nil {
		t.Error(err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}

	// braking error
	var errBreaking = errors.New("breaking error")
	ns = map[uint64]struct{}{n1: {}, n2: {}}
	called = 0
	err = idx.IterateHeads(pk, func(nonce uint64) (err error) {
		called++
		if _, ok := ns[nonce]; ok == false {
			t.Error("unexpected head:", nonce)
		}
		delete(ns, nonce)
		return errBreaking
	})
	if err != errBreaking {
		t.Error("wrong or missing error:", err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}

	return
}

func HeadsLen(t *testing.T, idx data.IdxDB) {
	// HeadsLen is number of heads stored

	var (
		pk, _ = cipher.GenerateKeyPair()

		l   int
		err error
	)

	// ErrNoSuchFeed
	if _, err = idx.HeadsLen(pk); err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// zero
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}
	if l, err = idx.HeadsLen(pk); err != nil {
		t.Error(err)
	} else if l != 0 {
		t.Error("wrong length:", l)
	}

	// one
	if err = idx.AddHead(pk, 1050); err != nil {
		t.Error(err)
	}
	if l, err = idx.HeadsLen(pk); err != nil {
		t.Error(err)
	} else if l != 1 {
		t.Error("wrong length:", l)
	}

	// two
	if err = idx.AddHead(pk, 1090); err != nil {
		t.Error(err)
	}
	if l, err = idx.HeadsLen(pk); err != nil {
		t.Error(err)
	} else if l != 2 {
		t.Error("wrong length:", l)
	}
}

//
// Roots
//

func AscendRoots(t *testing.T, idx data.IdxDB) {
	// AscendRoots iterates all Root object ascending order.
	// Use ErrStopIteration to stop iteration. Any error
	// (except the ErrStopIteration) returned by given
	// IterateRootsFunc will be passed through. The
	// AscendRoots doesn't update access time of a Root.
	// See also IterateRootsFunc docs.

	var (
		pk, sk        = cipher.GenerateKeyPair()
		nonce  uint64 = 1050

		called int
		err    error
	)

	// ErrNoSuchFeed
	err = idx.AscendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return
	})
	if err != data.ErrNoSuchFeed {
		t.Error("missing or unexpected error:", err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.AscendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return
	})
	if err != data.ErrNoSuchHead {
		t.Error("missing or unexpected error:", err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// no Root objects
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.AscendRoots(pk, nonce, func(seq uint64) (err error) {
		println("ASCEND", seq)
		called++
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// one
	var (
		h1, s1        = newRootByString("one", sk)
		seq1   uint64 = 10
	)
	if _, err = idx.SetRoot(pk, nonce, seq1, h1, s1); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.AscendRoots(pk, nonce, func(seq uint64) (err error) {
		called++
		if seq != seq1 {
			t.Error("unexpected seq", seq)
		}
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}

	// two
	var (
		h2, s2        = newRootByString("one", sk)
		seq2   uint64 = 20
		seqs          = []uint64{seq1, seq2}
	)
	if _, err = idx.SetRoot(pk, nonce, seq2, h2, s2); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.AscendRoots(pk, nonce, func(seq uint64) (err error) {
		called++
		if len(seqs) == 0 {
			t.Error("unexpected seq:", seq)
		}
		if seq != seqs[0] {
			t.Error("unexpected seq or wrong order:", seq, called)
		}
		seqs = seqs[1:]
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 2 {
		t.Error("wrong times called:", called)
	}

	// ErrStopIteration
	called = 0
	err = idx.AscendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return data.ErrStopIteration
	})
	if err != nil {
		t.Error(err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}

	// braking error
	var errBreaking = errors.New("breaking error")
	called = 0
	err = idx.AscendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return errBreaking
	})
	if err != errBreaking {
		t.Error("wrong or missing error:", err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}
}

func DescendRoots(t *testing.T, idx data.IdxDB) {
	// DescendRoots is the same as the Ascend, but it iterates
	// decending order. Use ErrStopIteration to stop
	// iteration. The DescendRoots doesn't update access time.
	// See also IterateRootsFunc docs.

	var (
		pk, sk        = cipher.GenerateKeyPair()
		nonce  uint64 = 1050

		called int
		err    error
	)

	// ErrNoSuchFeed
	err = idx.DescendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return
	})
	if err != data.ErrNoSuchFeed {
		t.Error("missing or unexpected error:", err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.DescendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return
	})
	if err != data.ErrNoSuchHead {
		t.Error("missing or unexpected error:", err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// no Root objects
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.DescendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 0 {
		t.Error("wrong times called:", called)
	}

	// one
	var (
		h1, s1        = newRootByString("one", sk)
		seq1   uint64 = 10
	)
	if _, err = idx.SetRoot(pk, nonce, seq1, h1, s1); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.DescendRoots(pk, nonce, func(seq uint64) (err error) {
		called++
		if seq != seq1 {
			t.Error("unexpected seq", seq)
		}
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}

	// two
	var (
		h2, s2        = newRootByString("one", sk)
		seq2   uint64 = 20
		seqs          = []uint64{seq2, seq1}
	)
	if _, err = idx.SetRoot(pk, nonce, seq2, h2, s2); err != nil {
		t.Error(err)
	}
	called = 0
	err = idx.DescendRoots(pk, nonce, func(seq uint64) (err error) {
		called++
		if len(seqs) == 0 {
			t.Error("unexpected seq:", seq)
		}
		if seq != seqs[0] {
			t.Error("unexpected seq or wrong order:", seq, called)
		}
		seqs = seqs[1:]
		return
	})
	if err != nil {
		t.Error(err)
	}
	if called != 2 {
		t.Error("wrong times called:", called)
	}

	// ErrStopIteration
	called = 0
	err = idx.DescendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return data.ErrStopIteration
	})
	if err != nil {
		t.Error(err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}

	// braking error
	var errBreaking = errors.New("breaking error")
	called = 0
	err = idx.DescendRoots(pk, nonce, func(uint64) (err error) {
		called++
		return errBreaking
	})
	if err != errBreaking {
		t.Error("wrong or missing error:", err)
	}
	if called != 1 {
		t.Error("wrong times called:", called)
	}
}

func HasRoot(t *testing.T, idx data.IdxDB) {
	// HasRoot the Roots Root with given seq?

	var (
		pk, sk        = cipher.GenerateKeyPair()
		nonce  uint64 = 1050
		seq    uint64 = 10

		ok  bool
		err error
	)

	// ErrNoSuchFeed
	if _, err = idx.HasRoot(pk, nonce, seq); err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.HasRoot(pk, nonce, seq); err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// has not
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	if ok, err = idx.HasRoot(pk, nonce, seq); err != nil {
		t.Error(err)
	} else if ok == true {
		t.Error("has")
	}

	// has
	var hash, sig = newRootByString("some", sk)
	if _, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if ok, err = idx.HasRoot(pk, nonce, seq); err != nil {
		t.Error(err)
	} else if ok == false {
		t.Error("has not")
	}

}

func RootsLen(t *testing.T, idx data.IdxDB) {
	// RootsLen is number of Root objects stored

	var (
		pk, sk        = cipher.GenerateKeyPair()
		nonce  uint64 = 1050
		seq    uint64 = 10

		l   int
		err error
	)

	// ErrNoSuchFeed
	if _, err = idx.RootsLen(pk, nonce); err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.RootsLen(pk, nonce); err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// has not
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	if l, err = idx.RootsLen(pk, nonce); err != nil {
		t.Error(err)
	} else if l != 0 {
		t.Error("wrong length", l)
	}

	// has
	var hash, sig = newRootByString("some", sk)
	if _, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if l, err = idx.RootsLen(pk, nonce); err != nil {
		t.Error(err)
	} else if l != 1 {
		t.Error("wrong length", l)
	}

}

func SetRoot(t *testing.T, idx data.IdxDB) {
	// SetRoot add or touch Root if exists

	var (
		pk, sk            = cipher.GenerateKeyPair()
		nonce, seq uint64 = 1050, 10
		hash, sig         = newRootByString("some", sk)

		root *data.Root
		err  error
	)

	// ErrNoSuchFeed
	_, err = idx.SetRoot(pk, nonce, seq, hash, sig)
	if err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	_, err = idx.SetRoot(pk, nonce, seq, hash, sig)
	if err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// set
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	var tp = time.Now()
	if root, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root stored")
	}
	if root.Create.After(tp) == false {
		t.Error("wrong creating time")
	}
	if root.Access.UnixNano() != 0 {
		t.Error("wrong access time")
	}
	var create = root.Create // for next test

	// twice
	if root, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root stored")
	}
	if root.Create.Equal(create) == false {
		t.Error("wrong creating time")
	}
	if root.Access.Equal(create) == false {
		t.Error("wrong access time")
	}

}

func SetNotTouchRoot(t *testing.T, idx data.IdxDB) {
	// SetNotTouchRoot add Root or do nothing if exists

	var (
		pk, sk            = cipher.GenerateKeyPair()
		nonce, seq uint64 = 1050, 10
		hash, sig         = newRootByString("some", sk)

		root *data.Root
		err  error
	)

	// ErrNoSuchFeed
	_, err = idx.SetNotTouchRoot(pk, nonce, seq, hash, sig)
	if err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	_, err = idx.SetNotTouchRoot(pk, nonce, seq, hash, sig)
	if err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// set
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	var tp = time.Now()
	if root, err = idx.SetNotTouchRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root stored")
	}
	if root.Create.After(tp) == false {
		t.Error("wrong creating time")
	}
	if root.Access.UnixNano() != 0 {
		t.Error("wrong access time")
	}
	var create = root.Create // for next test

	// twice
	if root, err = idx.SetNotTouchRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root stored")
	}
	if root.Create.Equal(create) == false {
		t.Error("wrong creating time")
	}
	if root.Access.UnixNano() != 0 {
		t.Error("wrong access time (touched)")
	}

}

func GetRoot(t *testing.T, idx data.IdxDB) {
	// GetRoot returns root and touches stored.

	var (
		pk, sk            = cipher.GenerateKeyPair()
		nonce, seq uint64 = 1050, 10
		hash, sig         = newRootByString("some", sk)

		root *data.Root
		err  error
	)

	// ErrNoSuchFeed
	if _, err = idx.GetRoot(pk, nonce, seq); err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.GetRoot(pk, nonce, seq); err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// ErrNotFound
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.GetRoot(pk, nonce, seq); err != data.ErrNotFound {
		t.Error(err)
		return
	}

	// get
	var set *data.Root
	if set, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if root, err = idx.GetRoot(pk, nonce, seq); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root")
	}
	if root.Access.Equal(set.Create) == false {
		t.Error("wrong access time")
	}
	if root.Create.Equal(set.Create) == false {
		t.Error("wrong create time")
	}

	// twice
	if root, err = idx.GetRoot(pk, nonce, seq); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root")
	}
	if root.Access.After(set.Create) == false {
		t.Error("wrong access time")
	}
	if root.Create.Equal(set.Create) == false {
		t.Error("wrong create time")
	}

}

func GetNotTouchRoot(t *testing.T, idx data.IdxDB) {
	// GetNotTouchRoot returns root.

	var (
		pk, sk            = cipher.GenerateKeyPair()
		nonce, seq uint64 = 1050, 10
		hash, sig         = newRootByString("some", sk)

		root *data.Root
		err  error
	)

	// ErrNoSuchFeed
	if _, err = idx.GetNotTouchRoot(pk, nonce, seq); err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.GetNotTouchRoot(pk, nonce, seq); err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// ErrNotFound
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.GetNotTouchRoot(pk, nonce, seq); err != data.ErrNotFound {
		t.Error(err)
		return
	}

	// get
	var set *data.Root
	if set, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if root, err = idx.GetNotTouchRoot(pk, nonce, seq); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root")
	}
	if root.Access.Equal(set.Create) == false {
		t.Error("wrong access time")
	}
	if root.Create.Equal(set.Create) == false {
		t.Error("wrong create time")
	}

	// twice
	if root, err = idx.GetNotTouchRoot(pk, nonce, seq); err != nil {
		t.Error(err)
		return
	}
	if root.Hash != hash || root.Sig != sig {
		t.Error("wrong root")
	}
	if root.Access.Equal(set.Create) == false {
		t.Error("wrong access time")
	}
	if root.Create.Equal(set.Create) == false {
		t.Error("wrong create time")
	}
}

func TakeRoot(t *testing.T, idx data.IdxDB) {
	// TakeRoot deletes Root returning it

	var (
		pk, sk            = cipher.GenerateKeyPair()
		nonce, seq uint64 = 1050, 10
		hash, sig         = newRootByString("some", sk)

		err error
	)

	// ErrNoSuchFeed
	if _, err = idx.TakeRoot(pk, nonce, seq); err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.TakeRoot(pk, nonce, seq); err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// ErrNotFound
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	if _, err = idx.TakeRoot(pk, nonce, seq); err != data.ErrNotFound {
		t.Error(err)
		return
	}

	// del
	var set, take *data.Root

	if set, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if take, err = idx.TakeRoot(pk, nonce, seq); err != nil {
		t.Error(err)
		return
	}
	var ok bool
	if ok, err = idx.HasRoot(pk, nonce, seq); err != nil {
		t.Error(err)
	} else if ok == true {
		t.Error("has")
	}

	if take.Hash != set.Hash || take.Sig != set.Sig {
		t.Error("wrong root")
	}
	if take.Access.Equal(set.Create) == false {
		t.Error("wrong access time")
	}
	if take.Create.Equal(set.Create) == false {
		t.Error("wrong create time")
	}
}

func DelRoot(t *testing.T, idx data.IdxDB) {
	// DelRoot deletes Root.

	var (
		pk, sk            = cipher.GenerateKeyPair()
		nonce, seq uint64 = 1050, 10
		hash, sig         = newRootByString("some", sk)

		err error
	)

	// ErrNoSuchFeed
	if err = idx.DelRoot(pk, nonce, seq); err != data.ErrNoSuchFeed {
		t.Error("wrong or missing error:", err)
	}

	// ErrNoSuchHead
	if err = idx.AddFeed(pk); err != nil {
		t.Error(err)
		return
	}
	if err = idx.DelRoot(pk, nonce, seq); err != data.ErrNoSuchHead {
		t.Error("wrong or missing error:", err)
	}

	// ErrNotFound
	if err = idx.AddHead(pk, nonce); err != nil {
		t.Error(err)
		return
	}
	if err = idx.DelRoot(pk, nonce, seq); err != data.ErrNotFound {
		t.Error(err)
		return
	}

	// del
	if _, err = idx.SetRoot(pk, nonce, seq, hash, sig); err != nil {
		t.Error(err)
		return
	}
	if err = idx.DelRoot(pk, nonce, seq); err != nil {
		t.Error(err)
		return
	}
	var ok bool
	if ok, err = idx.HasRoot(pk, nonce, seq); err != nil {
		t.Error(err)
	} else if ok == true {
		t.Error("has")
	}
}

// IsSafeClosed test case. The reopen fucntion
// can be nil if DB is in-memory.
func IsSafeClosed(
	t *testing.T, //                      : the T pointer
	idx data.IdxDB, //                    : idx already opened
	reopen func() (data.IdxDB, error), // : reopen idx to check the flag
) {
	// IsSafeClosed() bool

	if idx.IsSafeClosed() == false {
		t.Error("fresh db is not safe closed")
	}

	if reopen == nil {
		return
	}

	var err error
	if err = idx.Close(); err != nil {
		t.Error(err)
	}

	if idx, err = reopen(); err != nil {
		t.Error(err)
	}

	if idx.IsSafeClosed() == false {
		t.Error("not safe closed, after reopenning")
	}

}

// Close test case.
func Close(t *testing.T, idx data.IdxDB) {
	// Close() (err error)

	for i := 0; i < 2; i++ {
		if err := idx.Close(); err != nil {
			t.Error(i, err)
		}
	}

}
