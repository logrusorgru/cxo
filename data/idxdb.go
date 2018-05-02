package data

import (
	"github.com/skycoin/skycoin/src/cipher"
)

//
type IterateFeedsFunc func(pk cipher.PubKey) (err error)

//
type IterateHeadsFunc func(nonce uint64) (err error)

//
type IterateRootsFunc func(seq uint64) (err error)

// An IdxDB describes CXO database used to collect
// objects. The IdxDB keeps information about feeds,
// heads and root objects.
type IdxDB interface {

	//
	// Feeds
	//
	// AddFeed. Adding a feed twice or more times
	// does nothing.
	AddFeed(pk cipher.PubKey) (err error)
	// DelFeed with all heads and Root objects
	// unconditionally. If feed doesn't exist
	// then the Del returns ErrNoSuchFeed.
	DelFeed(pk cipher.PubKey) (err error)
	// Iterate all feeds. Use ErrStopIteration to
	// stop iteration. The Iterate passes any error
	// returned from given function through. Except
	// ErrStopIteration that turns nil. It's possible
	// to mutate the IdxDB inside the Iterate
	IterateFeeds(iterateFunc IterateFeedsFunc) (err error)
	// HasFeed returns true if the IdxDB contains
	// feed with given public key
	HasFeed(pk cipher.PubKey) (ok bool, err error)
	// FeedsLen is number of feeds in DB
	FeedsLen() (length int, err error)

	//
	// Heads
	//
	// AddHead new head with given nonce.
	// If a head with given nonce already
	// exists, then this method does nothing.
	AddHead(pk cipher.PubKey, nonce uint64) (err error)
	// DelHead deletes head with given nonce and
	// all its Root objects. The method returns
	// ErrNoSuchHead if a head with given nonce
	// doesn't exist.
	DelHead(pk cipher.PubKey, nonce uint64) (err error)
	// Has returns true if a head with given
	// nonce exits in the DB
	HasHead(pk cipher.PubKey, nonce uint64) (ok bool, err error)
	// Iterate over all heads
	IterateHeads(pk cipher.PubKey, iterateFunc IterateHeadsFunc) (err error)
	// HeadsLen is number of heads stored
	HeadsLen(pk cipher.PubKey) (length int, err error)

	//
	// Roots
	//
	// AscendRoots iterates all Root object ascending order.
	// Use ErrStopIteration to stop iteration. Any error
	// (except the ErrStopIteration) returned by given
	// IterateRootsFunc will be passed through. The
	// AscendRoots doesn't update access time of a Root.
	// See also IterateRootsFunc docs.
	AscendRoots(
		pk cipher.PubKey, nonce uint64, iterateFunc IterateRootsFunc,
	) (err error)
	// DescendRoots is the same as the Ascend, but it iterates
	// decending order. Use ErrStopIteration to stop
	// iteration. The DescendRoots doesn't update access time.
	// See also IterateRootsFunc docs.
	DescendRoots(
		pk cipher.PubKey, nonce uint64, iterateFunc IterateRootsFunc,
	) (err error)
	// Has the Roots Root with given seq?
	HasRoot(pk cipher.PubKey, nonce uint64, seq uint64) (ok bool, err error)
	// Len is number of Root objects stored
	RootsLen(pk cipher.PubKey, nonce uint64) (length int, err error)

	// SetRoot add or touch Root if exists
	SetRoot(
		pk cipher.PubKey,
		nonce uint64,
		seq uint64,
		hash cipher.SHA256,
		sig cipher.Sig,
	) (root *Root, err error)
	// SetNotTouch add Root or do nothing if exists
	SetNotTouchRoot(
		pk cipher.PubKey,
		nonce uint64,
		seq uint64,
		hash cipher.SHA256,
		sig cipher.Sig,
	) (root *Root, err error)
	// GetRoot returns root and touches stored.
	GetRoot(pk cipher.PubKey, nonce uint64, seq uint64) (root *Root, err error)
	// GetNotTouchRoot returns root.
	GetNotTouchRoot(
		pk cipher.PubKey, nonce uint64, seq uint64,
	) (root *Root, err error)
	// DelRoot deletes Root.
	DelRoot(pk cipher.PubKey, nonce uint64, seq uint64) (err error)

	// IsSafeClosed retursn true if last closing was successful,
	// and no data lost. New DB returns true too, even if it never
	// been closed before.
	IsSafeClosed() bool

	// Close IdxDB
	Close() error
}
