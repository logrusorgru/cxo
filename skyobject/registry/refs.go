package registry

import (
	"errors"
	"fmt"

	"github.com/disiqueira/gotree"

	"github.com/skycoin/skycoin/src/cipher"
	"github.com/skycoin/skycoin/src/cipher/encoder"
)

// A Refs represents slice of references that
// represented as Merkle-tree with some degree.
// E.g. every branch of the tree has degree-th
// subtrees (branches or leafs). And all values
// stored in leafs. The Merkle-tree is required
// to share the Refs over network, and share
// changes of the Refs easy way. The goal is
// reducing network pressure
//
// The Refs can has internal hash-table index if
// Pack that initailizes (used for first access
// the Refs) has HashTableIndex flag. All falgs
// related to the Refs stored inside the Refs
// during initialization of the Refs. The flags
// are not stored in DB and doesn't sent through
// network
//
// So, the Refs uses lazy-loading strategy by
// default (if no flags set). I.e. all branches
// of the tree are not loaded and loads by needs.
// It's possible to load entire Refs providing
// the EntireRefs flag
//
// The Refs is not thread safe
type Refs struct {
	// Hash that represnts the Refs. If Refs is
	// blank then this Hash is blank too
	Hash cipher.SHA256

	depth  int `enc:"-"` // depth - 1
	degree int `enc:"-"` // degree

	length int `enc:"-"` // length of Refs

	refsIndex `enc:"-"` // hash-table index
	refsNode  `enc:"-"` // leafs, branches, mods and length

	flags Flags `enc:"-"` // first use (load) flags

	// stack of iterators, if element is true, then length of the Refs
	// has been changed and the iterator have to find next element
	// from the Root (and set next element of the iterators slice to true
	// for next iterator). This way, the Refs provides a way to
	// iterate inside another iterator, modify tree insisde an iterator,
	// etc
	iterators []bool `enc:"-"`
}

func (r *Refs) initialize(pack Pack) (err error) {

	if r.mods != 0 {
		return // already initialized
	}

	r.mods = loadedMod     // mark as loaded
	r.flags = pack.Flags() // keep current flags

	if r.Hash == (cipher.SHA256{}) {
		return // blank Refs, don't need to load
	}

	var er encodedRefs
	if err = get(pack, r.Hash, &er); err != nil {
		return // get or decoding error
	}

	r.depth = int(er.Depth)
	r.degree = int(er.Degree)

	r.length = int(er.Length)

	if r.length == 0 || r.degree < 2 {
		return ErrInvalidEncodedRefs // invalid state
	}

	if r.flags&HashTableIndex != 0 {
		r.refsIndex = make(refsIndex)
	}

	// r.refsNode.hash is already blank

	return r.loadSubtree(pack, &r.refsNode, er.Elements, r.depth)
}

// Init does nothing, but if the Refs are not
// initialized, then the Init method initilizes
// it saving curretn flags of given pack
//
// It's possible to use some other methods to
// initialize the Refs (Len for example), but
// some methods of the Refs may not initialze
// it
//
// If the Refs is already initialized then
// it will not be initialized anymore
func (r *Refs) Init(pack Pack) (err error) {
	return r.initialize(pack)
}

// Len returns length of the Refs
func (r *Refs) Len(pack Pack) (ln int, err error) {
	if err = r.initialize(pack); err != nil {
		return
	}
	ln = r.length
	return
}

// Depth return real depth of the Refs
func (r *Refs) Depth(pack Pack) (depth int, err error) {
	if err = r.initialize(pack); err != nil {
		return
	}
	depth = r.depth + 1
	return
}

// Degree returns degree of the Refs tree
func (r *Refs) Degree() (degree int, err error) {
	if err = r.initialize(pack); err != nil {
		return
	}
	degree = r.degree
	return
}

// Flags returns current flags of the Refs.
// If the Refs is not initialized it returns
// zero. The method deosn't initializes
// Refs and can be used (e.g. is usefull) with
// initialized Refs only. The Frags are flags
// with which the Refs has been initialized
// before. You can check HashTableIndex flag
// and other
//
//     if refs.Flags()&registry.HashTableIndex != 0 {
//         // hash-table index used
//     } else {
//         // no hash-table index
//     }
//
func (r *Refs) Flags() (flags Flags) {
	return r.flags
}

// an encodedRefs represents encoded Refs
// that contains length, degree, depth and
// hashes of nested elements
type encodedRefs struct {
	Depth    uint32
	Degree   uint32
	Length   uint32
	Elements []cipher.SHA256
}

// Reset the Refs. If the Refs can't be loaded
// or the Refs is broken, then it can't be used
// anymore. To reload the Refs to make it useful
// use this method. The Reset method doesn't reloads
// the Refs, but allows reloading for other methods.
// It's impossible to reset during an iteration.
// The Reset resets flags of the Refs too
func (r *Refs) Reset() (err error) {

	if len(r.iterators) > 0 {
		return ErrRefsIterating // can't reset during iterating
	}

	r.degree = 0 // clear
	r.depth = 0  // clear

	// the node
	r.length = 0     // clear
	r.leafs = nil    // free
	r.branches = nil // free
	r.mods = 0       // mark as not loaded

	r.flags = 0 // clear flags
	return
}

// 1. only 'hash' of refsNode has meaning
// 2. refsNode loaded
// 3. refsNode loaded with all subtrees

// HasHash returns false if the Refs doesn't have given hash. It returns
// true if contains, or first error. It never returns ErrNotFound
//
// The big O of the call is O(1) if the HashTableIndex flag has been set.
// Otherwise, the big O is O(n), where n is real length of the Refs
func (r *Refs) HasHash(
	pack Pack, //          : pack to load (if not loaded yet)
	hash cipher.SHA256, // : hash to check out
) (
	ok bool, //            : true if has got
	err error, //          : error if any
) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if r.flags&HashTableIndex != 0 {
		_, ok = r.refsIndex[hash]
		return
	}

	// else -> iterate ascending

	err = r.Ascend(pack, func(_ int, elHash cipher.SHA256) (err error) {
		if elHash == hash {
			ok = true
			return ErrStopIteration
		}
		return // continue
	})

	return
}

// ValueByHash decodes value by given hash. If HashTableIndex
// is set, then hash table used. If given hash is blank then
// the ValueByHash returns ErrRefsElementIsNil if the Refs
// contans blank element or elements. The ValueByHash returns
// ErrNotFound if the Refs doesn't contain an element with given
// hash. If the HashTableIndex flag has been set, then the
// hash-table used to check presence.
//
// The big O of the call is the same as the big O of the
// HasHash call
func (r *Refs) ValueByHash(
	pack Pack, //          : pack to laod
	hash cipher.SHA256, // : hash to get
	obj interface{}, //    : pointer to object to decode
) (
	err error, //          : error if any
) {

	// initilize() inside the HashHash

	var ok bool
	if ok, err = r.HasHash(pack, hash); err != nil {
		return
	}
	if !ok {
		return ErrNotFound
	}
	if hash == (cipher.SHA256{}) {
		return ErrRefsElementIsNil
	}

	err = get(pack, hash, obj)
	return
}

func (r *Refs) indexOfHashByHashTable(hash cipher.SHA256) (i int, err error) {

	if res, ok := r.refsIndex[hash]; ok {
		re := res[len(res)-1]
		if re.upper == nil {
			return r.leafsBranches.indexOfLeaf(re) // Refs.leafs
		}
		return r.indexOfLeaf(re) // re->upper->etc
	}

	err = ErrNotFound
	return
}

// IndexOfHash returns index of element by hash. It returns ErrNotfound
// if the Refs doesn't have given hash. If HashTableIndex flag is set then
// and the Refs contains many elements with given hash, then which of them
// will be returned, is undefined
//
// The big O of the call is O(1) if the Refs doesn't contain element(s)
// with given hash, or O(depth) if there is at least one lement with
// given hash. Where the depth is depth (height) of the Refs tree
func (r *Refs) IndexOfHash(
	pack Pack, //          : pack to load
	hash cipher.SHA256, // : hsah to find
) (
	i int, //              : index of the element
	err error, //          : error if any
) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if r.flags&HashTableIndex != 0 {
		return r.indexOfHashByHashTable(hash)
	}

	// else -> iterate ascending

	var found bool

	err = r.Ascend(pack, func(index int, elHash cipher.SHA256) (err error) {
		if elHash == hash {
			found = true
			i = index
			return ErrStopIteration // break
		}
		return // continue
	})

	if err == nil && found == false {
		err = ErrNotFound
	}

	return
}

// --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- -

func (r *Refs) indexOfLeaf(re *refsElement) (i int, err error) {

	// re.upper is not nil
	if i, err = re.upper.leafsBranches.indexOfLeaf(re); err != nil {
		return
	}

	down, up := re.upper, re.upper.upper

	for ; up != nil; down, up = up, up.upper {
		for _, br := range up.branches {
			if br == down {
				break
			}
			i += br.length
		}
	}

	for _, br := range r.branches {
		if br == down {
			break
		}
		i += br.length
	}

	return
}

func (r *Refs) indicesOfLeaf(re *refsElement) (is []int, err error) {

	if is, err = re.upper.leafsBranches.indicesOfLeaf(re.Hash); err != nil {
		return
	}

	down, up := re.upper, re.upper.upper

	for ; up != nil; down, up = up, up.upper {
		for _, br := range up.branches {
			if br == down {
				break
			}
			for k := range is {
				is[k] += br.length
			}
		}
	}

	for _, br := range r.branches {
		if br == down {
			break
		}
		for k := range is {
			is[k] += br.length
		}
	}

	return
}

// IndicesByHash returns indices of all elements wiht given hash.
// It returns ErrNotFound if the Refs doesn't contain such elements.
// Order of the indices, the IndicesByHash returns, is undefined.
//
// The big O of the call is O(m * depth), where m is number of
// elements with given hash in the Refs if HashTableInex flag used
// by the Refs. Otherwise, the big O is O(n), where n is real length
// of the Refs
func (r *Refs) IndicesByHash(pack Pack, hash cipher.SHA256) (is []int,
	err error) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if r.flags&HashTableIndex != 0 {

		var ok bool
		var res []*refsElement //

		if res, ok = r.refsIndex[hash]; !ok {
			err = ErrNotFound
			return
		}

		var js []int
		for _, re := range res {
			// find index
			if re.upper == nil {
				js, err = r.leafsBranches.indicesOfLeaf(re.Hash)
			} else {
				js, err = r.indicesOfLeaf(re)
			}
			if err != nil {
				return
			}
			is = append(is, js)
		}

		return
	}

	// else -> iterate ascending

	err = r.Ascend(pack, func(index int, elHash cipher.SHA256) (err error) {
		if elHash == hash {
			is = append(is, index)
		}
		return // continue
	})

	if err == nil && len(is) == 0 {
		err = ErrNotFound
	}

	return
}

// ValueOfHashWithIndex decodes value by given hash
// and returns index of the value. It returns actual
// index and ErrRefsElementIsNil if given hash is
// blank but exists in the Refs. The ValueOfHashWithIndex
// method is usablility wrapper over the IndexOfHash with
// related notes, the big O and limitations
func (r *Refs) ValueOfHashWithIndex(pack Pack, hash cipher.SHA256,
	obj interface{}) (i int, err error) {

	// initialize() inside the IndexOfHash

	if i, err = r.IndexOfHash(pack, hash); err != nil {
		return
	}

	if hash == (cipher.SHA256{}) {
		err = ErrRefsElementIsNil
		return
	}

	err = get(pack, hash, obj)
	return
}

func validateIndex(i int, length int) (err error) {
	if i < 0 || i >= length {
		err = ErrIndexOutOfRange
	}
	return
}

func (r *Refs) hashByIndex(pack Pack, i int, depth int, upper *refsNode,
	lb *leafsBranches) (hash cipher.SHA256, err error) {

	if depth == 0 {

		for _, el := range lb.leafs {
			if i == 0 {
				hash = el.Hash
				return // got it
			}
			i--
		}
		err = ErrInvalidRefs // can't find (invalid state)
		return

	}

	// else if depth > 0

	for _, br := range lb.branches {

		if err = r.loadRefsNodeIfNeed(pack, br, depth, upper); err != nil {
			return // error loading
		}

		if i > br.length {
			i -= br.length
			continue
		}

		return r.hashByIndex(pack, i, depth-1, br, &br.leafsBranches)
	}

	err = ErrInvalidRefs // can't find (invalid state)
	return
}

// HashByIndex returns hash by index
//
// The big O of the call is O(depth)
func (r *Refs) HashByIndex(pack Pack, i int) (hash cipher.SHA256, err error) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if err = validateIndex(i, r.length); err != nil {
		return
	}

	hash, err = r.hashByIndex(pack, i, r.depth, nil, &r.leafsBranches)
	return
}

// ValueByIndex returns value by index or ErrNotFound
// or another error. It also returns hash of the value.
// The ValueByIndex returns ErrRefsElementIsNil if element
// has been found but represents nil (blank hash)
func (r *Refs) ValueByIndex(pack Pack, i int,
	obj interface{}) (hash cipher.SHA256, err error) {

	// initialize() inside the HashByIndex

	if hash, err = r.HashByIndex(pack, i); err != nil {
		return
	}
	if hash == (cipher.SHA256{}) {
		err = ErrRefsElementIsNil
		return
	}
	err = get(pack, hash, obj)
	return
}

// encode as is
func (r *Refs) encode() []byte {
	var er encodedRefs

	er.Degree = uint32(r.degree)
	er.Depth = uint32(r.depth)
	er.Length = uint32(r.length)

	if r.depth == 0 {
		er.Elements = make([]cipher.SHA256, 0, len(r.leafs))
		for _, el := range r.leafs {
			er.Elements = append(er.Elements, el.Hash)
		}
	} else {
		er.Elements = make([]cipher.SHA256, 0, len(r.branches))
		for _, br := range r.branches {
			er.Elements = append(er.Elements, br.hash)
		}
	}

	return encoder.Serialize(er)
}

// update Refs.Hash only, without deep exploring
func (r *Refs) updateRootHash(pack Pack) (err error) {

	if r.length == 0 {
		if r.Hash == (cipher.SHA256{}) {
			return // already clear
		}
		r.Hash = cipher.SHA256{}
		r.mods |= (contentMod | originMod) // mark as modified
		return                             // it's enough
	}

	// so, r.length is not 0

	val := r.encode()
	hash := cipher.SumSHA256(val)

	if hash == r.Hash {
		return // already up to date
	}

	r.mods |= (contentMod | originMod) // mark as modified
	return pack.Set(hash, val)         // save
}

// update only if the Refs is not lazy
func (r *Refs) updateRootHashIfNeed(pack Pack) (err error) {
	if r.flags&LazyUpdating != 0 {
		return // let's be lazy
	}
	return r.updateRootHash()
}

// replace hash of the element for new one
func (r *Refs) setElementHash(el *refsElement,
	newHash cipher.SHA256) (replaced bool) {

	if el.Hash == newHash {
		return // just the same
	}

	if r.flags&HashTableIndex != 0 {
		r.delElementFromIndex(el.Hash, el)
		r.addElementToIndex(newHash, el)
	}

	el.Hash = newHash // set
	replaced = true   // has been replaced

	// has been modified, but length still the same
	r.mods |= (contentMod | originMod)

	return
}

func (r *Refs) setHashByIndex(pack Pack, i int, hash cipher.SHA256, depth int,
	upper *refsNode, lb *leafsBranches) (replaced bool, err error) {

	if depth == 0 {
		for k, el := range lb.leafs {
			if i == 0 {
				replaced = r.setElementHash(el, hash)
				return
			}
			i--
		}
		return ErrInvalidRefs // can't find
	}

	// else if depth > 0

	for _, br := range lb.branches {

		if err = r.loadRefsNodeIfNeed(pack, br, depth, upper); err != nil {
			return // error loading
		}

		if i > br.length {
			i -= br.length
			continue
		}

		replaced, err = r.setHashByIndex(pack, i, hash, depth-1, br,
			&br.leafsBranches)

		if err != nil {
			return
		}

		if replaced == false {
			return // nothing has been changed
		}

		return r.updateNodeHashIfNeed(pack, br, depth-1)
	}

	return ErrInvalidRefs // can't find
}

// SetHashByIndex replaces hash of element with given index with
// given hash
//
// The big O of the call is O(depth)
func (r *Refs) SetHashByIndex(pack Pack, i int,
	hash cipher.SHA256) (err error) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if err = validateIndex(i, r.length); err != nil {
		return
	}

	var replaced bool
	replaced, err = r.setHashByIndex(pack, i, hash, r.depth, nil,
		&r.leafsBranches)

	if err != nil {
		r.broken = err // treat the err as breaking
		return
	}

	if replaced == false {
		return // nothing has been changed
	}

	return r.updateRootHashIfNeed(pack)
}

// SetValueByIndex saves given value calculating its hash and sets this
// hash to given index. You must be sure that schema of given element is
// schema of the Refs. Otherwise, Refs will be broken. Use nil-interface{}
// to set blank hash
func (r *Refs) SetValueByIndex(pack Pack, i int, obj interface{}) (err error) {

	// initialize() inside the SetHashByIndex

	var hash cipher.SHA256

	if obj != nil {
		if hash, err = pack.Add(encoder.Serialize(obj)); err != nil {
			return
		}
	}

	return r.SetHashByIndex(pack, i, hash)
}

// decrNodeLength after deleting
func (r *Refs) decrNodeLength(pack Pack, rn *refsNode, depth int) (blank bool,
	err error) {

	if rn.length--; rn.length == 0 {
		rn.hash = cipher.SHA256{}        // clear hash
		rn.leafs, rn.branches = nil, nil // free subtree
		blank = true                     // mark as blank (removed)
		return                           // that's all
	}

	// depends on LazyUpdating flag
	err = r.updateNodeHashIfNeed(pack, rn, depth) // if need
	return
}

// decrRootLength after deleting
func (r *Refs) decrRootLength(pack Pack) error {

	if r.length--; r.length == 0 {
		r.Hash = cipher.SHA256{}          // clear the hash anyway
		r.mods |= (lengthMod | originMod) // modified
		r.leafs, r.branches = nil, nil    // free subtree
		return                            // that's all
	}

	// depends on LazyUpdating flag
	return r.updateRootHashIfNeed(pack) // if need
}

func (r *Refs) deleteByIndex(pack Pack, i, depth int, upper *refsElement,
	lb *leafsBranches) (err error) {

	if depth == 0 {
		for k, el := range lb.leafs {
			if i == 0 {
				r.deleteLeafByIndex(lb, i, el)
				return
			}
			i--
		}
		return ErrInvalidRefs // can't find
	}

	// else if depth > 0

	for k, br := range lb.branches {

		if err = r.loadRefsNodeIfNeed(pack, br, depth, upper); err != nil {
			return
		}

		if i > br.length {
			i -= br.length
			continue
		}

		// it's not the loop, we will here once per call maximum

		err = r.deleteByIndex(pack, i, depth-1, br, &br.leafsBranches)
		if err != nil {
			return
		}

		// reduce length (has been deleted)

		var blank bool
		if blank, err = r.decrNodeLength(pack, br, depth-1); err != nil {
			return // some error
		}

		if blank == true {
			// so, if it's blank, then we have to remove the branch from
			// the lb.branches
			lb.deleteBranchByIndex(k)
		}

		return // that's all
	}

	return ErrInvalidRefs // can't find
}

// DeleteByIndex deletes single element from the Refs changing
// the Refs
func (r *Refs) DeleteByIndex(pack Pack, i int) (err error) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if err = validateIndex(i, r.length); err != nil {
		return
	}

	err = r.deleteByIndex(pack, i, r.depth, nil, &r.leafsBranches)
	if err != nil {
		return
	}

	return r.decrRootLength(pack)
}

// TODO
func (r *Refs) delElementBubbling(pack Pack, el *refsElement) (err error) {

	up := el.upper // *refsNode or nil (the Refs)

	if up == nil {
		if err = r.leafsBranches.deleteLeaf(el); err != nil {
			return
		}
		return r.decrRootLength(pack)
	}

	var di int // current depth
	var blank bool

	for ; up != nil; up = up.upper {
		if blank, err = r.decrNodeLength(pack, up, depth-di); err != nil {
			return
		}
		if blank == true {
			if err = up.leafsBranches.deleteBranch(up); err != nil {
				return
			}
		}
		di-- // decrement depth
	}

	return r.decrRootLength(pack)
}

// DeleteByHash deletes all elements by given hash, reducing length of the
// Refs. The method returns ErrNotfound if the Refs doesn't contain such
// elements
//
// The big O of the call is O(m * depth), where m is number of elements with
// given hash, if HashTableIndex flag has been set. If the flag is not used
// by the Refs, then the big O is O(n), where n is real length of the Refs
func (r *Refs) DeleteByHash(pack Pack, hash cipher.SHA256) (err error) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if r.flags&HashTableIndex != 0 {
		// using hash-table

		res, ok := r.index[hash]
		if !ok {
			return ErrNotFound
		}

		for _, el := range res {
			r.delElementBubbling(el)
		}

		return
	}

	// TODO (kostyarin): implement and use (a)descend+delete method
	//                   that joins descending and deleting

	// else -> iterate descending

	// https://play.golang.org/p/5tvrkq5692

	var deleted bool // at least one

	err = r.Descend(pack, func(i int, elHash cipher.SHA256) (err error) {
		if elHash == hash {
			deleted = true
			return r.DeleteByIndex(pack, i) // continue
		}
		return // continue
	})

	if err == nil && deleted == false {
		err = ErrNotFound
	}

	return
}

// TODO (kostyarin): implement the DeleteSliceByIndices
//
// // DeleteSliceByIndices deletes slice from the 'from' to the 'to'
// // arguments. The 'from' and 'to' arguments are like a golang [a:b].
// // The method reduces length of the Refs
// func (r *Refs) DeleteSliceByIndices(pack Pack, from, to int) (err error) {
//
//	//
//
//	return
// }

func validateSliceIndices(i, j, length int) (err error) {
	if i < 0 || j < 0 || i > length || j > length {
		err = ErrIndexOutOfRange
	} else if i > j {
		err = ErrInvalidSliceIndex
	}
	return
}

// Slice returns new Refs that contains values of this Refs from
// given i (inclusive) to given j (exclusive). If i and j are valid
// and equal, then the Slcie return new empty Refs
func (r *Refs) Slice(pack Pack, i, j int) (slice *Refs, err error) {

	// https://play.golang.org/p/4tP7_MuCN9

	if err = r.initialize(pack); err != nil {
		return
	}

	if err = validateSliceIndices(i, j, r.length); err != nil {
		return
	}

	slice = new(Refs)

	slice.degree = r.degree
	if r.flags&HashTableIndex != 0 {
		slice.index = make(map[cipher.SHA256]*refsNode)
	}
	slice.flags = r.flags

	if i == j {
		return // done
	}

	// TODO (kostyarin): ascend from i to j appending to the slice

	return
}

func (r *Refs) startIterating() {
	r.iterating = append(r.iterating, false)
}

// for defer
func (r *Refs) stopIterating() {
	r.iterating = r.iterating[:len(r.iterating)-1]
}

// mus tbe called inside an iterator
func (r *Refs) isFindingIteratorsIndexFromRootRequired() (yep bool) {
	if r.iterators[len(r.iterators)-1] == true {
		if len(r.iterators) > 1 {
			r.iterators[len(r.iterators)-2] = true // pass to next
		}
		r.iterators[len(r.iterators)-1] = false // reset
		yep = true
	}
	return
}

// An IterateFunc represents function for iterating over
// the Refs ascending and desceidig order. It receives
// index of element and hash of the element. Use
// ErrStopIteration to break an iteration
type IterateFunc func(i int, hash cipher.SHA256) (err error)

// Ascend iterates over all values ascending order until
// first error or the end. Use ErrStopIteration to break
// iteration. Any error is returned by given function
// (except the ErrStopIteration) is returned by the Ascend()
// method
//
// The Ascend allows to cahnge the Refs inside given
// ascendFunc. Be careful, this way you can start an infinity
// Ascend (for example, if the ascenFunc appends something to
// the Refs every itteration). It's possile to iterate
// inside the ascendFunc too
func (r *Refs) Ascend(pack Pack, ascendFunc IterateFunc) error {
	return r.ascendFrom(pack, 0, ascendFunc)
}

func (r *Refs) ascendFrom(pack Pack, from int,
	ascendFunc IterateFunc) (err error) {

	if err = r.initialize(pack); err != nil {
		return
	}

	if err = validateIndex(from, r.length); err != nil {
		return
	}

	if r.length == 0 {
		return // empty Refs
	}

	r.startIterating()
	defer r.stopIterating()

	var rewind bool  // find next element from root of the Refs
	var i int = from // i is next index to find

	for {
		// - pass is number of elements iterated (not skipped)
		//        for current step
		// - rewind means that we need to find next element from
		//        root of the Refs, because the Refs has been
		//        changed
		// - err is loading error, malformed Refs error or
		//        ErrStopIteration
		pass, rewind, err = r.ascend(pack, i, ascendFunc, r.depth, nil, r.leafs,
			r.branches)
		// so, we need to find next element from root of the Refs
		if rewind == true {
			i += pass // shift the i to don't repeat elements
			continue  // find next index from root of the Refs
		}
		// the loop is necesary for rewinding only
		break
	}

	if err == ErrStopIteration {
		err = nil // clear the ErrStopIteration
	}

	return
}

// ascend depending on depth and i (starting element), check
// changes after every call of the ascendFunc
//
// arguments:
//  - pack is Pack instance
//  - i is starting element
//  - ascendFunc is given function
//  - depth is current depth level (0 for leafs)
//  - upper is upper *refsNode or nil if it's the Refs (required for loading)
//  - leafs are leafs of current *refsNode or of the Refs
//  - branches are branches of current *refsNode or of the Refs
//
// reply:
//  - pass is number of lements skipped or iterated
//  - rewind is true if finding next index from root of the Refs is required
//  - err is some error
//
func (r *Refs) ascend(pack Pack, i int, ascendFunc IterateFunc, depth int,
	upper *refsNode, leafs []*refsElement, branches []*refsNode) (pass int,
	rewind bool, err error) {

	if depth == 0 {
		return r.ascendLeafs(i, ascendFunc, leafs)
	}

	// else if depth > 0

	return r.ascendBranches(pack, i, ascendFunc, depth, upper, branches)
}

func (r *Refs) ascendLeafs(i int, ascendFunc IterateFunc,
	leafs []*refsElement) (pass int, rewind bool, err error) {

	var skip int // skip elements before the i

	for _, el := range leafs {
		if el.Deleted == true {
			continue // deleted elements do not exist
		}

		if skip < i {
			skip++ // skip this element
			continue
		}

		// here i == skip

		if err = ascendFunc(i, el.Hash); err != nil {
			return
		}

		// increment all counters
		i++    // current index
		skip++ // skip (not really skip)
		pass++ // processed elements

		// check out changes of the Refs
		if rewind = r.isFindingIteratorsIndexFromRootRequired(); rewind {
			// we need to find next element from root of the Refs,
			// because the ascendFunc changes the Refs tree
			return
		}

		// no changes required, continue
	}

	return
}

func (r *Refs) ascendBranches(pack Pack, i int, ascendFunc IterateFunc,
	depth int, upper *refsNode, branches []*refsNode) (pass int, rewind bool,
	err error) {

	var subpass int // pass for a subtree (for a branch)
	var skip int    // skip elements finding the i

	for _, br := range branches {

		if err = r.loadRefsNodeIfNeed(pack, br, depth, upper); err != nil {
			return // eror loading the branch
		}

		if br.length == 0 {
			continue // skip empty branches
		}

		if skip+br.length < i {
			skip += br.length // skip this branch finding i-th element
			continue
		}

		subpass, rewind, err = r.ascend(pack, i, ascendFunc, depth-1, br,
			br.leafs, br.branches)

		pass += subpass // process elements
		skip += subpass // increment the skip variable (don't really skip)

		if err != nil || rewind == true {
			return // some error or changes
		}

		i += subpass // increment current index

		// this is local variable and incrementing has meaning only
		// if we a going to ascend next branch

		// continue
	}

	return
}

// Descend iterates over all values descending order until
// first error or the end. Use ErrStopIteration to
// break iteration. Any error is returned by given function
// (except the ErrStopIteration) is returned by the Descend()
// method
//
// The Descend allows to cahnge the Refs inside given
// ascendFunc. It's possile to iterate inside the descendFunc
// too
func (r *Refs) Descend(pack Pack, descendFunc IterateFunc) (err error) {

	// we have to initialize the Refs here to get actual length of
	// the Refs to use it as argument to the descendFrom method

	if err = r.initialize(pack); err != nil {
		return
	}

	return r.descendFrom(pack, r.length-1, descendFunc)
}

func (r *Refs) descendFrom(pack Pack, from int,
	descendFunc IterateFunc) (err error) {

	// the Refs are already initialized by Descend or DescendFrom methods

	if err = validateIndex(from, r.length); err != nil {
		return
	}

	if r.length == 0 {
		return // empty Refs
	}

	r.startIterating()
	defer r.stopIterating()

	var rewind bool       // find next element from root of the Refs
	var i = from          // i is next index to find
	var si = r.length - 1 // subtree strting index (from the end)

	for {
		// - pass is number of elements iterated (not skipped)
		//        for current step
		// - rewind means that we need to find next element from
		//        root of the Refs, because the Refs has been
		//        changed
		// - err is loading error, malformed Refs error or
		//        ErrStopIteration
		pass, rewind, err = r.descend(pack, si, i, descendFunc, r.depth, nil,
			r.leafs, r.branches)
		// so, we need to find next element from root of the Refs
		if rewind == true {
			i -= pass // shift the i to don't repeat elements
			continue  // find next index from root of the Refs
		}
		// the loop is necesary for rewinding only
		break
	}

	if err == ErrStopIteration {
		err = nil // clear the ErrStopIteration
	}

	return

}

// descend depending on depth and i (starting element), check
// changes after every call of the descendFunc
//
// arguments:
//  - pack is Pack instance
//  - si is index from which the subtree starts from (from the end)
//  - i is starting element
//  - descendFunc is given function
//  - depth is current depth level (0 for leafs)
//  - upper is upper *refsNode or nil if it's the Refs (required for loading)
//  - leafs are leafs of current *refsNode or of the Refs
//  - branches are branches of current *refsNode or of the Refs
//
// reply:
//  - pass is number of lements skipped or iterated
//  - rewind is true if finding next index from root of the Refs is required
//  - err is some error
//
func (r *Refs) descend(pack Pack, si, i int, descendFunc IterateFunc, depth int,
	upper *refsNode, leafs []*refsElement, branches []*refsNode) (pass int,
	rewind bool, err error) {

	if depth == 0 {
		return r.descendLeafs(si, i, descendFunc, leafs)
	}

	// else if depth > 0

	return r.descendBranches(pack, si, i, descendFunc, depth, upper, branches)
}

func (r *Refs) descendLeafs(si, i int, descendFunc IterateFunc,
	leafs []*refsElement) (pass int, rewind bool, err error) {

	// so, the si is global index of last element of the leafs;
	// we have to find i-th index to start iterating from it

	// use the si as 'skip' counter, e.g. the si keeps
	// current global index, since the si is local variabe we
	// are free to decrement it

	for k := len(leafs) - 1; k >= 0; k-- {
		el := leafs[k]

		if el.Deleted == true {
			continue // deleted elements do not exist
		}

		// the i can't be greater then the si, only less or equal

		if i < si {
			si-- // skip current element finding i-th
			continue
		}

		// here i == si

		if err = descendFunc(i, el.Hash); err != nil {
			return
		}

		// decrement all counters
		i--    // current index
		si--   // skip (not really skip)
		pass++ // processed elements

		// check out changes of the Refs
		if rewind = r.isFindingIteratorsIndexFromRootRequired(); rewind {
			// we need to find next element from root of the Refs,
			// because the ascendFunc changes the Refs tree
			return
		}

		// no changes required, continue
	}

	return
}

func (r *Refs) descendBranches(pack Pack, si, i int, descendFunc IterateFunc,
	depth int, upper *refsNode, branches []*refsNode) (pass int, rewind bool,
	err error) {

	// we are using the si as 'skip' counter, the si will keep
	// current global index; e.g. for every branch the si
	// will keep its ending global index

	var subpass int // pass for a subtree (for a branch)

	for k := len(branches) - 1; k >= 0; k-- {
		br := branches[k]

		if err = r.loadRefsNodeIfNeed(pack, br, depth, upper); err != nil {
			return // eror loading the branch
		}

		if br.length == 0 {
			continue // skip empty branches
		}

		if i < si-br.length {
			si -= br.length // skip this branch finding i-th element
			continue
		}

		subpass, rewind, err = r.descend(pack, si, i, descendFunc, depth-1, br,
			br.leafs, br.branches)

		pass += subpass // process elements

		if err != nil || rewind == true {
			return // some error or changes
		}

		si -= subpass // e.g. si -= br.length
		i -= subpass  // decrement current index

		// these are local variables and decrementing has meaning only
		// if we a going to descend next branch

		// continue
	}

	return
}

// AscendFrom iterates over all values ascending order starting
// from the 'from' element until first error or the end. Use
// ErrStopIteration to break iteration. Any error is returned
// by given function (except the ErrStopIteration) is returned
// by the AscendFrom() method
func (r *Refs) AscendFrom(pack Pack, from int, ascendFunc IterateFunc) error {

	return r.ascendFrom(pack, from, ascendFunc)
}

// DescendFrom iterates over all values descending order starting
// from the 'from' element until first error or the end. Use
// ErrStopIteration to break iteration. Any error is returned
// by given function (except the ErrStopIteration) is returned
// by the DescendFrom() method
func (r *Refs) DescendFrom(pack Pack, from int,
	descendFunc IterateFunc) (err error) {

	// we have to initialize the Refs here to find its length

	if err = r.initialize(pack); err != nil {
		return
	}

	return r.descendFrom(pack, from, descendFunc)
}

// AppendValues appends given objects to the Refs. You
// must be sure that type of this obejcts is type of this
// Refs. All nil-interfaces will be treated as blank
// references (cipher.SHA256{})
func (r *Refs) AppendValues(pack Pack, objs ...interface{}) (err error) {

	if len(objs) == 0 {
		return // nothing ot append
	}

	hashes := make([]cipher.SHA256, 0, len(objs))

	var hash cipher.SHA256

	for _, obj := range objs {
		if obj == nil {
			hashes == append(hashes, cipher.SHA256{}) // nil = blank
			continue
		}
		if hash, err = pack.Add(encoder.Serialize(obj)); err != nil {
			return
		}
		hashes = append(hashes, hash)
	}

	return r.AppendHashes(pack, hashes)
}

func initializeRefs(pack Pack, rs ...*Refs) (err error) {
	for _, r := range rs {
		if err = r.initialize(pack); err != nil {
			return
		}
	}
	return
}

// AppendRefs appends another Refs to this one
func (r *Refs) AppendRefs(pack Pack, refs *Refs) (err error) {

	if err = initializeRefs(pack, r, refs); err != nil {
		return
	}

	// TODO (kostyarin): implement

	return
}

// freeSpaceOnTail returns free space on tail of the Refs
// we can fill with new elements; if we want to append
// something to the Refs we can use the space without
// unnecessary rebuiling (wihtout increasing depth)
func (r *Refs) freeSpaceOnTail(pack Pack, depth int, upper *refsNode,
	leafs []*refsElement, branches []*refsNode) (fst int, err error) {

	// the Refs mut be initialized here

	if depth == 0 {
		// look the leafs
		fst = r.freeSpaceOnTailInLeafs(leafs)
		return
	}

	// else -> look the branches
	return r.freeSpaceOnTailInBranches(pack, depth, upper, branches)
}

func (r *Refs) freeSpaceOnTailInLeafs(leafs []*refsElement) (fst int) {
	// so, every leaf can contain one element, and
	// max length of leafs is r.degree, but we
	// should check deleted elements; e.g. we can't
	// just degree - len(leafs)

	fst = r.degree - len(leafs)

	// iterate the leafs from end to find
	// not deleted element

	for k := len(leafs) - 1; k >= 0; k-- {
		if leafs[k].Deleted == true {
			fst++ // it's free
			continue
		}
		break // no more free space in the leafs
	}

	return
}

func (r *Refs) freeSpaceOnTailInBranches(pack Pack, depth int, upper *refsNode,
	branches []*refsNode) (fst int, err error) {

	var bfst int // free space of a branch
	var tem int  // free elements of totally empty branch

	// every branch can keep (degree**depth+1) elements,
	// but since the depth is (real depth - 1), and depth
	// of a branch is depth - 1 (because the depth argument is
	// depth of branch that keeps this this branches argument,
	// and 'depth - 1' is depth of branches or leafs of elements
	// of the branches argument), and the depth argument is
	// greater then 0; thus the tem will be degree**depth

	// and the tem is max elements of a branch,
	// but if the branch has this number of free
	// elements, then it is empty

	tem = pow(r.degree, depth)

	// check out fullness of the branches

	// if length of the branches argument is not r.degree, then
	// if contains free 'r.degree - len(branches)' branches
	// length of which is 'r.degree**depth' (the tem)

	if fst = r.degree - len(r.branches); fst > 0 {
		fst *= tem // real value
	}

	// add all totally empty branches from end

	for k := len(branches) - 1; k >= 0; k-- {

		br := branches[k]

		if err = r.loadRefsNodeIfNeed(pack, br, depth-1, upper); err != nil {
			return
		}

		// totally empty branch contains degree**depth free elements;

		// so, we are string from the end, and if free space
		// of a branch is the tem, then we look next (previous)
		// branch

		if br.length == 0 {
			fst += tem // yet another empty branch
			continue
		}

		bfst, err = r.freeSpaceOnTail(pack, depth-1, br, br.leafs, br.branches)
		if err != nil {
			return // loading error
		}

		fst += bfst // add free places

		if bfst == tem {
			// the branch turns totally empty
			continue // look next (previous)
		}

		break // enough
	}

	return
}

// a**b (power)
func pow(a, b int) (p int) {
	p = 1
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// dpethToFit returns depth (real depth - 1); the
// Refs with this depth can fit the 'fit' elements
//
// arguments:
// - degree is degree of the Refs
// - start is strting depth (that don't fit)
// - fit is number of elements to fit
func depthToFit(degree, start, fit int) int {
	// the start is 'real depth - 1', thus we add 2 instead of
	// 1 (e.g. we add the elided 1 and one more 1)
	for start += 2; pow(degree, start) < fit; start++ {
	}
	return start - 1 // found (-1 turs the start to be Refs.depth)
}

// increaseDepth to fit 'fit' elements; the method replaces
// this Refs with another that contains the same elements,
// but has enough dpeth to fit the 'fit' elements;
// hases and lengths of every node of the Refs including
// the Refs structure are empty and should be filled after;
// the Refs must to be initilized
func (r *Refs) increaseDepth(pack Pack, fit int) (err error) {
	// since, max number of elements, that the Refs can fit,
	// is pow(r.degree, r.depth+1), then required depth is
	// the base r.degree logarithm of fit; but since we need
	// interger number, we are using loop to find the number

	depth := depthToFit(r.degree, r.depth, fit)

	// now the depth is depth we need

	var nr Refs // new Refs

	nr.degree = r.degree
	nr.depth = depth
	nr.flags = r.flags

	// copy iterators, because we can do that inside and IterateFunc
	nr.iterators = r.iterators

	if nr.flags&HashTableIndex != 0 {
		nr.index = make(map[cipher.SHA256][]*refsElement)
	}

	// build new refs without hashes and length;
	// the hashes and lengths will be filled later

	// TOTH (kostyarin): may be there is a faster way?

	err = r.Ascend(pack, func(_ int, hash cipher.SHA256) (err error) {
		var ok bool
		nr.leafs, nr.branches, ok = nr.appendIgnoringDeleted(pack, hash,
			nr.depth, nil, nr.leafs, nr.branches)
		if !ok {
			panic("can't append to fresh Refs (increasing depth)")
		}
		return // continue
	})

	if err != nil {
		return // keep the Refs untouched
	}

	// TODO (kostyarin): walk from tail setting length 'n' hash fileds

	*r = rn // replace
	return
}

// appendIgnoringDeleted append given hash to the Refs ignoring
// deleted elements (e.g. the Refs must be freshly created, without
// deleted elements). The method sets length and hash fields only for
// full branches. E.g. after this call we have to walk from end to
// set hsah and length firelds for non-full branches and for the
// Refs itself; the appendIgnoringDeleted returns actual (new) values
// of leafs and branches every time (even if some error occurs);
// it returns true if givne hash has been appended successfully;
// if it returns an error then the error is error of Pack.Add;
// it returns false if given leafs+branches can't fit one more
// hash
func (r *Refs) appendIgnoringDeleted(pack Pack, hash cipher.SHA256, depth int,
	upper *refsNode, leafs []*refsElement,
	branches []*refsNode) (newLeafs []*refsElement, newBranches []*refsNode,
	ok bool) {

	newLeafs, newBranches = leafs, branches // return actual values allways

	if depth == 0 {
		newLeafs, ok = r.appendToLeafsIgnoringDeleted(pack, hash, upper,
			leafs)
		return
	}

	newBranches, ok = r.appendToBranchesIgnoringDeleted(pack, hash, depth,
		upper, branches)
	return
}

func (r *Refs) appendToLeafsIgnoringDeleted(pack Pack, hash cipher.SHA256,
	upper *refsNode, leafs []*refsElement) (newLeafs []*refsElement, ok bool) {

	newLeafs = leafs // return actual leafs every time

	if len(leafs) == r.degree {
		return // leafs, false, nil (can't append anymore)
	}

	re := &refsElement{
		Hash:  hash, // Deleted: false,
		upper: upper,
	}

	newLeafs = append(leafs, re)

	if r.flags&HashTableIndex != 0 {
		r.addElementToIndex(hash, re)
	}

	return
}

func (r *Refs) appendToBranchesIgnoringDeleted(pack Pack, hash cipher.SHA256,
	depth int, upper *refsNode, branches []*refsNode) (newBranches []*refsNode,
	ok bool) {

	newBranches = branches // return actual branches every time

	// first of all, we are trying to append
	// to last branch, and then checks len-degree

	if len(branches) > 0 {
		br := branches[len(branches)-1]

		br.branches, br.leafs, ok = r.appendIgnoringDeleted(pack, hash,
			depth-1, br, br.leafs, br.branches)
		if ok == true || err != nil {
			return
		}

		// not ok and err == nil
	}

	if len(branches) == r.degree {
		return // branches, false, nil (can't append anymore)
	}

	br := &refsNode{
		upper: upper,
		root:  r, // TODO (kostyarin): the field is never used
	}

	newBranches = append(branches, br)
	br.branches, br.leafs, ok = r.appendIgnoringDeleted(pack, hash,
		depth-1, br, br.leafs, br.branches)

	return
}

// AppendHashes appends given hashes to the Refs. You must be sure that
// type of the obejcts (the hashes refers to) is type of the Refs
func (r *Refs) AppendHashes(pack Pack, hashes ...cipher.SHA256) (err error) {

	if len(hashes) == 0 {
		return // nothing to append
	}

	if err = r.initialize(pack); err != nil {
		return
	}

	var fst int // free space on tail

	fst, err = r.freeSpaceOnTail(pack, r.depth, nil, r.leafs, r.branches)
	if err != nil {
		return // loading error
	}

	if len(hashes) > fst {
		// so, we have to rebuild the Refs tree increasing its depth
		// to fit all elements including new

		if err = r.increaseDepth(pack, len(hashes)+r.length); err != nil {
			return // loading error
		}

	}

	// so, now we are sure that the Refs can fit new elements

	// TODO (kostyarin): append

	return
}

// Clear the Refs making them blank
func (r *Refs) Clear() (err error) {

	// TODO (kostyarin): iterating

	r.Hash = cipher.SHA256{}

	r.depth = 0
	r.degree = 0

	r.index = nil

	r.length = 0

	r.leafs = nil
	r.branches = nil

	r.flags = 0

	r.modified = false

	return nil
}

// Rebuild the Refs if need. It's impossible to rebuild
// while the Refs is iterating. In most cases you should
// not call the Rebuild, beacuse it's called automatically
// by Pack during saving. The Rebuild rebuilds the Refs tree
// to choose best (compact and fast) depth removing deleted
// elements (all deleted elements still present in the tree
// after CutElementByIndex and CutSliceByIndex that not
// implemented yet). Also, the Refs tree can grows horizontally
// in some cases to speed up some operations. But after the
// grow the tree is not to fast to access elements and should
// be rebuilt. The Rebuild solves all this issues. If the Refs
// tree has not been changed, then the Rebuild does nothing.
// Also, the Rebuild can keep some dleted elements if if their
// removal will not change depth of the Refs tree.
//
// Also, the depth is the height of the Refs tree, but root
// of the tree is above all. Ha-ha
func (r *Refs) Rebuild(pack Pack) (err error) {

	if r.iterating > 0 {
		return ErrRefsIsIterating // can't rebuild while the Refs is iterating
	}

	// (degree + 1) ^ depth == real length

	// TODO (kostyarin): rebuild

	return
}

// Len(pack Pack) (ln int, err error)
//
// VaueByHash(pack Pack, hash cipher.SHA256, obj interface{}) (err error)
// ValueByHashWithIndex(pack Pack, hash cipher.SHA256, obj interface{}) (i int, err error)
// ValueByIndex(pack Pack, i int, obj interface{}) (hash cipher.SHA256, err error)
//
// DelByIndex(pack Pack, i int) (err error)
// Cut(pack Pack, i, j int) (err error)
//
// Slice(pack Pack, i, j int) (err error)
//
// Ascend(pack Pack, func(i int, hash cipher.SHA256)) (err error)
// Descend(pack Pack, func(i int, hash cipher.SHA256)) (err error)
//
// AscendFrom(pack Pack, from int, func(i int, hash cipher.SHA256)) (err error)
// DescendFrom(pack Pack, from int, func(i int, hash cipher.SHA256)) (err error)
//
// Append(pack Pack, obj ...interface{}) (err error)
// AppendRefs(pack Pack, refs *Refs) (err error)
// AppendHashes(pack Pack, hash ...cipher.SHA256) (err error)
//
// Clear()
// Rebuild(pack Pack) (err error)

/*

// A Refs represetns array of references.
// The Refs contains RefsElem(s). To delete
// an element use Delete method of the
// RefsElem. It's impossible to reborn
// deleted RefsElem. To cahgne Hash field
// of the Refs use SetHash method
type Refs struct {
	Hash cipher.SHA256 // hash of the Refs

	// the Hash is also hash of nested nodes

	depth  int `enc:"-"` // not stored
	degree int `enc:"-"` // not stored

	length   int         `enc:"-"`
	branches []*Refs     `enc:"-"` // branches
	leafs    []*RefsElem `enc:"-"` // leafs

	upper *Refs                       `enc:"-"` // upper node (nil for root)
	index map[cipher.SHA256]*RefsElem `enc:"-"` // index (or reference)

	// lock
	// - don't rebuild the tree in some cases
	// - don't allow Append, Ascend, Descend
	//   methods in some cases
	iterating bool `enc:"-"`

	ch bool `enc:"-"` // true if the Refs has been changed or fresh
}

func (r *Refs) encode(root bool, depth int) (val []byte) {
	if root {
		var ers encodedRefs
		ers.Degree = uint32(r.degree)
		ers.Length = uint32(r.length)
		ers.Depth = uint32(r.depth)
		if depth == 0 {
			ers.Nested = make([]cipher.SHA256, 0, len(r.leafs))
			for _, leaf := range r.leafs {
				if leaf.Hash == (cipher.SHA256{}) {
					continue
				}
				ers.Nested = append(ers.Nested, leaf.Hash)
			}
		} else {
			ers.Nested = make([]cipher.SHA256, 0, len(r.branches))
			for _, br := range r.branches {
				if br.Hash == (cipher.SHA256{}) {
					continue
				}
				ers.Nested = append(ers.Nested, br.Hash)
			}
		}
		val = encoder.Serialize(&ers)
	} else {
		var ern encodedRefsNode
		ern.Length = uint32(r.length)
		if depth == 0 {
			ern.Nested = make([]cipher.SHA256, 0, len(r.leafs))
			for _, leaf := range r.leafs {
				if leaf.Hash == (cipher.SHA256{}) {
					continue
				}
				ern.Nested = append(ern.Nested, leaf.Hash)
			}
		} else {
			ern.Nested = make([]cipher.SHA256, 0, len(r.branches))
			for _, br := range r.branches {
				if br.Hash == (cipher.SHA256{}) {
					continue
				}
				ern.Nested = append(ern.Nested, br.Hash)
			}
		}
		val = encoder.Serialize(&ern)
	}
	return
}

// IsBlank returns true if the Refs is blank
func (r *Refs) IsBlank() bool {
	return r.Hash == (cipher.SHA256{})
}

// Eq returns true if given Refs is the same
// as this one (ccompare hashes only)
func (r *Refs) Eq(x *Refs) bool {
	return r.Hash == x.Hash
}

// Short string
func (r *Refs) Short() string {
	return r.Hash.Hex()[:7]
}

// String implements fmt.Stringer interface
func (r *Refs) String() string {
	return r.Hash.Hex()
}

// Developer:
//
// There are 3 states of Refs.
// 1) Only Hash field has meaning
// 2) Branches or leafs contains (1) state nodes
// 3) Branches or leafs are loaded
//
// So, if Hash is blank. Then nothing to load. In this case all (1), (2)
// and (3) states are equal. If the Hash is not blank, then the node
// has nested nodes (and non-zero length). Thus, if length is zero, then
// the Hash should be cleared
//
// If HashTableIndex, EntireTree or EntireMerkleTree flags set,
// then all nodes should be loaded to (3) state if possible

func (r *Refs) isLoaded() bool {
	if r.Hash == (cipher.SHA256{}) {
		return true
	}
	return r.length > 0
}

func (r *Refs) isRoot() bool {
	return r.upper == nil
}

// setup wn field of leaf-Ref
func (r *Refs) setupInternalsOfLeaf(leaf *RefsElem) {
	leaf.rn = r.rn
	leaf.upper = r
}

// the r is root
func (r *Refs) attach() {
	if r.depth == 0 {
		if len(r.leafs) == 0 {
			return
		}
		if up := r.leafs[0].upper; up == r {
			return // everything is ok already
		} else if up != nil {
			up.leafs = nil // clear
			up.rn = nil    // detach
		}
		for _, leaf := range r.leafs {
			leaf.upper = r // re-attach
		}
		return
	}
	if len(r.branches) == 0 {
		return
	}
	if up := r.branches[0].upper; up == r {
		return // everything is ok already
	} else if up != nil {
		up.branches = nil // clear
		up.rn = nil       // detach
	}
	for _, br := range r.branches {
		br.upper = r // re-attach
	}
}

// from (1) to (2)
func (r *Refs) load(depth int) (err error) {

	if r.rn == nil {
		return errors.New("Refs is not attached to Pack")
	}

	// since we are using non-pinter values we must to know,
	// are nested nodes points to the r
	r.attach()

	if r.isLoaded() {
		return // already loaded
	}

	if r.isRoot() && r.rn.pack.flags&HashTableIndex != 0 {
		r.rn.index = make(map[cipher.SHA256]*RefsElem)
	}

	var val []byte
	if val, err = r.rn.pack.get(r.Hash); err != nil {
		return
	}

	var nested []cipher.SHA256 // nested nodes

	if r.isRoot() {
		var er encodedRefs
		if err = encoder.DeserializeRaw(val, &er); err != nil {
			return
		}
		r.depth = int(er.Depth)   // get depth from database
		r.degree = int(er.Degree) // get degree from database
		r.length = int(er.Length) // get length from database

		depth = r.depth // it's root and we are using depth from database

		nested = er.Nested // nested nodes or leafs
	} else {
		var ern encodedRefsNode
		if err = encoder.DeserializeRaw(val, &ern); err != nil {
			return
		}
		r.length = int(ern.Length) // length of the node

		nested = ern.Nested // nested nodes
	}

	if depth == 0 {

		// the nested contains leafs

		r.leafs = make([]*RefsElem, 0, len(nested))

		for _, hash := range nested {

			leaf := new(RefsElem)
			leaf.Hash = hash
			r.setupInternalsOfLeaf(leaf)

			r.leafs = append(r.leafs, leaf)

			if r.rn.pack.flags&HashTableIndex != 0 {
				r.rn.index[hash] = leaf
			}

			if r.rn.pack.flags&EntireTree != 0 {
				if _, err = leaf.Value(); err != nil { // load value of the leaf
					return
				}
			}

		}

		return
	}

	// the nested contains branches

	r.branches = make([]*Refs, 0, len(nested))

	for _, hash := range nested {

		branch := new(Refs)

		branch.Hash = hash

		branch.rn = r.rn
		branch.upper = r

		r.branches = append(r.branches, branch)

		if r.rn.pack.flags&(EntireTree|EntireMerkleTree|HashTableIndex) != 0 {
			// go deepper
			if err = branch.load(depth - 1); err != nil {
				return
			}
		}

	}

	return
}

// load a Refs and get it length (or error)
func (r *Refs) len(depth int) (ln int, err error) {
	if err = r.load(depth); err != nil {
		return
	}
	return r.length, nil
}

// Len returns length of the Refs. The length is amount of
// non-blank elements. It returns error if the Refs can't
// be loaded from database
func (r *Refs) Len() (ln int, err error) {
	return r.len(0)
}

// RefByIndex returns element by index. To delete an element
// by index use RefByIndex and then Clear result (if found).
// You also can set another value using this approach.
func (r *Refs) RefByIndex(i int) (ref *RefsElem, err error) {
	if err = r.load(0); err != nil {
		return
	}
	if err = validateIndex(i, r.length); err != nil {
		return
	}
	ref, err = r.refByIndex(r.depth, i)
	return
}

func (r *Refs) refByIndex(depth, i int) (ref *RefsElem, err error) {

	// the r is loaded
	// i >= 0

	if depth == 0 {
		// i is index in r.leafs
		if i >= len(r.leafs) {
			err = fmt.Errorf(
				"malformed tree: not enough leafs in {%s}, leafs: %d, want: %d",
				r.Hash.Hex()[:7], len(r.leafs), i)
			return
		}

		ref = r.leafs[i]
		return
	}

	// find branch subtracting length

	// so, we need to load branches from database to
	// get actual lengths

	for _, br := range r.branches {

		var ln int // length
		if ln, err = br.len(depth - 1); err != nil {
			return
		}

		// so if i less then the ln, then the br is branch we are looking for
		if i < ln {
			// the br is loaded by the len call above
			ref, err = br.refByIndex(depth-1, i) //
			return                               // done
		}

		i -= ln // subtract and go further
	}

	// TODO (kostyarin): explain the error, add hash and any other information
	//                   to find broken branch
	err = errors.New("malformed tree: index not found in branches")
	return
}

func (r *Refs) refByHashWihtoutHashTable(hash cipher.SHA256) (i int,
	needle *RefsElem, err error) {

	if r.length == 0 {
		err = ErrNotFound
		return // 0, nil, {not found}
	}

	_, err = r.descend(r.depth, r.length-1,
		func(k int, el *RefsElem) (_ error) {
			if el.Hash == hash {
				needle, i = el, k
				return ErrStopIteration
			}
			return
		})
	if err == ErrStopIteration {
		err = nil
	}
	if err == nil && needle == nil {
		err = ErrNotFound
	}
	return
}

// RefByHash returns Ref by its hash. If HashTableIndex flag
// is set, and there are many values with the same hash,
// then which value will be retuned is not defined. If the
// flag is not set, then it returns last value
func (r *Refs) RefByHash(hash cipher.SHA256) (needle *RefsElem, err error) {

	if err = r.load(0); err != nil {
		return
	}

	if r.rn.pack.flags&HashTableIndex != 0 {
		if needle = r.rn.index[hash]; needle == nil {
			err = ErrNotFound
		}
		return
	}

	_, needle, err = r.refByHashWihtoutHashTable(hash)
	return
}

// RefByHashWithIndex returns RefsElem by its hash and index of the
// element. It returns ErrNotFound if the needle has not been found
func (r *Refs) RefByHashWithIndex(hash cipher.SHA256) (i int, needle *RefsElem,
	err error) {

	if err = r.load(0); err != nil {
		return
	}

	if r.rn.pack.flags&HashTableIndex != 0 {
		var ok bool
		if needle, ok = r.rn.index[hash]; !ok {
			err = ErrNotFound
			return // 0, nil, ErrNotFound
		}
		// walk up
		if needle.upper == nil {
			err = errors.New("the RefsElem is detached from Refs-tree" +
				" (it shoud not happen, please contact CXO developer)")
			return
		}
		// upper of the needle contans leafs,
		// all nodes above contains branches

		// check the needle.upper
		for _, leaf := range needle.upper.leafs {
			if leaf == needle {
				break
			}
			i++
		}

		cur := needle.upper // the node that contains leafs (the needle holder)
		if cur.upper == nil {
			return // we're done
		}

		// the cur already checked out, take a look above

		for up := cur.upper; up != nil; cur, up = up, up.upper {
			// so, we need to check out branches only,
			// because the needle.upper contains leafs, but
			// all nodes above contains brances

			for _, br := range up.branches {
				if br == cur {
					break
				}
				i += br.length
			}

		}
		return
	}

	i, needle, err = r.refByHashWihtoutHashTable(hash)
	return
}

// reduce depth, the r is root, the r is loaded
func (r *Refs) reduceIfNeed() (err error) {
	if r.rn.iterating == true {
		return // don't reduce we are inside Ascend or Descend
	}
	var depth = r.depth
	for ; depth > 0 && pow(r.degree, depth-1) >= r.length; depth-- {
	}
	if depth != r.depth {
		err = r.changeDepth(depth)
	}
	return
}

// An IterateRefsFunc represents iterator over Refs.
// Feel free to delete and modify elements inside the
// function. Don't modify the Refs using Append, Clear
// or similar inside the function. Use ErrStopIteration
// to stop the iteration
type IterateRefsFunc func(i int, ref *RefsElem) (err error)

// Ascend iterates over Refs ascending order. See also
// docs for IterateRefsFunc. It's safe to delete an element
// of the Refs inside the Ascend, but it's unsafe to
// append something to the Refs while the Append ecxecutes
func (r *Refs) Ascend(irf IterateRefsFunc) (err error) {

	if err = r.load(0); err != nil {
		return
	}

	// we can't rebuild the tree while iterating,
	// but we can rebuild it after

	r.rn.iterating = true                     // lock
	defer func() { r.rn.iterating = false }() // unlock

	_, err = r.ascend(r.depth, 0, irf)
	if err != nil && err != ErrStopIteration {
		return
	}

	r.rn.iterating = false // unlock
	err = r.reduceIfNeed() // overwrite the ErrStopIteration if so
	return
}

func (r *Refs) ascend(depth, i int, irf IterateRefsFunc) (pass int, err error) {

	// root already loaded
	if err = r.load(depth); err != nil {
		return
	}

	if r.length == 0 {
		return // nothing to iterate
	}

	if depth == 0 { // leafs
		for _, leaf := range r.leafs {
			if leaf.Hash == (cipher.SHA256{}) {
				continue
			}
			if err = irf(i, leaf); err != nil {
				return
			}
			i++
			pass++
		}
		return
	}

	// branches
	for _, br := range r.branches {
		if br.Hash == (cipher.SHA256{}) {
			continue
		}
		var subpass int
		if subpass, err = br.ascend(depth-1, i, irf); err != nil {
			return
		}
		i += subpass
		pass += subpass
	}

	return
}

// Descend iterates over Refs descending order. See also
// docs for IterateRefsFunc. It's safe to delete an element
// of the Refs inside the Ascend, but it's unsafe to
// append something to the Refs while the Append ecxecutes
func (r *Refs) Descend(irf IterateRefsFunc) (err error) {

	if err = r.load(0); err != nil {
		return
	}

	if r.length == 0 {
		return
	}

	// we can't rebuild the tree while iterating,
	// but we can rebuild it after

	r.rn.iterating = true
	defer func() { r.rn.iterating = false }()

	_, err = r.descend(r.depth, r.length-1, irf)
	if err != nil && err != ErrStopIteration {
		return
	}

	r.rn.iterating = false // unlock
	err = r.reduceIfNeed()

	return
}

func (r *Refs) descend(depth, i int, irf IterateRefsFunc) (pass int,
	err error) {

	// root already loaded

	if err = r.load(depth); err != nil {
		return
	}

	if r.length == 0 {
		return // nothing to iterate
	}

	if depth == 0 { // leafs
		for k := len(r.leafs) - 1; k >= 0; k-- {
			leaf := r.leafs[k]
			if leaf.Hash == (cipher.SHA256{}) {
				continue
			}
			if i < 0 {
				// TODO (kostyarin): detailed error
				err = errors.New(
					"negative index during Descend: malformed tree")
				return
			}

			if err = irf(i, leaf); err != nil {
				return
			}

			i--
			pass++
		}
		return
	}

	// branches

	for k := len(r.branches) - 1; k >= 0; k-- {
		br := r.branches[k]
		if br.Hash == (cipher.SHA256{}) {
			continue
		}
		if i < 0 {
			// TODO (kostyarin): detailed error
			err = errors.New(
				"negative index during Descend: malformed tree")
			return
		}

		var subpass int
		if subpass, err = br.descend(depth-1, i, irf); err != nil {
			return
		}

		i -= subpass
		pass += subpass
	}

	return
}

// free spece that we can use to append new items;
// for example the tree can contain 1000 elements
// and contains only 980, and the tree has 10
// zero elements; and this zero-elements not on tail;
// thus we can insert 10 elements only; if we need
// to insert more, then we need to rebuild the tree
// removing zero elements (and possible increasing
// depth); but if we have enough place on tail then
// we can aviod unnecessary rebuilding
//
// the fsot is free space on tail of the node
func (r *Refs) freeSpaceOnTail(degree, depth int) (fsot int, err error) {

	if err = r.load(depth); err != nil {
		return
	}

	// r.degree is amount o branches or leafs per node
	// thus every node can contain (r.degree ** (depth+1))
	// (we are using depth+1, because the depth is 'actual depth'-1)

	if depth == 0 {
		fsot = degree - len(r.leafs)
		return
	}

	if len(r.branches) == 0 {
		fsot = pow(degree, depth+1)
		return
	}

	// empty branches
	if eb := degree - len(r.branches); eb > 0 {
		fsot = eb * pow(degree, (depth-1)+1)
	}

	// check out last branch
	lastBranch := r.branches[len(r.branches)-1]

	var lb int
	if lb, err = lastBranch.freeSpaceOnTail(degree, depth-1); err != nil {
		return
	}

	fsot += lb
	return
}

// try to insert given reference to the node;
//
// the el is not blank
// the tryInsert doesn't cahnge 'length' and 'Hash' fields,
// thus we need to walk the tree (from tail or whole tree) and
// check the length and change hashes if the length has been changed
func (r *Refs) tryInsert(degree, depth int, el *RefsElem) (ok bool,
	err error) {

	if err = r.load(depth); err != nil {
		return
	}

	if depth == 0 {

		// we are not replacing zero elements with new

		if len(r.leafs) >= degree {
			return // can not
		}

		// setup the el and insert
		r.setupInternalsOfLeaf(el)

		if r.rn.pack.flags&HashTableIndex != 0 {
			r.rn.index[el.Hash] = el // insert to the index
		}

		r.leafs, ok = append(r.leafs, el), true // inserted
		return
	}

	// 1) find last branch and try to insert, return if ok
	// 2) create new branch if possible and insert

	if len(r.branches) > 0 {

		lb := r.branches[len(r.branches)-1]
		if ok, err = lb.tryInsert(degree, depth-1, el); err != nil || ok {
			return // success or error
		}

	}

	// is there a free space for new branch?

	if len(r.branches) >= degree {
		return // can not insert
	}

	// add new branch and insert

	nb := new(Refs) // new branch

	nb.rn = r.rn
	nb.upper = r

	if ok, err = nb.tryInsert(degree, depth-1, el); err != nil {
		return // never happens (a precaution)
	}

	if !ok {
		panic("fatality: bug in the code")
	}

	r.branches = append(r.branches, nb)
	return
}

// save the Refs (not recursive)
func (r *Refs) save(depth int) {

	var nl int                 // new length
	var nested []cipher.SHA256 // nested elements

	if depth == 0 {
		nested = make([]cipher.SHA256, 0, len(r.leafs))
		for _, leaf := range r.leafs {
			if leaf.Hash == (cipher.SHA256{}) {
				continue
			}
			nl++
			nested = append(nested, leaf.Hash)
		}
	} else {
		nested = make([]cipher.SHA256, 0, len(r.branches))
		for _, br := range r.branches {
			if br.Hash == (cipher.SHA256{}) {
				continue
			}
			nl += br.length
			nested = append(nested, br.Hash)
		}
	}

	r.length = nl // store actual (possible new) length
	if nl == 0 {
		// clear (don't save blank Refs)
		r.depth = 0
		r.leafs, r.branches = nil, nil
		if r.Hash != (cipher.SHA256{}) {
			r.Hash, r.ch = cipher.SHA256{}, true // changed
		}
		return
	}

	if r.isRoot() {
		// root Refs represented as encodedRefs
		var er encodedRefs            // encode to save and get hash
		er.Degree = uint32(r.degree)  // keep in database
		er.Depth = uint32(depth)      // keep in database
		er.Length = uint32(nl)        // keep in database
		er.Nested = nested            // actually, keep in database
		hash, _ := r.rn.pack.save(er) // save into cache
		if hash != r.Hash {
			r.Hash, r.ch = hash, true
		}
	} else {
		// non-root Refs represented as encodedRefsNode
		var ern encodedRefsNode
		ern.Nested = nested            // keep in database
		ern.Length = uint32(nl)        // keep in daabase
		hash, _ := r.rn.pack.save(ern) // save into cache
		if hash != r.Hash {
			r.Hash, r.ch = hash, true
		}
	}
	return
}

// walk from tail to fild nodes that has incorrect 'Hash' and 'length'
// fields to set proper; the method doesn't check hashes of nested
// nodes or leafs, it checks only lengths; e.g. the method is useful
// after insert (Append or cahngeDepth); it can't be used after Ascend
// or Descend (if someone cahnges or removes elements while iterating),
// because it walks from tail and stops on first correct (unchanged) node
func (r *Refs) updateHashLengthFieldsFromTail(depth int) (set bool, err error) {

	if err = r.load(depth); err != nil {
		return
	}

	var al int // actual length

	if depth == 0 {
		for _, leaf := range r.leafs {
			if leaf.Hash == (cipher.SHA256{}) {
				continue
			}
			al++
		}
		if al != r.length { // has been changed
			r.length, set = al, true
			r.save(depth)
		}
		return
	}

	var bset = true // set of branch (set to true to check first from tail)

	for i := len(r.branches) - 1; i >= 0; i-- {
		br := r.branches[i]
		if br.Hash == (cipher.SHA256{}) {
			if len(br.branches) == 0 && len(br.leafs) == 0 {
				continue
			}
		}
		if bset {
			// we here only if this branch is first from tail or
			// previous (from tail) branch has been updated
			bset, err = br.updateHashLengthFieldsFromTail(depth - 1)
			if err != nil {
				return
			}
			set = set || bset
		}
		al += br.length // actual length
	}

	if al != r.length || set { // has been changed
		r.length, set = al, true
		r.save(depth)
	}

	return
}

// change depth of the tree or remove zero elements
//
// the r is loaded
func (r *Refs) changeDepth(depth int) (err error) {

	if depth < 0 {
		return errors.New("negative depth: malformd tree or bug")
	}

	var nr Refs // new Refs

	nr.depth = depth
	nr.degree = r.degree
	nr.rn = &refsNode{
		pack: r.rn.pack,
		sch:  r.rn.sch,
	}

	if r.rn.pack.flags&HashTableIndex != 0 {
		nr.rn.index = make(map[cipher.SHA256]*RefsElem)
	}

	// 1) create branches (keep 'Hash' and 'length' fields blank)
	// 2) walk throut the tree and set proper 'Hash' and 'length' fields

	_, err = r.ascend(r.depth, 0, func(_ int, ref *RefsElem) (err error) {
		var ok bool
		if ok, err = nr.tryInsert(nr.degree, nr.depth, ref); err != nil {
			return
		}
		if !ok {
			panic("can't insert to freshly created tree: bug")
		}
		return
	})

	if err != nil {
		return
	}

	// let's walk through entire nr to set proper 'length' and 'Hash' fields,
	// saving new elements (encoded nodes and root-Refs)
	if _, err = nr.updateHashLengthFieldsFromTail(nr.depth); err != nil {
		return
	}

	*r = nr     // replace the r with nr
	r.ch = true // has been changed

	// set proper upper
	if r.depth == 0 {
		for _, leaf := range r.leafs {
			leaf.upper = r
		}
		return
	}
	for _, br := range r.branches {
		br.upper = r
	}

	return
}

// Schema of the Refs. The method can returns nil if
// the Refs are not attached
func (r *Refs) Schema() (sch Schema) {
	if r.rn == nil {
		return
	}
	return r.rn.sch
}

// Append obejcts to the Refs. Type of the objects must
// be the same as type of the Refs. Nils will be skipped,
// even if it's nil of some type or nil-interface{}
func (r *Refs) Append(objs ...interface{}) (err error) {

	if len(objs) == 0 {
		return // nothing to append
	}

	if err = r.load(0); err != nil {
		return
	}

	if r.rn.pack.flags&ViewOnly != 0 {
		return ErrViewOnlyTree
	}

	// rid out of nils

	// os - Schema of an object
	// ns - new schema of the Refs if it's nil for now

	var os, ns Schema

	var i int
	for _, obj := range objs {
		if obj == nil {
			continue // delete from the objs
		}

		if os, obj, err = r.rn.pack.initialize(obj); err != nil {
			return
		}

		// check out schemas

		if r.rn.sch != nil {
			if r.rn.sch != os {
				err = fmt.Errorf(
					"can't Append object of another type: want %q, got %q",
					r.rn.sch.String(), os.String())
				return
			}
		} else if ns != nil && ns != os {
			err = fmt.Errorf("can't append obejct of differetn types: %q, %q",
				ns.String(), os.String())
			return
		} else {
			// r.wn.sch == nil
			// ns == nil
			// so, let's initialize the ns
			ns = os
		}

		if obj == nil {
			continue // delete from the objs
		}

		objs[i] = obj
		i++
	}
	objs = objs[:i] // delete all deleted

	if len(objs) == 0 {
		return // only nils
	}

	if r.rn.sch == nil {
		r.rn.sch = ns // this will be schema of the Refs
	}

	// do we need to rebuild the tree to place new elements
	var fsot int
	if fsot, err = r.freeSpaceOnTail(r.degree, r.depth); err != nil {
		return
	}

	if fsot < len(objs) {
		// so, we need to rebuild the tree removing zero elements or
		// increasing depth (and removing the zero elements, actually)

		// new depth, required space
		var depth, required int = r.depth, r.length + len(objs)
		for ; pow(r.degree, depth) < required; depth++ {
		}

		// rebuild the tree, depth can be the same
		if err = r.changeDepth(depth); err != nil {
			return
		}

	}

	// append
	var ok bool
	for _, obj := range objs {
		ref := new(RefsElem)

		ref.rn = r.rn                     //
		ref.value = obj                   // keep initialized value
		ref.Hash, _ = r.rn.pack.save(obj) // set Hash field, saving object
		ref.ch = true                     // fresh non-blank

		if ok, err = r.tryInsert(r.degree, r.depth, ref); err != nil {
			return
		}
		if !ok {
			panic("can't insert inside Append: bug")
		}
	}

	// walk from tail to update Hash and length fields of the
	// tree after appending, saving new and changed elements
	_, err = r.updateHashLengthFieldsFromTail(r.depth)
	return
}

// AppendHashes to the Refs. Type of the objects must
// be the same as type of the Refs. It never checked
// Blank hashes will be skipped
func (r *Refs) AppendHashes(hashes ...cipher.SHA256) (err error) {

	if len(hashes) == 0 {
		return // nothing to append
	}

	if err = r.load(0); err != nil {
		return
	}

	if r.rn.pack.flags&ViewOnly != 0 {
		return ErrViewOnlyTree
	}

	// rid out of blank hashes
	var i int
	for _, hash := range hashes {
		if hash == (cipher.SHA256{}) {
			continue // delte from the hashes
		}
		hashes[i] = hash
		i++
	}
	hashes = hashes[:i] // delete all deleted

	if len(hashes) == 0 {
		return // only blank
	}

	// do we need to rebuild the tree to place new elements
	var fsot int
	if fsot, err = r.freeSpaceOnTail(r.degree, r.depth); err != nil {
		return
	}

	if fsot < len(hashes) {
		// so, we need to rebuild the tree removing zero elements or
		// increasing depth (and removing the zero elements, actually)

		// new depth, required space
		var depth, required int = r.depth, r.length + len(hashes)
		for ; pow(r.degree, depth) < required; depth++ {
		}

		// rebuild the tree, depth can be the same
		if err = r.changeDepth(depth); err != nil {
			return
		}

	}

	// append
	var ok bool
	for _, hash := range hashes {
		el := new(RefsElem)

		el.rn = r.rn   //
		el.Hash = hash //

		if ok, err = r.tryInsert(r.degree, r.depth, el); err != nil {
			return
		}
		if !ok {
			panic("can't insert inside Append: bug")
		}
	}

	// walk from tail to update Hash and length fields of the
	// tree after appending, saving new and changed elements
	_, err = r.updateHashLengthFieldsFromTail(r.depth)
	return
}

// TODO (kostyarin): implement Slice(i, j int) (cut *Refs, err error)

// Clear the Refs making them blank
func (r *Refs) Clear() {
	if r.isLoaded() {
		// detach nested form the Refs for golang GC,
		// and to avoid potential memory leaks, and,
		// mainly, to protect the Refs from bubbling
		// changes
		if r.depth == 0 {
			for _, leaf := range r.leafs {
				leaf.upper = nil // detach from Refs
				leaf.rn = nil
			}
		} else {
			for _, br := range r.branches {
				br.upper = nil
				br.rn = nil
			}
		}
	}
	r.Hash = cipher.SHA256{}
	r.ch = true
	r.length = 0
	r.depth = 0
	if r.rn.pack.flags&HashTableIndex != 0 {
		r.rn.index = make(map[cipher.SHA256]*RefsElem) // clear
	}
	return
}

// utils

// bubble changes up
func (r *Refs) bubble() (err error) {
	stack := []*Refs{}

	// push unsaved nodes to the stack
	var up *Refs
	for up = r; up != nil; up = up.upper {
		stack = append(stack, up)
	}

	root := stack[len(stack)-1]
	if root.depth != len(stack)-1 {
		// it's detached branch of Refs
		return
	}

	// index is depth
	for i, br := range stack {
		br.save(i)
	}

	// the bubble can be triggered by deleteing
	// and length o the tree can be changed,
	// thus we need to rebuild the tree if it
	// contains a lot of zero-elements

	// up keeps root Refs
	err = root.reduceIfNeed()
	return
}

// max possible non-zero items, the Refs must be root and loaded
// the depth is depth-1 (because it can be 0) and we're using
// depth+1 to calculate the items
func (r *Refs) items() int {
	return pow(r.degree, r.depth+1)
}

// encoded representation of Refs
type encodedRefs struct {
	Depth  uint32          // depth of the Refs (depth-1)
	Degree uint32          // deger of the Refs
	Length uint32          // total elements
	Nested []cipher.SHA256 // hashes of nested objects (leafs or ndoes)
}

type encodedRefsNode struct {
	Length uint32 // total elements
	Nested []cipher.SHA256
}

// a**b
func pow(a, b int) (p int) {
	p = 1
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------

// print debug tree

// DebugString returns string that represents the Refs as
// is. E.g. as Merkle-tree. If the tree is not loaded then
// this mehtod prints only loaded parts. To load all branches
// to print pass true
func (r *Refs) DebugString(forceLoad bool) string {
	var gt gotree.GTStructure
	gt.Name = "[](refs) " + r.Short() + " " + fmt.Sprint(r.length)

	if !forceLoad && !r.isLoaded() {
		gt.Name += " (not loaded)"
		return gotree.StringTree(gt)
	}

	if err := r.load(0); err != nil {
		gt.Items = []gotree.GTStructure{{Name: "(error): " + err.Error()}}
		return gotree.StringTree(gt)
	}

	gt.Items = r.debugItems(forceLoad, r.depth)
	return gotree.StringTree(gt)
}

func (r *Refs) debugItems(forceLoad bool,
	depth int) (its []gotree.GTStructure) {

	if forceLoad {
		if err := r.load(depth); err != nil {
			return []gotree.GTStructure{{Name: "(error): " + err.Error()}}
		}
	}

	if depth == 0 {
		for _, leaf := range r.leafs {
			its = append(its, gotree.GTStructure{
				Name: "*(ref) " + leaf.Short(),
			})
		}
		return
	}

	for _, br := range r.branches {
		its = append(its, gotree.GTStructure{
			Name:  br.Hash.Hex()[:7] + " " + fmt.Sprint(br.length),
			Items: br.debugItems(forceLoad, depth-1),
		})
	}
	return

}

// ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------

// RefsElem

// RefsElem is element of Refs
type RefsElem struct {
	Hash cipher.SHA256 // hash of the RefsElem

	// internals
	value interface{} // golang-value of the RefsElem
	rn    *refsNode   // related
	upper *Refs       // upper node
	ch    bool        // has been changed
}

// Short string
func (r *RefsElem) Short() string {
	return r.Hash.Hex()[:7]
}

// String implements fmt.Stringer interface
func (r *RefsElem) String() string {
	return r.Hash.Hex()
}

// SetHash replaces hash of the RefsElem with given one
func (r *RefsElem) SetHash(hash cipher.SHA256) (err error) {
	if r.Hash == hash {
		return
	}
	if hash == (cipher.SHA256{}) {
		return r.Delete()
	}

	if r.rn != nil && r.rn.pack.flags&HashTableIndex != 0 {
		delete(r.rn.index, r.Hash)
		r.rn.index[hash] = r
	}

	r.value = nil // clear related value
	r.Hash = hash
	r.ch = true
	return
}

// Delete the RefsElem from related Refs
func (r *RefsElem) Delete() (err error) {
	if r.Hash == (cipher.SHA256{}) {
		return
	}

	if r.rn != nil && r.rn.pack.flags&HashTableIndex != 0 {
		delete(r.rn.index, r.Hash)
	}

	r.Hash = (cipher.SHA256{})

	if r.upper != nil {
		err = r.upper.bubble()
	}

	r.value = nil
	r.rn = nil // detach
	r.upper = nil
	r.ch = true // changed
	return
}

// Value of the RefsElem. If err is nil, then obj is not
func (r *RefsElem) Value() (obj interface{}, err error) {

	// copy pasted from Ref.Value with little changes
	//
	// TODO (kostyarin): DRY with the Ref.Value

	if r.rn == nil {
		err = errors.New(
			"can't get value: the RefsElem is not attached to Pack")
		return
	}

	if r.value != nil {
		obj = r.value // already have
		return
	}

	if r.rn.sch == nil {
		err = errors.New("can't get value: Schema of related Refs is nil")
		return
	}

	// obtain encoded object
	var val []byte
	if val, err = r.rn.pack.get(r.Hash); err != nil {
		return
	}

	// unpack and setup
	if obj, err = r.rn.pack.unpackToGo(r.rn.sch.Name(), val); err != nil {
		return
	}
	r.value = obj // keep

	return
}

// SetValue of the element. A nil removes this
// element from related Refs
func (r *RefsElem) SetValue(obj interface{}) (err error) {

	// copy-pasted from Ref.SetValue with little changes
	//
	// TODO (kostyarin): DRY with Ref.SetValue

	if obj == nil {
		return r.Delete()
	}

	if r.rn == nil {
		return errors.New(
			"can't set value: the RefsElem is not attached to Pack")
	}

	if r.upper == nil {
		return errors.New(
			"can't set value: the RefsElem is not attached to Refs")
	}

	if r.rn.pack.flags&ViewOnly != 0 {
		return ErrViewOnlyTree
	}

	var sch Schema
	if sch, obj, err = r.rn.pack.initialize(obj); err != nil {
		return fmt.Errorf("can't set value to RefsElem: %v", err)
	}

	if r.rn.sch != nil && r.rn.sch != sch {
		return fmt.Errorf(`can't set value: type of given object "%T"`+
			" is not type of the Ref %q", obj, r.rn.sch.String())
	}

	if obj == nil {
		return r.Delete() // the obj was a nil pointer of some type
	}

	r.rn.sch = sch // keep schema (if it was nil or the same)
	r.value = obj  // keep

	if key, val := r.rn.pack.dsave(obj); key != r.Hash {

		if r.rn.pack.flags&HashTableIndex != 0 {
			if r.Hash != (cipher.SHA256{}) {
				delete(r.rn.index, r.Hash)
			}
			r.rn.index[key] = r
		}

		r.Hash = key
		r.ch = true
		r.rn.pack.set(key, val) // save

		if err = r.upper.bubble(); err != nil {
			return
		}
	}

	return
}

// Save updates hash using underlying value.
// The vaue can be changd from outside, this
// method encodes it and updates hash
func (r *RefsElem) Save() (err error) {
	if r.rn == nil || r.value == nil || r.upper == nil {
		return // detached or not loaded
	}

	key, val := r.rn.pack.dsave(r.value) // don't save - get key-value pair
	if key == r.Hash {
		return // exactly the same
	}

	if r.rn.pack.flags&HashTableIndex != 0 {
		if r.Hash != (cipher.SHA256{}) {
			delete(r.rn.index, r.Hash)
		}
		r.rn.index[key] = r
	}

	r.rn.pack.set(key, val) // save
	r.Hash, r.ch = key, false

	err = r.upper.bubble() // bubble up
	return
}


*/