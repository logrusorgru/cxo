package memory

import (
	"errors"

	"github.com/skycoin/cxo/data"
)

const (
	red   = true
	black = false
)

type node struct {
	parent, left, right *node

	color bool
	seq   uint64
	value *data.Root
}

var sentinel = &node{nil, nil, nil, black, 0, nil}

func init() {
	sentinel.left, sentinel.right = sentinel, sentinel
}

type tree struct {
	root   *node
	length int
}

func (t *tree) rotateLeft(x *node) {
	y := x.right
	x.right = y.left
	if y.left != sentinel {
		y.left.parent = x
	}
	if y != sentinel {
		y.parent = x.parent
	}
	if x.parent != nil {
		if x == x.parent.left {
			x.parent.left = y
		} else {
			x.parent.right = y
		}
	} else {
		t.root = y
	}
	y.left = x
	if x != sentinel {
		x.parent = y
	}
}

func (t *tree) rotateRight(x *node) {
	y := x.left
	x.left = y.right
	if y.right != sentinel {
		y.right.parent = x
	}
	if y != sentinel {
		y.parent = x.parent
	}
	if x.parent != nil {
		if x == x.parent.right {
			x.parent.right = y
		} else {
			x.parent.left = y
		}
	} else {
		t.root = y
	}
	y.right = x
	if x != sentinel {
		x.parent = y
	}
}

func (t *tree) insertFixup(x *node) {
	for x != t.root && x.parent.color == red {
		if x.parent == x.parent.parent.left {
			y := x.parent.parent.right
			if y.color == red {
				x.parent.color = black
				y.color = black
				x.parent.parent.color = red
				x = x.parent.parent
			} else {
				if x == x.parent.right {
					x = x.parent
					t.rotateLeft(x)
				}
				x.parent.color = black
				x.parent.parent.color = red
				t.rotateRight(x.parent.parent)
			}
		} else {
			y := x.parent.parent.left
			if y.color == red {
				x.parent.color = black
				y.color = black
				x.parent.parent.color = red
				x = x.parent.parent
			} else {
				if x == x.parent.left {
					x = x.parent
					t.rotateRight(x)
				}
				x.parent.color = black
				x.parent.parent.color = red
				t.rotateLeft(x.parent.parent)
			}
		}
	}
	t.root.color = black
}

// silent rewrite if exist
func (t *tree) insertNode(seq uint64, value *data.Root) {
	current := t.root
	var parent *node
	for current != sentinel {
		if seq == current.seq {
			current.value = value
			return
		}
		parent = current
		if seq < current.seq {
			current = current.left
		} else {
			current = current.right
		}
	}
	x := &node{
		value:  value,
		parent: parent,
		left:   sentinel,
		right:  sentinel,
		color:  red,
		seq:    seq,
	}
	if parent != nil {
		if seq < parent.seq {
			parent.left = x
		} else {
			parent.right = x
		}
	} else {
		t.root = x
	}
	t.insertFixup(x)
	t.length++
}

func (t *tree) deleteFixup(x *node) {
	for x != t.root && x.color == black {
		if x == x.parent.left {
			w := x.parent.right
			if w.color == red {
				w.color = black
				x.parent.color = red
				t.rotateLeft(x.parent)
				w = x.parent.right
			}
			if w.left.color == black && w.right.color == black {
				w.color = red
				x = x.parent
			} else {
				if w.right.color == black {
					w.left.color = black
					w.color = red
					t.rotateRight(w)
					w = x.parent.right
				}
				w.color = x.parent.color
				x.parent.color = black
				w.right.color = black
				t.rotateLeft(x.parent)
				x = t.root
			}
		} else {
			w := x.parent.left
			if w.color == red {
				w.color = black
				x.parent.color = red
				t.rotateRight(x.parent)
				w = x.parent.left
			}
			if w.right.color == black && w.left.color == black {
				w.color = red
				x = x.parent
			} else {
				if w.left.color == black {
					w.right.color = black
					w.color = red
					t.rotateLeft(w)
					w = x.parent.left
				}
				w.color = x.parent.color
				x.parent.color = black
				w.left.color = black
				t.rotateRight(x.parent)
				x = t.root
			}
		}
	}
	x.color = black
}

func (t *tree) deleteNode(z *node) {
	var x, y *node
	if z == nil || z == sentinel {
		return
	}
	if z.left == sentinel || z.right == sentinel {
		y = z
	} else {
		y = z.right
		for y.left != sentinel {
			y = y.left
		}
	}
	if y.left != sentinel {
		x = y.left
	} else {
		x = y.right
	}
	x.parent = y.parent
	if y.parent != nil {
		if y == y.parent.left {
			y.parent.left = x
		} else {
			y.parent.right = x
		}
	} else {
		t.root = x
	}
	if y != z {
		z.seq = y.seq
		z.value = y.value
	}
	if y.color == black {
		t.deleteFixup(x)
	}
	t.length--
}

func (t *tree) findNode(seq uint64) (n *node) {
	for n = t.root; n != sentinel; {
		if seq == n.seq {
			return
		}
		if seq < n.seq {
			n = n.left
		} else {
			n = n.right
		}
	}
	return sentinel
}

func newTree() *tree {
	return &tree{
		root: sentinel,
	}
}

func (t *tree) set(seq uint64, value *data.Root) {
	t.insertNode(seq, value)
}

func (t *tree) del(seq uint64) {
	t.deleteNode(t.findNode(seq))
}

func (t *tree) get(seq uint64) *data.Root {
	return t.findNode(seq).value
}

func (t *tree) exist(seq uint64) bool {
	return t.findNode(seq) != sentinel
}

// floor returns the largest key node in the subtree
// rooted at x less than or equal to the given key
func (n *node) floor(seq uint64) *node {
	if n == sentinel {
		return sentinel
	}
	switch {
	case seq == n.seq:
		return n
	case seq < n.seq:
		return n.left.floor(seq)
	default:
	}
	var fn = n.right.floor(seq)
	if fn != sentinel {
		return fn
	}
	return n
}

// ceilig returns the smallest key node in the subtree
// rooted at x greater than or equal to the given key
func (n *node) ceiling(seq uint64) *node {
	if n == sentinel {
		return sentinel
	}
	switch {
	case seq == n.seq:
		return n
	case seq > n.seq:
		return n.right.ceiling(seq)
	default:
	}
	var cn = n.left.floor(seq)
	if cn != sentinel {
		return cn
	}
	return n
}

// floor returns the largest key in
// the tree less than or equal to key
func (t *tree) floor(seq uint64) *node {
	if t.root == sentinel {
		return sentinel
	}
	return t.root.floor(seq)
}

// ceiling returns the smallest key in
// the tree greater than or equal to key
func (t *tree) ceiling(seq uint64) *node {
	if t.root == sentinel {
		return sentinel
	}
	return t.root.ceiling(seq)
}

func (n *node) leftParent() (lp *node) {
	if n.parent == nil {
		return sentinel
	}
	if n.parent.left == n {
		return sentinel
	}
	return n.parent
}

func (n *node) rightParent() (rp *node) {
	if n.parent == nil {
		return sentinel
	}
	if n.parent.right == n {
		return sentinel
	}
	return n.parent
}

func (n *node) first() *node {
	for n.left != sentinel {
		n = n.left
	}
	return n
}

func (n *node) last() *node {
	for n.right != sentinel {
		n = n.right
	}
	return n
}

func (n *node) next() *node {
	if n.right != sentinel {
		return n.right.first()
	}
	var nxt *node
	for {
		if nxt = n.leftParent(); nxt == sentinel {
			if n.parent == nil {
				return sentinel
			}
			return n.parent
		}
		n = nxt
	}
}

func (n *node) prev() *node {
	if n.left != sentinel {
		return n.left.last()
	}
	var prv *node
	for {
		if prv = n.rightParent(); prv == sentinel {
			if n.parent == nil {
				return sentinel
			}
			return n.parent
		}
		n = prv
	}
}

type walker func(key uint64, value *data.Root) error

var errStop = errors.New("stop a walking")

// ascend >= the 'from' until the end or errStop
func (t *tree) ascend(from uint64, wl walker) (err error) {
	for n := t.ceiling(from); n != sentinel; n = n.next() {
		if err = wl(n.seq, n.value); err != nil {
			if err == errStop {
				err = nil
			}
			return
		}
	}
	return
}

// descend <= the 'from' until the begining or errStop
func (t *tree) descend(from uint64, wl walker) (err error) {
	for n := t.floor(from); n != sentinel; n = n.prev() {
		if err = wl(n.seq, n.value); err != nil {
			if err == errStop {
				err = nil
			}
			return
		}
	}
	return
}
