package memory

import (
	"testing"

	"github.com/skycoin/cxo/data"
)

func Test_tree(t *testing.T) {

	var (
		x   = newTree()
		err error
	)

	for i, j := uint64(0), uint64(1001); i < 1001; i, j = i+1, j-1 {
		if i%2 == 0 {
			x.set(i, nil)
		}
		if j%2 != 0 {
			x.set(j, nil)
		}
	}

	var i uint64
	err = x.ascend(0, func(seq uint64, _ *data.Root) (_ error) {
		if seq != i {
			t.Error("wrong seq:", seq, i)
		}
		i++
		return
	})
	if err != nil {
		t.Error(err)
	}

	i = 1001
	err = x.descend(1001, func(seq uint64, _ *data.Root) (_ error) {
		if seq != i {
			t.Error("wrong seq:", seq, i)
		}
		i--
		return
	})
	if err != nil {
		t.Error(err)
	}

	for i, j := uint64(0), uint64(1001); i < 1001; i, j = i+1, j-1 {
		if i%2 == 0 {
			x.del(i)
		}
		if j%2 != 0 {
			x.del(j)
		}
	}

}
