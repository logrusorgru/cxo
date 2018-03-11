// Package cxds implements cxo/data.CXDS
// interface. E.g. the package provides data
// store for CXO. The data store is key value
// store, in which the key is SHA256 hash of
// value. And every value has references
// counter (rc). The counter set outside the
// CXDS. The rc is number references to an
// object. The CXDS never remove objects
// with zero-rc.
package cxds

import (
	"encoding/binary"
	"errors"

	"github.com/skycoin/skycoin/src/cipher"
	"github.com/skycoin/skycoin/src/cipher/encoder"
)

// comon errors
var (
	ErrEmptyValue       = errors.New("empty value")
	ErrWrongValueLength = errors.New("wrong value length")
)

//
func getRefsCount(val []byte) (rc uint32) {
	rc = binary.BigEndian.Uint32(val)
	return
}

//
func setRefsCount(val []byte, rc uint32) {
	binary.BigEndian.PutUint32(val, rc)
	return
}

func getHash(val []byte) (key cipher.SHA256) {
	return cipher.SumSHA256(val)
}

// meta info

type metaInfo struct {
	AmountAll  uint32 // amount of all objects
	AmountUsed uint32 // amount of used objects (rc > 0)

	VolumeAll  uint32 // volume of all objets
	VolumeUsed uint32 // volume of used objects (rc > 0)

	IsSafeClosed bool // safe closed flag
}

func (m *metaInfo) encode() []byte {
	return encoder.Serialize(m)
}

func (m *metaInfo) decode(v []byte) (err error) {
	return encoder.DeserializeRaw(v, m)
}

// increment slice
func incSlice(b []byte) {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == 0xff {
			b[i] = 0
			continue // increase next byte
		}
		b[i]++
		return
	}
}
