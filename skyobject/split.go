package skyobject

import (
	"io"

	"github.com/skycoin/skycoin/src/cipher"
	"github.com/skycoin/skycoin/src/cipher/encoder"

	"github.com/skycoin/cxo/skyobject/registry"
)

// Split data from given io.Reader to registry.Refs. Since, the CXO
// has MaxObjectSize, we can't store objects bigger then the limit.
// The Split splits a big object to registry.Refs (Merkle-tree) saving
// inpur in database. Given Refs will be cleaned up before splitting.
// Since, the CXO can save regitered types only, you have to use a dummy
// type for pieces of the data. For example:
//
//     // register types to use
//
//     // the ype required for CXO
//     type Piece struct {
//         Data []byte
//     }
//
//     // type that describes a file
//     type File struct {
//         Name sring
//         Content registry.Refs `skyobject:"schema=Piece"`
//     }
//
//     // register types
//     var reg = registry.NewRegistry(func(r *registry.Reg) {
//         r.Register("pkg.Piece", Piece{})
//         r.Register("pkg.File", File{})
//     })
//
//     var (
//         cnt *skyobject.Container
//         pack registry.Pack
//         root registry.Root
//     )
//
//     // --------------------------------------------------------
//     // create of get the Container, Pack (Unpack) and Root here
//     // --------------------------------------------------------
//
//     // open file
//     fl, err := os.Open(aBigBinaryFile)
//     if err != nil {
//         // hande error
//     }
//     defer fl.Close()
//
//    // file we are going to save
//     var file File
//
//     file.Name = fl.Name()
//
//     if err = cnt.Split(pack, fl, &file.Content); err != nil {
//         // handle error
//     }
//
//     // so, now the file saved in CXO DB and the Content field of the
//     // file variable points to it
//
//
func (c *Container) Split(
	pack registry.Pack,
	r io.Reader,
	refs *registry.Refs,
) (
	err error,
) {

	refs.Clear() // clear the Refs first

	var (
		buf = make([]byte, c.conf.MaxObjectSize-4) // encoded length
		n   int

		key cipher.SHA256
	)

	// if file is empty we are using blank Refs

	n, err = io.ReadFull(r, buf)

	for err == nil {

		// save the piece
		if key, err = pack.Add(encoder.Serialize(buf)); err != nil {
			return
		}

		// append the hash to the Refs
		if err = refs.AppendHashes(pack, key); err != nil {
			return
		}

		n, err = io.ReadFull(r, buf) // next piece

	}

	// the io.ReadFull returns io.EOF only if
	// the n is zero (no bytes read)

	if err == io.EOF {
		return // blank Refs or end
	}

	if err == io.ErrUnexpectedEOF {

		// if n is greater then zero, then
		// the piece is end of the Reader

		if n == 0 {
			return
		}

		// save the last piece
		if key, err = pack.Add(encoder.Serialize(buf[:n])); err != nil {
			return
		}

		// append the hash to the Refs
		err = refs.AppendHashes(pack, key)
	}

	return // an other error
}

// Concat is opposite to the Split method (see the Split for details).
// Given Refs must be type of a flat type that contains []byte only.
// Such as
//
//     type Piece struct {
//         Data []byte
//     }
//
// The Concat joins data splitted by the Split method and writes the data
// to given io.Writer. For example (see also the Split method)
//
//     var (
//         file File               // see dosc of the Split method
//         root registry.Root      //
//         c *skyobject.Container  //
//     )
//
//     // --------------------------------------
//     // get Container, root and the file here
//     // --------------------------------------
//
//     fl, err := os.Create(file.Name)
//     if err != nil {
//         // handle error
//     }
//     defer fl.Close()
//
//     // get regsitry.Pack
//
//     var pack registry.Pack
//     if pack, err = c.Pack(&root, nil); err != nil {
//         // handle error
//     }
//
//     if err = c.Concat(pack, &file.Refs, fl); err != nil {
//         // handle error
//     }
//
//     // now the fl contains data stored in CXO
//
func (c *Container) Concat(
	pack registry.Pack,
	refs *registry.Refs,
	w io.Writer,
) (
	err error,
) {

	var wrap struct {
		Data []byte
	}

	err = refs.Ascend(pack, func(_ int, key cipher.SHA256) (err error) {
		var val []byte
		if val, err = pack.Get(key); err != nil {
			return // pass through
		}
		if err = encoder.DeserializeRaw(val, &wrap); err != nil {
			return
		}
		_, err = w.Write(wrap.Data)
		return // pass the err through and stop the Ascend if the err is not nil
	})

	return
}
