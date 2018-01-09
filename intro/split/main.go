package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/skycoin/skycoin/src/cipher"

	"github.com/skycoin/cxo/skyobject"
	"github.com/skycoin/cxo/skyobject/registry"
)

const (
	whitePng string = "skycoin-white.png"
	blackPng string = "skycoin-black.png"

	suffix string = "-from-cxo"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

// A Piece is stub type since we can use only
// registered and named structures. The Merkle-tree
// (the registry.Refs) will be type of the Piece
// but in reality the Piece will never used.
type Piece struct {
	Data []byte
}

// A File represents a big file
type File struct {
	Name    string
	Content registry.Refs `skyobject:"schema=split.Piece"`
}

func main() {

	// remove files
	//
	//  - skycoin-white-from-cxo.png
	//  - skycoin-black-from-cxo.png
	//
	// if exist

	os.Remove(suffixedName(whitePng))
	os.Remove(suffixedName(blackPng))

	var reg = registry.NewRegistry(func(r *registry.Reg) {
		r.Register("split.Piece", Piece{})
		r.Register("split.File", File{})
	})

	var conf = skyobject.NewConfig()             // get default config
	conf.InMemoryDB = true                       // for this example
	conf.MaxObjectSize = skyobject.MinObjectSize // 1024 (for this example)

	var c, err = skyobject.NewContainer(conf)

	if err != nil {
		log.Fatal(err)
	}

	var pk, sk = cipher.GenerateKeyPair()

	if err = c.AddFeed(pk); err != nil {
		log.Fatal(err)
	}

	// load files from filesystem and store them in the CXO
	createRoot(c, reg, pk, sk)

	// laod files from the CXO and store them in filesystem
	// with different names
	lookupRoot(c, pk)

}

// create Root with files loaded from filesystem
func createRoot(c *skyobject.Container, reg *registry.Registry,
	pk cipher.PubKey, sk cipher.SecKey) {

	var up, err = c.Unpack(sk, reg)

	if err != nil {
		log.Fatal(err)
	}

	var r = new(registry.Root)

	r.Pub = pk
	r.Nonce = 90210
	r.Descriptor = []byte("split, version: 1")

	// load files from filesystem
	var white, black = loadFile(whitePng, c, up), loadFile(blackPng, c, up)

	// schema of the File
	var sch registry.Schema
	if sch, err = reg.SchemaByName("split.File"); err != nil {
		return
	}

	// dynamic references points to the Files
	var wdr, bdr registry.Dynamic

	wdr.Schema = sch.Reference()
	if err = wdr.SetValue(up, white); err != nil {
		log.Fatal(err)
	}

	bdr.Schema = sch.Reference()
	if err = bdr.SetValue(up, black); err != nil {
		log.Fatal(err)
	}

	// add the Dynamic references to Root
	r.Refs = append(r.Refs, wdr, bdr)

	// and save the Root
	if err = c.Save(up, r); err != nil {
		log.Fatal(err)
	}

	fmt.Println("files loaded")

}

func lookupRoot(c *skyobject.Container, pk cipher.PubKey) {

	var r, err = c.LastRoot(pk, c.ActiveHead(pk))

	if err != nil {
		log.Fatal(err)
	}

	if len(r.Refs) != 2 {
		log.Fatal("invalid length of Root.Refs")
	}

	var pack registry.Pack
	if pack, err = c.Pack(r, nil); err != nil {
		log.Fatal(err)
	}

	var white, black File

	// we know that the Refs[0] contains File
	if err = r.Refs[0].Value(pack, &white); err != nil {
		log.Fatal(err)
	}

	if err = r.Refs[1].Value(pack, &black); err != nil {
		log.Fatal(err)
	}

	saveFile(suffixedName(white.Name), c, pack, &white.Content)
	saveFile(suffixedName(black.Name), c, pack, &black.Content)

	fmt.Println("files stored")

}

// load file from filesystem and save it in CXO creating
// File instance that points to the saved data
func loadFile(name string, c *skyobject.Container,
	pack registry.Pack) (file *File) {

	fl, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer fl.Close()

	file = new(File)
	file.Name = fl.Name()

	if err = c.Split(pack, fl, &file.Content); err != nil {
		log.Fatal(err)
	}

	return
}

// white.png -> white-suffix.png
func suffixedName(name string) (sn string) {
	var ext = filepath.Ext(name)
	return strings.TrimSuffix(name, ext) + suffix + ext
}

// save file from CXO to given name in filesystem
func saveFile(name string, c *skyobject.Container, pack registry.Pack,
	refs *registry.Refs) {

	fl, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer fl.Close()

	if err = c.Concat(pack, refs, fl); err != nil {
		log.Fatal(err)
	}

}
