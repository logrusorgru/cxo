package main

//
// #include <stdint.h>
//
// typedef uintptr_t GoError; // error
//
// #include <registry.h>
//
import "C"

import (
	"sync"
	"unsafe"

	"github.com/skycoin/cxo/skyobject/registry"
)

type crefs struct {
	sync.Mutex
	m map[uintptr]interface{}
}

// "malloc"
func (c *crefs) addObject(ptr interface{}) C.uintptr_t {

	var void = uintptr(unsafe.Pointer(ptr))

	c.Lock()
	defer c.Unlock()

	c.m[void] = ptr

	return C.uintptr_t(void)
}

// "free"
func (c *crefs) delObject(void C.uintptr_t) {
	c.Lock()
	defer c.Unlock()

	delete(c.m, uintptr(void))
}

func (c *crefs) getObject(void C.uintptr_t) (obj interface{}) {
	c.Lock()
	defer c.Unlock()

	return c.m[uintptr(void)]
}

// objects allocated by C
var refs = crefs{
	m: make(map[uintptr]interface{}),
}

func main() {
	// We need the main function to make possible
	// CGO compiler to compile the package as C shared library
}
