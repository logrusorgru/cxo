package data

import (
	"reflect"
	"sync"
	"time"

	"github.com/skycoin/skycoin/src/cipher"
)

// A GetBeforeHookResultFunc used with Hooks to get result of hook call
type GetBeforeHookResultFunc func(meta interface{}, err error)

// A Hooks implements hoosk keepper
type Hooks struct {

	// before

	beforeTouchMutex sync.Mutex
	beforeTouchHooks []BeforeTouchHookFunc

	beforeSetMutex sync.Mutex
	beforeSetHooks []BeforeSetHookFunc

	beforeGetMutex sync.Mutex
	beforeGetHooks []BeforeGetHookFunc

	beforeIncrMutex sync.Mutex
	beforeIncrHooks []BeforeIncrHookFunc

	beforeDelMutex sync.Mutex
	beforeDelHooks []BeforeDelHookFunc

	// after

	afterTouchMutex sync.Mutex
	afterTouchHooks []AfterTouchHookFunc

	afterSetMutex sync.Mutex
	afterSetHooks []AfterSetHookFunc

	afterGetMutex sync.Mutex
	afterGetHooks []AfterGetHookFunc

	afterIncrMutex sync.Mutex
	afterIncrHooks []AfterIncrHookFunc

	afterDelMutex sync.Mutex
	afterDelHooks []AfterDelHookFunc
}

//
// Before
//

// Touch

// AddBeforeTouchHook to tail of before-touch-hooks
func (h *Hooks) AddBeforeTouchHook(beforeTouchHookFunc BeforeTouchHookFunc) {
	h.beforeTouchMutex.Lock()
	defer h.beforeTouchMutex.Unlock()

	h.beforeTouchHooks = append(h.beforeTouchHooks, beforeTouchHookFunc)
}

// DelBeforeTouchHook all matches
func (h *Hooks) DelBeforeTouchHook(beforeTouchHookFunc BeforeTouchHookFunc) {
	h.beforeTouchMutex.Lock()
	defer h.beforeTouchMutex.Unlock()

	var (
		i    int
		hook BeforeTouchHookFunc
	)

	for _, hook = range h.beforeTouchHooks {
		h.beforeTouchHooks[i] = nil // clear first
		if isFuncsEqual(hook, beforeTouchHookFunc) == false {
			h.beforeTouchHooks[i] = hook
			i++
		}
	}

	h.beforeTouchHooks = h.beforeTouchHooks[:i]
}

// CallBeforeTouchHook used to call before-touch-hooks
func (h *Hooks) CallBeforeTouchHook(
	key cipher.SHA256,
	getBeforeHookResultFunc GetBeforeHookResultFunc,
) {
	h.beforeTouchMutex.Lock()
	defer h.beforeTouchMutex.Unlock()

	var (
		meta interface{}
		err  error
	)

	for _, hook := range h.beforeTouchHooks {
		meta, err = hook(key)
		getBeforeHookResultFunc(meta, err)
		if err != nil {
			return
		}
	}
}

// Set

// AddBeforeSetHook to tail of before-set-hooks
func (h *Hooks) AddBeforeSetHook(beforeSetHookFunc BeforeSetHookFunc) {
	h.beforeSetMutex.Lock()
	defer h.beforeSetMutex.Unlock()

	h.beforeSetHooks = append(h.beforeSetHooks, beforeSetHookFunc)
}

// DelBeforeSetHook all matches
func (h *Hooks) DelBeforeSetHook(beforeSetHookFunc BeforeSetHookFunc) {
	h.beforeSetMutex.Lock()
	defer h.beforeSetMutex.Unlock()

	var (
		i    int
		hook BeforeSetHookFunc
	)

	for _, hook = range h.beforeSetHooks {
		h.beforeSetHooks[i] = nil // clear first
		if isFuncsEqual(hook, beforeSetHookFunc) == false {
			h.beforeSetHooks[i] = hook
			i++
		}
	}

	h.beforeSetHooks = h.beforeSetHooks[:i]
}

// CallBeforeSetHook used to call before-set-hooks
func (h *Hooks) CallBeforeSetHook(
	key cipher.SHA256,
	val []byte,
	incrBy int64,
	getBeforeHookResultFunc GetBeforeHookResultFunc,
) {
	h.beforeSetMutex.Lock()
	defer h.beforeSetMutex.Unlock()

	var (
		meta interface{}
		err  error
	)

	for _, hook := range h.beforeSetHooks {
		meta, err = hook(key, val, incrBy)
		getBeforeHookResultFunc(meta, err)
		if err != nil {
			return
		}
	}
}

// Get

// AddBeforeGetHook to tail of before-get-hooks
func (h *Hooks) AddBeforeGetHook(beforeGetHookFunc BeforeGetHookFunc) {
	h.beforeGetMutex.Lock()
	defer h.beforeGetMutex.Unlock()

	h.beforeGetHooks = append(h.beforeGetHooks, beforeGetHookFunc)
}

// DelBeforeGetHook all matches
func (h *Hooks) DelBeforeGetHook(beforeGetHookFunc BeforeGetHookFunc) {
	h.beforeGetMutex.Lock()
	defer h.beforeGetMutex.Unlock()

	var (
		i    int
		hook BeforeGetHookFunc
	)

	for _, hook = range h.beforeGetHooks {
		h.beforeGetHooks[i] = nil // clear first
		if isFuncsEqual(hook, beforeGetHookFunc) == false {
			h.beforeGetHooks[i] = hook
			i++
		}
	}

	h.beforeGetHooks = h.beforeGetHooks[:i]
}

// CallBeforeGetHook used to call before-get-hooks
func (h *Hooks) CallBeforeGetHook(
	key cipher.SHA256,
	incrBy int64,
	getBeforeHookResultFunc GetBeforeHookResultFunc,
) {
	h.beforeGetMutex.Lock()
	defer h.beforeGetMutex.Unlock()

	var (
		meta interface{}
		err  error
	)

	for _, hook := range h.beforeGetHooks {
		meta, err = hook(key, incrBy)
		getBeforeHookResultFunc(meta, err)
		if err != nil {
			return
		}
	}
}

// Incr

// AddBeforeIncrHook to tail of before-incr-hooks
func (h *Hooks) AddBeforeIncrHook(beforeIncrHookFunc BeforeIncrHookFunc) {
	h.beforeIncrMutex.Lock()
	defer h.beforeIncrMutex.Unlock()

	h.beforeIncrHooks = append(h.beforeIncrHooks, beforeIncrHookFunc)
}

// DelBeforeIncrHook all matches
func (h *Hooks) DelBeforeIncrHook(beforeIncrHookFunc BeforeIncrHookFunc) {
	h.beforeIncrMutex.Lock()
	defer h.beforeIncrMutex.Unlock()

	var (
		i    int
		hook BeforeIncrHookFunc
	)

	for _, hook = range h.beforeIncrHooks {
		h.beforeIncrHooks[i] = nil // clear first
		if isFuncsEqual(hook, beforeIncrHookFunc) == false {
			h.beforeIncrHooks[i] = hook
			i++
		}
	}

	h.beforeIncrHooks = h.beforeIncrHooks[:i]
}

// CallBeforeIncrHook used to call before-incr-hooks
func (h *Hooks) CallBeforeIncrHook(
	key cipher.SHA256,
	incrBy int64,
	getBeforeHookResultFunc GetBeforeHookResultFunc,
) {
	h.beforeIncrMutex.Lock()
	defer h.beforeIncrMutex.Unlock()

	var (
		meta interface{}
		err  error
	)

	for _, hook := range h.beforeIncrHooks {
		meta, err = hook(key, incrBy)
		getBeforeHookResultFunc(meta, err)
		if err != nil {
			return
		}
	}
}

// Del

// AddBeforeDelHook to tail of before-del-hooks
func (h *Hooks) AddBeforeDelHook(beforeDelHookFunc BeforeDelHookFunc) {
	h.beforeDelMutex.Lock()
	defer h.beforeDelMutex.Unlock()

	h.beforeDelHooks = append(h.beforeDelHooks, beforeDelHookFunc)
}

// DelBeforeDelHook all matches
func (h *Hooks) DelBeforeDelHook(beforeDelHookFunc BeforeDelHookFunc) {
	h.beforeDelMutex.Lock()
	defer h.beforeDelMutex.Unlock()

	var (
		i    int
		hook BeforeDelHookFunc
	)

	for _, hook = range h.beforeDelHooks {
		h.beforeDelHooks[i] = nil // clear first
		if isFuncsEqual(hook, beforeDelHookFunc) == false {
			h.beforeDelHooks[i] = hook
			i++
		}
	}

	h.beforeDelHooks = h.beforeDelHooks[:i]
}

// CallBeforeDelHook used to call before-del-hooks
func (h *Hooks) CallBeforeDelHook(
	key cipher.SHA256,
	getBeforeHookResultFunc GetBeforeHookResultFunc,
) {
	h.beforeDelMutex.Lock()
	defer h.beforeDelMutex.Unlock()

	var (
		meta interface{}
		err  error
	)

	for _, hook := range h.beforeDelHooks {
		meta, err = hook(key)
		getBeforeHookResultFunc(meta, err)
		if err != nil {
			return
		}
	}
}

//
// After
//

// Touch

// AddAfterTouchHook to tail of after-touch-hooks
func (h *Hooks) AddAfterTouchHook(afterTouchHookFunc AfterTouchHookFunc) {
	h.afterTouchMutex.Lock()
	defer h.afterTouchMutex.Unlock()

	h.afterTouchHooks = append(h.afterTouchHooks, afterTouchHookFunc)
}

// DelAfterTouchHook all matches
func (h *Hooks) DelAfterTouchHook(afterTouchHookFunc AfterTouchHookFunc) {
	h.afterTouchMutex.Lock()
	defer h.afterTouchMutex.Unlock()

	var (
		i    int
		hook AfterTouchHookFunc
	)

	for _, hook = range h.afterTouchHooks {
		h.afterTouchHooks[i] = nil // clear first
		if isFuncsEqual(hook, afterTouchHookFunc) == false {
			h.afterTouchHooks[i] = hook
			i++
		}
	}

	h.afterTouchHooks = h.afterTouchHooks[:i]
}

// CallAfterTouchHook used to call after-touch-hooks
func (h *Hooks) CallAfterTouchHook(
	key cipher.SHA256,
	access time.Time,
	err error,
) {
	h.afterTouchMutex.Lock()
	defer h.afterTouchMutex.Unlock()

	for _, hook := range h.afterTouchHooks {
		hook(key, access, err)
	}
}

// Set

// AddAfterSetHook to tail of after-set-hooks
func (h *Hooks) AddAfterSetHook(afterSetHookFunc AfterSetHookFunc) {
	h.afterSetMutex.Lock()
	defer h.afterSetMutex.Unlock()

	h.afterSetHooks = append(h.afterSetHooks, afterSetHookFunc)
}

// DelAfterSetHook all matches
func (h *Hooks) DelAfterSetHook(afterSetHookFunc AfterSetHookFunc) {
	h.afterSetMutex.Lock()
	defer h.afterSetMutex.Unlock()

	var (
		i    int
		hook AfterSetHookFunc
	)

	for _, hook = range h.afterSetHooks {
		h.afterSetHooks[i] = nil // clear first
		if isFuncsEqual(hook, afterSetHookFunc) == false {
			h.afterSetHooks[i] = hook
			i++
		}
	}

	h.afterSetHooks = h.afterSetHooks[:i]
}

// CallAfterSetHook used to call after-set-hooks
func (h *Hooks) CallAfterSetHook(
	key cipher.SHA256,
	obj *Object,
	err error,
) {
	h.afterSetMutex.Lock()
	defer h.afterSetMutex.Unlock()

	for _, hook := range h.afterSetHooks {
		hook(key, obj, err)
	}
}

// Get

// AddAfterGetHook to tail of after-get-hooks
func (h *Hooks) AddAfterGetHook(afterGetHookFunc AfterGetHookFunc) {
	h.afterGetMutex.Lock()
	defer h.afterGetMutex.Unlock()

	h.afterGetHooks = append(h.afterGetHooks, afterGetHookFunc)
}

// DelAfterGetHook all matches
func (h *Hooks) DelAfterGetHook(afterGetHookFunc AfterGetHookFunc) {
	h.afterGetMutex.Lock()
	defer h.afterGetMutex.Unlock()

	var (
		i    int
		hook AfterGetHookFunc
	)

	for _, hook = range h.afterGetHooks {
		h.afterGetHooks[i] = nil // clear first
		if isFuncsEqual(hook, afterGetHookFunc) == false {
			h.afterGetHooks[i] = hook
			i++
		}
	}

	h.afterGetHooks = h.afterGetHooks[:i]
}

// CallAfterGetHook used to call after-get-hooks
func (h *Hooks) CallAfterGetHook(
	key cipher.SHA256,
	obj *Object,
	err error,
) {
	h.afterGetMutex.Lock()
	defer h.afterGetMutex.Unlock()

	for _, hook := range h.afterGetHooks {
		hook(key, obj, err)
	}
}

// Incr

// AddAfterIncrHook to tail of after-incr-hooks
func (h *Hooks) AddAfterIncrHook(afterIncrHookFunc AfterIncrHookFunc) {
	h.afterIncrMutex.Lock()
	defer h.afterIncrMutex.Unlock()

	h.afterIncrHooks = append(h.afterIncrHooks, afterIncrHookFunc)
}

// DelAfterIncrHook all matches
func (h *Hooks) DelAfterIncrHook(afterIncrHookFunc AfterIncrHookFunc) {
	h.afterIncrMutex.Lock()
	defer h.afterIncrMutex.Unlock()

	var (
		i    int
		hook AfterIncrHookFunc
	)

	for _, hook = range h.afterIncrHooks {
		h.afterIncrHooks[i] = nil // clear first
		if isFuncsEqual(hook, afterIncrHookFunc) == false {
			h.afterIncrHooks[i] = hook
			i++
		}
	}

	h.afterIncrHooks = h.afterIncrHooks[:i]
}

// CallAfterIncrHook used to call after-incr-hooks
func (h *Hooks) CallAfterIncrHook(
	key cipher.SHA256, // :
	rc int64, //          :
	access time.Time, //  :
	err error, //         :
) {
	h.afterIncrMutex.Lock()
	defer h.afterIncrMutex.Unlock()

	for _, hook := range h.afterIncrHooks {
		hook(key, rc, access, err)
	}
}

// Del

// AddAfterDelHook to tail of after-del-hooks
func (h *Hooks) AddAfterDelHook(afterDelHookFunc AfterDelHookFunc) {
	h.afterDelMutex.Lock()
	defer h.afterDelMutex.Unlock()

	h.afterDelHooks = append(h.afterDelHooks, afterDelHookFunc)
}

// DelAfterDelHook all matches
func (h *Hooks) DelAfterDelHook(afterDelHookFunc AfterDelHookFunc) {
	h.afterDelMutex.Lock()
	defer h.afterDelMutex.Unlock()

	var (
		i    int
		hook AfterDelHookFunc
	)

	for _, hook = range h.afterDelHooks {
		h.afterDelHooks[i] = nil // clear first
		if isFuncsEqual(hook, afterDelHookFunc) == false {
			h.afterDelHooks[i] = hook
			i++
		}
	}

	h.afterDelHooks = h.afterDelHooks[:i]
}

// CallAfterDelHook used to call after-del-hooks
func (h *Hooks) CallAfterDelHook(
	key cipher.SHA256, // :
	obj *Object, //       :
	err error, //         :
) {
	h.afterDelMutex.Lock()
	defer h.afterDelMutex.Unlock()

	for _, hook := range h.afterDelHooks {
		hook(key, obj, err)
	}
}

//
// ---
//

func isFuncsEqual(aFunc, bFunc interface{}) (eq bool) {
	var a, b = reflect.ValueOf(aFunc), reflect.ValueOf(bFunc)
	eq = a.Pointer() == b.Pointer()
	return
}
