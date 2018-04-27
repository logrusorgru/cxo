package data

import (
	"reflect"
	"sync"
	"time"

	"github.com/skycoin/skycoin/src/cipher"
)

// A HooksKeepper implements hoosk keepper
type HooksKeepper struct {

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
func (h *HooksKeepper) AddBeforeTouchHook(
	beforeTouchHookFunc BeforeTouchHookFunc,
) {

	h.beforeTouchMutex.Lock()
	defer h.beforeTouchMutex.Unlock()

	h.beforeTouchHooks = append(h.beforeTouchHooks, beforeTouchHookFunc)
}

// DelBeforeTouchHook all matches
func (h *HooksKeepper) DelBeforeTouchHook(
	beforeTouchHookFunc BeforeTouchHookFunc,
) {

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

// BeforeTouchHooks used to call before-touch-hooks,
// use the BeforeTouchHooks using following construction
//
//     defer hooks.BeforeTouchHooksClose()
//     for _, hook := range BeforeTouchHooks() {
//         hook(args)
//     }
//
func (h *HooksKeepper) BeforeTouchHooks() (hooks []BeforeTouchHookFunc) {
	h.beforeTouchMutex.Lock()
	return h.beforeTouchHooks
}

func (h *HooksKeepper) BeforeTouchHooksClose() {
	h.beforeTouchMutex.Unlock()
}

// Set

// AddBeforeSetHook to tail of before-set-hooks
func (h *HooksKeepper) AddBeforeSetHook(beforeSetHookFunc BeforeSetHookFunc) {
	h.beforeSetMutex.Lock()
	defer h.beforeSetMutex.Unlock()

	h.beforeSetHooks = append(h.beforeSetHooks, beforeSetHookFunc)
}

// DelBeforeSetHook all matches
func (h *HooksKeepper) DelBeforeSetHook(beforeSetHookFunc BeforeSetHookFunc) {
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

// BeforeSetHooks used to call before-set-hooks,
// use the BeforeSetHooks using following construction
//
//     defer hooks.BeforeSetHooksClose()
//     for _, hook := range BeforeSetHooks() {
//         hook(args)
//     }
//
func (h *HooksKeepper) BeforeSetHooks() (hooks []BeforeSetHookFunc) {
	h.beforeSetMutex.Lock()
	return h.beforeSetHooks
}

func (h *HooksKeepper) BeforeSetHooksClose() {
	h.beforeSetMutex.Unlock()
}

// Get

// AddBeforeGetHook to tail of before-get-hooks
func (h *HooksKeepper) AddBeforeGetHook(beforeGetHookFunc BeforeGetHookFunc) {
	h.beforeGetMutex.Lock()
	defer h.beforeGetMutex.Unlock()

	h.beforeGetHooks = append(h.beforeGetHooks, beforeGetHookFunc)
}

// DelBeforeGetHook all matches
func (h *HooksKeepper) DelBeforeGetHook(beforeGetHookFunc BeforeGetHookFunc) {
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

// BeforeGetHooks used to call before-get-hooks,
// use the BeforeGetHooks using following construction
//
//     defer hooks.BeforeGetHooksClose()
//     for _, hook := range BeforeGetHooks() {
//         hook(args)
//     }
//
func (h *HooksKeepper) BeforeGetHooks() (hooks []BeforeGetHookFunc) {
	h.beforeGetMutex.Lock()
	return h.beforeGetHooks
}

func (h *HooksKeepper) BeforeGetHooksClose() {
	h.beforeGetMutex.Unlock()
}

// Incr

// AddBeforeIncrHook to tail of before-incr-hooks
func (h *HooksKeepper) AddBeforeIncrHook(
	beforeIncrHookFunc BeforeIncrHookFunc,
) {

	h.beforeIncrMutex.Lock()
	defer h.beforeIncrMutex.Unlock()

	h.beforeIncrHooks = append(h.beforeIncrHooks, beforeIncrHookFunc)
}

// DelBeforeIncrHook all matches
func (h *HooksKeepper) DelBeforeIncrHook(
	beforeIncrHookFunc BeforeIncrHookFunc,
) {

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

// BeforeIncrHooks used to call before-incr-hooks,
// use the BeforeIncrHooks using following construction
//
//     defer hooks.BeforeIncrHooksClose()
//     for _, hook := range BeforeIncrHooks() {
//         hook(args)
//     }
//
func (h *HooksKeepper) BeforeIncrHooks() (hooks []BeforeIncrHookFunc) {
	h.beforeIncrMutex.Lock()
	return h.beforeIncrHooks
}

func (h *HooksKeepper) BeforeIncrHooksClose() {
	h.beforeIncrMutex.Unlock()
}

// Del

// AddBeforeDelHook to tail of before-del-hooks
func (h *HooksKeepper) AddBeforeDelHook(beforeDelHookFunc BeforeDelHookFunc) {
	h.beforeDelMutex.Lock()
	defer h.beforeDelMutex.Unlock()

	h.beforeDelHooks = append(h.beforeDelHooks, beforeDelHookFunc)
}

// DelBeforeDelHook all matches
func (h *HooksKeepper) DelBeforeDelHook(beforeDelHookFunc BeforeDelHookFunc) {
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

// BeforeDelHooks used to call before-del-hooks,
// use the BeforeDelHooks using following construction
//
//     defer hooks.BeforeDelHooksClose()
//     for _, hook := range BeforeDelHooks() {
//         hook(args)
//     }
//
func (h *HooksKeepper) BeforeDelHooks() (hooks []BeforeDelHookFunc) {
	h.beforeDelMutex.Lock()
	return h.beforeDelHooks
}

func (h *HooksKeepper) BeforeDelHooksClose() {
	h.beforeDelMutex.Unlock()
}

//
// After
//

// Touch

// AddAfterTouchHook to tail of after-touch-hooks
func (h *HooksKeepper) AddAfterTouchHook(
	afterTouchHookFunc AfterTouchHookFunc,
) {

	h.afterTouchMutex.Lock()
	defer h.afterTouchMutex.Unlock()

	h.afterTouchHooks = append(h.afterTouchHooks, afterTouchHookFunc)
}

// DelAfterTouchHook all matches
func (h *HooksKeepper) DelAfterTouchHook(
	afterTouchHookFunc AfterTouchHookFunc,
) {

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

// CallAfterTouchHooks used to call after-touch-hooks
func (h *HooksKeepper) CallAfterTouchHooks(
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
func (h *HooksKeepper) AddAfterSetHook(afterSetHookFunc AfterSetHookFunc) {
	h.afterSetMutex.Lock()
	defer h.afterSetMutex.Unlock()

	h.afterSetHooks = append(h.afterSetHooks, afterSetHookFunc)
}

// DelAfterSetHook all matches
func (h *HooksKeepper) DelAfterSetHook(afterSetHookFunc AfterSetHookFunc) {
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

// CallAfterSetHooks used to call after-set-hooks
func (h *HooksKeepper) CallAfterSetHooks(
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
func (h *HooksKeepper) AddAfterGetHook(afterGetHookFunc AfterGetHookFunc) {
	h.afterGetMutex.Lock()
	defer h.afterGetMutex.Unlock()

	h.afterGetHooks = append(h.afterGetHooks, afterGetHookFunc)
}

// DelAfterGetHook all matches
func (h *HooksKeepper) DelAfterGetHook(afterGetHookFunc AfterGetHookFunc) {
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

// CallAfterGetHooks used to call after-get-hooks
func (h *HooksKeepper) CallAfterGetHooks(
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
func (h *HooksKeepper) AddAfterIncrHook(afterIncrHookFunc AfterIncrHookFunc) {
	h.afterIncrMutex.Lock()
	defer h.afterIncrMutex.Unlock()

	h.afterIncrHooks = append(h.afterIncrHooks, afterIncrHookFunc)
}

// DelAfterIncrHook all matches
func (h *HooksKeepper) DelAfterIncrHook(afterIncrHookFunc AfterIncrHookFunc) {
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

// CallAfterIncrHooks used to call after-incr-hooks
func (h *HooksKeepper) CallAfterIncrHooks(
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
func (h *HooksKeepper) AddAfterDelHook(afterDelHookFunc AfterDelHookFunc) {
	h.afterDelMutex.Lock()
	defer h.afterDelMutex.Unlock()

	h.afterDelHooks = append(h.afterDelHooks, afterDelHookFunc)
}

// DelAfterDelHook all matches
func (h *HooksKeepper) DelAfterDelHook(afterDelHookFunc AfterDelHookFunc) {
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

// CallAfterDelHooks used to call after-del-hooks
func (h *HooksKeepper) CallAfterDelHooks(
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
