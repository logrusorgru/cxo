package rediscxds

import (
"sync"

	"github.com/skycoin/cxo/data"
)

type hooks struct {
	mx sync.Mutex
	// before
	beforeTouchHooks []data.BeforeTouchHookFunc
	beforeSetHooks   []data.BeforeSetHookFunc
	beforeGetHooks   []data.BeforeGetHookFunc
	beforeIncrHooks  []data.BeforeIncrHookFunc
	beforeDelHooks   []data.BeforeDelHookFunc
	// after
	afterTouchHooks []data.AfterTouchHookFunc
	afterSetHooks   []data.AfterSetHookFunc
	afterGetHooks   []data.AfterGetHookFunc
	afterIncrHooks  []data.AfterIncrHookFunc
	afterDelHooks   []data.AfterDelHookFunc
}

//
// Before
//

	// AddBeforeTouchHook to tail of before-touch-hooks
	AddBeforeTouchHook(beforeTouchHookFunc BeforeTouchHookFunc)
	// DelBeforeTouchHook all matches
	DelBeforeTouchHook(beforeTouchHookFunc BeforeTouchHookFunc)

func (h *hooks) AddBeforeSetHook(beforeSetHookFunc data.BeforeSetHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	h.beforeSetHooks = append(h.beforeSetHooks, h.beforeSetHookFunc)
}

func (h *hooks) DelBeforeSetHook(beforeSetHookFunc data.BeforeSetHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	var (
		i int
		hook data.BeforeSetHookFunc
	)

	for _, hook = range h.beforeSetHooks {
		h.beforeSetHooks[i] = nil // clear first
		if hook != beforeSetHookFunc {
			h.beforeSetHooks[i] = hook
			i++
		}
	}

	h.beforeSetHooks = h.beforeSetHooks[:i]
}

func (h *hooks) AddBeforeGetHook(beforeGetHookFunc data.BeforeGetHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	h.beforeGetHooks = append(h.beforeGetHooks, h.beforeGetHookFunc)
}

func (h *hooks) DelBeforeGetHook(beforeGetHookFunc data.BeforeGetHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	var (
		i int
		hook data.BeforeGetHookFunc
	)

	for _, hook = range h.beforeGetHooks {
		h.beforeGetHooks[i] = nil // clear first
		if hook != beforeGetHookFunc {
			h.beforeGetHooks[i] = hook
			i++
		}
	}

	h.beforeGetHooks = h.beforeGetHooks[:i]
}

func (h *hooks) AddBeforeIncrHook(beforeIncrHookFunc data.BeforeIncrHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	h.beforeIncrHooks = append(h.beforeIncrHooks, h.beforeIncrHookFunc)
}

func (h *hooks) DelBeforeIncrHook(beforeIncrHookFunc data.BeforeIncrHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	var (
		i int
		hook data.BeforeIncrHookFunc
	)

	for _, hook = range h.beforeIncrHooks {
		h.beforeIncrHooks[i] = nil // clear first
		if hook != beforeIncrHookFunc {
			h.beforeIncrHooks[i] = hook
			i++
		}
	}

	h.beforeIncrHooks = h.beforeIncrHooks[:i]
}

func (h *hooks) AddBeforeDelHook(beforeDelHookFunc data.BeforeDelHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	h.beforeDelHooks = append(h.beforeDelHooks, h.beforeDelHookFunc)
}

func (h *hooks) DelBeforeDelHook(beforeDelHookFunc data.BeforeDelHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	var (
		i int
		hook data.BeforeDelHookFunc
	)

	for _, hook = range h.beforeDelHooks {
		h.beforeDelHooks[i] = nil // clear first
		if hook != beforeDelHookFunc {
			h.beforeDelHooks[i] = hook
			i++
		}
	}

	h.beforeDelHooks = h.beforeDelHooks[:i]
}

func (h *hooks) AddAfterSetHook(afterSetHookFunc data.AfterSetHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	h.afterSetHooks = append(h.afterSetHooks, h.afterSetHookFunc)
}

func (h *hooks) DelAfterSetHook(afterSetHookFunc data.AfterSetHookFunc) {
	h.mx.Lock()
	defer h.mx.Unlock()

	var (
		i int
		hook data.AfterSetHookFunc
	)

	for _, hook = range h.beforeSetHooks {
		h.beforeSetHooks[i] = nil // clear first
		if hook != afterSetHookFunc {
			h.beforeSetHooks[i] = hook
			i++
		}
	}

	h.beforeSetHooks = h.beforeSetHooks[:i]
}

func (h *hooks) AddAfterGetHook(afterGetHookFunc data.AfterGetHookFunc) {
	h.afterGetHooks = append(h.afterGetHooks, h.afterGetHookFunc)
}

func (h *hooks) DelAfterGetHook(afterGetHookFunc data.AfterGetHookFunc) {
	var (
		i int
		hook data.AfterGetHookFunc
		)

	for _, hook := range h.afterGetHooks {
		if hook != afterGetHookFunc {
			h.data.AfterGetHooks[i] = hook
			i++
		}
	}

	for ; j := ii j len(h.afterGetHooks ); j++ {
		h.afterGetHooks [j] = nil
	}
	h.afterGetHooks = h.afterGetHooks[:i]
}

func (h *hooks) AddAfterIncrHook(afterIncrHookFunc data.AfterIncrHookFunc) {
	h.afterIncrHooks = append(h.afterIncrHooks, h.afterIncrHookFunc)
}

func (h *hooks) DelAfterIncrHook(afterIncrHookFunc data.AfterIncrHookFunc) {
	var (
		i int
		hook data.AfterIncrHookFunc
		)

	for _, hook := range h.afterIncrHooks {
		if hook != afterIncrHookFunc {
			h.data.AfterIncrHooks[i] = hook
			i++
		}
	}

	for j := i; j < len(h.afterIncrHooks); j++ {
		h.afterIncrHooks[j] = nil
	}
	h.afterIncrHooks = h.afterIncrHooks[:i]
}

func (h *hooks) AddAfterDelHook(afterDelHookFunc data.AfterDelHookFunc) {
	h.afterDelHooks = append(h.afterDelHooks, h.afterDelHookFunc)
}

func (h *hooks) DelAfterDelHook(afterDelHookFunc data.AfterDelHookFunc) {
	var (
		i int
		hook data.AfterDelHookFunc
		)

	for _, hook := range h.afterDelHooks {
		if hook != afterDelHookFunc {
			h.data.AfterDelHooks[i] = hook
			i++
		}
	}

	for ; j := ii j len(h.afterDelHooks ); j++ {
		h.afterDelHooks [j] = nil
	}
	h.afterDelHooks = h.afterDelHooks[:i]
}
