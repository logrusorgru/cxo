package data

import (
	"time"

	"github.com/skycoin/skycoin/src/cipher"
)

// Before.

// A BeforeTouchHookFunc called before the Touch method.
type BeforeTouchHookFunc func(
	key cipher.SHA256, // : } key to touch (argument)
) (
	meta interface{}, //  : infor for DB engine
	err error, //         : error to terminate call
)

// A BeforeGetHookFunc called before the Set* methods.
type BeforeSetHookFunc func(
	key cipher.SHA256, // : }
	val []byte, //        : } arguments
	incrBy int64, //      : }
) (
	meta interface{}, //  : info for DB engine
	err error, //         : error to terminate call
)

// A BeforeGetHookFunc called before the Get* methods.
type BeforeGetHookFunc func(
	key cipher.SHA256, // : } arguments
	incrBy int64, //      : }
) (
	meta interface{}, //  : info for DB engine
	err error, //         : error to terminate call
)

// A BeforeIncrHookFunc called before the Incr* methods.
type BeforeIncrHookFunc func(
	key cipher.SHA256, // : } arguments
	incrBy int64, //      : }
) (
	meta interface{}, //  : info for DB engine
	err error, //         : error to terminate call
)

// A BeforeDelHookFunc called before the Del and the Take methods.
type BeforeDelHookFunc func(
	key cipher.SHA256, // : } argument
) (
	meta interface{}, //  : info for DB engine
	err error, //         : error to terminate call
)

// After.

// An AfterTouchHookFunc called after Touch method call.
type AfterTouchHookFunc func(key cipher.SHA256, access time.Time, err error)

// An AfterSetHookFunc called after the Set* methods call.
type AfterSetHookFunc func(key cipher.SHA256, obj *Object, err error)

// An AfterGetHookFunc called after the Get* methods call.
type AfterGetHookFunc func(key cipher.SHA256, obj *Object, err error)

// An AfterIncrHookFunc called after the Incr* methods call.
type AfterIncrHookFunc func(
	key cipher.SHA256, // :
	rc int64, //          :
	access time.Time, //  :
	err error, //         :
)

// An AfterDelHookFunc called after the Del and  the Take
// methods call. If the hook called after the Del method,
// then the 'obj' argument of the hook is nil, even if
// the 'err' argument is nil. Because, the Del doesn't
// return Object.
type AfterDelHookFunc func(key cipher.SHA256, obj *Object, err error)

// Hooks.
type Hooks interface {
	// There are Before- and After- hooks.
	//
	// The Before hooks called before appropriate call
	// and can teminate DB call returning error. Also,
	// Before hooks can returns meta information for
	// DB engine.
	//
	// The After hooks called after DB call with reply
	// and error if any.
	//
	// Add- and Del- hooks methods works this way:
	// Add-methods adds provided hook to tail of chain
	// of hooks, Del-methods removes all matches.
	//
	//
	// Before hooks.
	//
	// AddBeforeTouchHook to tail of before-touch-hooks
	AddBeforeTouchHook(beforeTouchHookFunc BeforeTouchHookFunc)
	// DelBeforeTouchHook all matches
	DelBeforeTouchHook(beforeTouchHookFunc BeforeTouchHookFunc)
	//
	// AddBeforeSetHook to tail of before-set-hooks
	AddBeforeSetHook(beforeSetHookFunc BeforeSetHookFunc)
	// DelBeforeSetHook all matches
	DelBeforeSetHook(beforeSetHookFunc BeforeSetHookFunc)
	//
	// AddBeforeGetHook to tail of all before-get-hooks
	AddBeforeGetHook(beforeGetHookFunc BeforeGetHookFunc)
	// DelBeforeGetHook all matches
	DelBeforeGetHook(beforeGetHookFunc BeforeGetHookFunc)
	//
	// AddBeforeIncrHook to tail of before-incr-hooks
	AddBeforeIncrHook(beforeIncrHookFunc BeforeIncrHookFunc)
	// DelBeforeIncrHook all matches
	DelBeforeIncrHook(beforeIncrHookFunc BeforeIncrHookFunc)
	//
	// AddBeforeDelHook to tail of before-set-hooks
	AddBeforeDelHook(beforeDelHookFunc BeforeDelHookFunc)
	// DelBeforeDelHook all matches
	DelBeforeDelHook(beforeDelHookFunc BeforeDelHookFunc)
	//
	//
	// After hooks.
	//
	// AddAfterTouchHook to tail of after-touch-hooks
	AddAfterTouchHook(afterTouchHookFunc AfterTouchHookFunc)
	// DelAfterTouchHook all matches
	DelAfterTouchHook(afterTouchHookFunc AfterTouchHookFunc)
	//
	// AddAfterSetHook to tail of after-set-hooks
	AddAfterSetHook(afterSetHookFunc AfterSetHookFunc)
	// DelAfterSetHook all matches
	DelAfterSetHook(afterSetHookFunc AfterSetHookFunc)
	//
	// AddAfterGetHook to tail of after-get-hooks
	AddAfterGetHook(afterGetHookFunc AfterGetHookFunc)
	// DelAfterGetHook all matches
	DelAfterGetHook(afterGetHookFunc AfterGetHookFunc)
	//
	// AddAfterGetHook to tail of after-get-hooks
	AddAfterIncrHook(afterIncrHookFunc AfterIncrHookFunc)
	// DelAfterGetHook all matches
	DelAfterIncrHook(afterIncrHookFunc AfterIncrHookFunc)
	//
	// AddAfterGetHook to tail of after-get-hooks
	AddAfterDelHook(afterDelHookFunc AfterDelHookFunc)
	// DelAfterGetHook all matches
	DelAfterDelHook(afterDelHookFunc AfterDelHookFunc)
}
