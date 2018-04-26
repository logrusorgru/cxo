package rediscxds

// get

//go:generate textFileToGoConst -in=lua/get.lua -o=getLua.go -c=getLua
//go:generate textFileToGoConst -in=lua/getIncr.lua -o=getIncrLua.go -c=getIncrLua
//go:generate textFileToGoConst -in=lua/getIncrNotTouch.lua -o=getIncrNotTouchLua.go -c=getIncrNotTouchLua

// set

//go:generate textFileToGoConst -in=lua/setIncr.lua -o=setIncrLua.go -c=setIncrLua
//go:generate textFileToGoConst -in=lua/setIncrNotTouch.lua -o=setIncrNotTouchLua.go -c=setIncrNotTouchLua

// incr

//go:generate textFileToGoConst -in=lua/incr.lua -o=incrLua.go -c=incrLua
//go:generate textFileToGoConst -in=lua/incrNotTouch.lua -o=incrNotTouchLua.go -c=incrNotTouchLua
