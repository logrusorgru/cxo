package redis

//
//go:generate textFileToGoConst -in=lua/add_head.lua -o=addHeadLua.go -c=addHeadLua
//
//go:generate textFileToGoConst -in=lua/del_feed.lua -o=delFeedLua.go -c=delFeedLua
//go:generate textFileToGoConst -in=lua/del_head.lua -o=delHeadLua.go -c=delHeadLua
//go:generate textFileToGoConst -in=lua/del_root.lua -o=delRootLua.go -c=delRootLua
//
//go:generate textFileToGoConst -in=lua/take_root.lua -o=takeRootLua.go -c=takeRootLua
//
//go:generate textFileToGoConst -in=lua/feeds_len.lua -o=feedsLenLua.go -c=feedsLenLua
//go:generate textFileToGoConst -in=lua/heads_len.lua -o=headsLenLua.go -c=headsLenLua
//go:generate textFileToGoConst -in=lua/roots_len.lua -o=rootsLenLua.go -c=rootsLenLua
//
//go:generate textFileToGoConst -in=lua/get_root.lua -o=getRootLua.go -c=getRootLua
//go:generate textFileToGoConst -in=lua/get_root_not_touch.lua -o=getRootNotTouchLua.go -c=getRootNotTouchLua
//
//go:generate textFileToGoConst -in=lua/has_head.lua -o=hasHeadLua.go -c=hasHeadLua
//go:generate textFileToGoConst -in=lua/has_root.lua -o=hasRootLua.go -c=hasRootLua
//
//go:generate textFileToGoConst -in=lua/range_roots.lua -o=rangeRootsLua.go -c=rangeRootsLua
//
//go:generate textFileToGoConst -in=lua/set_root.lua -o=setRootLua.go -c=setRootLua
//go:generate textFileToGoConst -in=lua/set_root_not_touch.lua -o=setRootNotTouchLua.go -c=setRootNotTouchLua
//
