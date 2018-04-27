// GENERATED BY textFileToGoConst
// GitHub:     github.com/logrusorgru/textFileToGoConst
// input file: lua/take.lua
// generated:  Fri Apr 27 12:25:52 +03 2018

package rediscxds

const takeLua = `
--[[
	keys/argv: expire, hex
	reply:     exists, val, rc, access, create
]]--

local expire = ARGV[1];
local hex    = ARGV[2];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false, false, false};
end

local object = redis.call("HMGET", hex,
	"val",     -- 1
	"rc",      -- 2
	"access",  -- 3
	"create"); -- 4

-- delete hash
redis.call("DEL", hex);

-- delete expire-waiter
if expire ~= 0 then
	redis.call("DEL", hex .. ".ex");
end

return {1, object[1], object[2], object[3], object[4]};
`
