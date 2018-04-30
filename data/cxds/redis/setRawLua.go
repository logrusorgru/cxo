// GENERATED BY textFileToGoConst
// GitHub:     github.com/logrusorgru/textFileToGoConst
// input file: lua/setRaw.lua
// generated:  Mon Apr 30 18:09:09 +03 2018

package redis

const setRawLua = `
--[[
	keys/argv: expire, hex, val, rc, access, create
	reply:     overwritten, prev_vol, prev_rc
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local val    = ARGV[3];
local rc     = ARGV[4];
local access = ARGV[5];
local create = ARGV[6];

-- is exists
local exists = redis.call("EXISTS", hex);

-- previous values
local prev_vol = "";
local prev_rc  = "";

-- get previous rc and volume (length of val)
if exists == 1 then
	local prev = redis.call("HMGET", hex,
		"val", -- 1
		"rc"); -- 2

	prev_vol = tostring(string.len(prev[1]));
	prev_rc  = prev[2];
end

-- create new or overwrite existing
redis.call("HMSET", hex,
	"val", val,
	"rc", rc,
	"access", access,
	"create", create);


-- update expire (object can be removed between shutdown and start)
if expire ~= "0" then
	redis.call("SETEX", ":" .. hex, expire, 1);
end

return {exists, prev_vol, prev_rc};
`
