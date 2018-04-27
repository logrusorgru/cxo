
--[[
	keys/argv: expire, hex, val, rc, access, create
	reply:     prev_rc, prev_vol
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local val    = ARGV[3];
local rc     = ARGV[4];
local access = ARGV[5];
local create = ARGV[6];

local prev = redis.call("HMGET", hex,
	"val", -- 1
	"rc"); -- 2

-- create new or overwrite existing
redis.call("HMSET", hex,
	"val", val,
	"rc", rc,
	"access", access,
	"create", create);


-- update expire (object can be removed between shutdown and start)
if expire ~= 0 then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

if prev[1] == false then
	return {prev[2], prev[1]}; -- 0, 0
end

return {prev[2], string.len(prev[1])}; -- prev_rc, len(prev_val)
