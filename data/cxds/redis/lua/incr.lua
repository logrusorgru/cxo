
--[[
	keys/argv: expire, hex, incr, now
	reply:     exists, vol, rc, access
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local incr   = ARGV[3];
local now    = ARGV[4];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, 0, 0, 0};
end

local object = redis.call("HMGET", hex,
	"val",     -- 1
	"rc",      -- 2
	"access"); -- 3

-- new rc
local rc  = object[2] + incr;

-- volume (size) of value
local vol = string.len(object[1]);

-- incr and touch
redis.call("HMSET", hex,
	"rc", rc,       -- incr
	"access", now); -- touch

-- update expire (object can be removed between shutdown and start)
if expire ~= "0" then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {
	1,
	vol,
	rc,
	tonumber(object[3])
};
