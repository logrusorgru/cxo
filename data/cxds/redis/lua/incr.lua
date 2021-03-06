
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
	return {0, false, false, false};
end

-- incr by
redis.call("HINCRBY", hex,
	"rc", incr);

-- get last access time and value to get its volume
local object = redis.call("HMGET", hex,
	"val",     -- 1
	"rc",      -- 2
	"access"); -- 3

-- volume (size) of value
local vol = tostring(string.len(object[1]));

-- touch
redis.call("HSET", hex,
	"access", now); -- touch

-- update expire (object can be removed between shutdown and start)
if expire ~= "0" then
	redis.call("SETEX", ":" .. hex, expire, 1);
end

return {1, vol, object[2], object[3]};
