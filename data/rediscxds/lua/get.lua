
--[[
	keys/argv: expire, hex, now
	reply:     exists, val, rc, access, create
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local now    = ARGV[3];

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

-- touch (update access time)
redis.call("HSET", hex, "access", now);

-- update expire (object can be removed between shutdown and start)
if expire ~= 0 then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {1, object[1], object[2], object[3], object[4]};
