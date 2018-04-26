
--[[
	keys/argv: expire, hex, incr, now
	reply:     exists, rc, access
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local incr   = ARGV[3];
local now    = ARGV[4];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false};
end

local object = redis.call("HMGET", hex,
	"rc",      -- 1
	"access"); -- 2

local rc = object[1] + incr;

-- incr and touch
redis.call("HMSET", hex,
	"rc", rc,       -- incr
	"access", now); -- touch

-- update expire (object can be removed between shutdown and start)
if expire ~= 0 then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {1, rc, object[2]};