
--[[
	keys/argv: expire, hex, incr
	reply:     exists, vol, rc, access
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local incr   = ARGV[3];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false, false};
end

-- get last access time and value to get its volume
local object = redis.call("HMGET", hex,
	"val",     -- 1
	"access"); -- 2

-- incr
local rc = redis.call("HINCRBY", hex,
	"rc", incr);

-- update expire (object can be removed between shutdown and start)
if expire ~= 0 then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {1, string.len(object[1]), rc, object[2]};
