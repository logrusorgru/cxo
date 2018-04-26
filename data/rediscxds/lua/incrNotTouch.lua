
--[[
	keys/argv: expire, hex, incr
	reply:     exists, rc, access
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local incr   = ARGV[3];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false};
end

-- get last access time
local access = redis.call("HMGET", hex, "access");

-- incr
local rc = redis.call("HINCRBY", hex,
	"rc", incr);

-- update expire (object can be removed between shutdown and start)
if expire ~= 0 then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {1, rc, access};
