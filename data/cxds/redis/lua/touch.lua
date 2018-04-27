
--[[
	keys/argv: expire, hex, now
	reply:     exists, access
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local now    = ARGV[3];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, 0};
end

-- get last access time
local access = redis.call("HGET", hex, "access");

-- touch (update access time)
redis.call("HSET", hex, "access", now);

-- update expire (object can be removed between shutdown and start)
if expire ~= "0" then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {
	1,
	tonumber(access)
};
