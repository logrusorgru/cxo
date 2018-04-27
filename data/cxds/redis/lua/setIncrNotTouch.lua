
--[[
	keys/argv: expire, hex, val, incr, now
	reply:     rc, access, create
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local val    = ARGV[3];
local incr   = ARGV[4];
local now    = ARGV[5];

local exists = redis.call("EXISTS", hex);

local rc     = 0;
local access = 0;
local create = 0;

-- if exists
if exists == 1 then

	-- get existing
	local object = redis.call("HMGET", hex,
		"rc",      -- 1
		"access",  -- 2
		"create"); -- 3

	rc     = object[1] + incr;
	access = object[2];
	create = object[3];

	-- set new
	redis.call("HMSET", hex,
		"val", val,
		"rc", rc);

else

	rc     = incr;
	create = now;

	-- create new
	redis.call("HMSET", hex,
		"val", val,
		"rc", rc,
		"access", now,  -- access time can't be less then create time
		"create", now);

end

-- update expire (object can be removed between shutdown and start)
if expire ~= "0" then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {
	tonumber(rc),
	tonumber(access),
	tonumber(create)
};
