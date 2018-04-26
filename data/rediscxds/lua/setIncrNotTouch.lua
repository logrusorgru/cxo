
--[[
	keys/argv: hex, val, incr, now
	reply:     rc, access, create
]]--

local hex  = ARGV[1];
local val  = ARGV[2];
local incr = ARGV[3];
local now  = ARGV[4];

local exists = redis.call("EXISTS", hex);

local rc     = 0;
local access = 0;
local create = 0;

-- if not exist
if exists == 0 then

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

return {rc, access, create};
