
--[[
	keys/argv: expire, hex, val, incr, now
	reply:     created, rc, access, create
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local val    = ARGV[3];
local incr   = ARGV[4];
local now    = ARGV[5];

local created = 0;
local exists  = redis.call("EXISTS", hex);

local rc     = "";
local access = "";
local create = "";

-- if exists
if exists == 1 then

	-- created = 0;

	-- incr by
	redis.call("HINCRBY", hex,
		"rc", incr);

	-- get existing
	local object = redis.call("HMGET", hex,
		"rc",      -- 1
		"access",  -- 2
		"create"); -- 3

	rc     = object[1];
	access = object[2];
	create = object[3];

	-- set new
	redis.call("HSET", hex,
		"val", val);

else

	created = 1;

	rc     = incr;
	access = now;
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
	redis.call("SETEX", ":" .. hex, expire, 1);
end

return {created, rc, access, create};
