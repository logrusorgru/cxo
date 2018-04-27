// GENERATED BY textFileToGoConst
// GitHub:     github.com/logrusorgru/textFileToGoConst
// input file: lua/setIncr.lua
// generated:  Fri Apr 27 15:40:27 +03 2018

package rediscxds

const setIncrLua = `
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
		"rc", rc,
		"access", now);

else

	rc     = incr;
	access = now;
	create = now;

	-- create new
	redis.call("HMSET", hex,
		"val", val,
		"rc", rc,
		"access", now,
		"create", now);

end

-- update expire (object can be removed between shutdown and start)
if expire ~= 0 then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return {rc, access, create};
`
