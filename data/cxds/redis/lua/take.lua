
--[[
	keys/argv: expire, hex
	reply:     exists, val, rc, access, create
]]--

local expire = ARGV[1];
local hex    = ARGV[2];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, 0, 0, 0};
end

local object = redis.call("HMGET", hex,
	"val",     -- 1
	"rc",      -- 2
	"access",  -- 3
	"create"); -- 4

-- delete hash
redis.call("DEL", hex);

-- delete expire-waiter
if expire ~= "0" then
	redis.call("DEL", hex .. ".ex");
end

return {
	1,
	object[1],
	tonumber(object[2]),
	tonumber(object[3]),
	tonumber(object[4])
};
