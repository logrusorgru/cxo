
--[[
	keys/argv: expire, hex
	reply:     deleted, vol, rc
]]--

local expire = ARGV[1];
local hex    = ARGV[2];

local exists = redis.call("EXISTS", hex);

-- if not exists
if exists == 0 then
	return {0, 0, 0};
end

local object = redis.call("HMGET", hex,
	"val", -- 1
	"rc"); -- 2

-- delete hash
redis.call("DEL", hex);

-- delete expire-waiter
if expire ~= "0" then
	redis.call("DEL", hex .. ".ex");
end

return {
	1,
	string.len(object[1]),
	tonumber(object[2])
};
