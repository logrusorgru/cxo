
--[[
	keys/argv: expire, hex
	reply:     deleted, vol, rc
]]--

local expire = ARGV[1];
local hex    = ARGV[2];

local exists = redis.call("EXISTS", hex);

-- if not exists
if exists == 0 then
	return {0, false, false};
end

local object = redis.call("HMGET", hex,
	"val", -- 1
	"rc"); -- 2

local rc  = object[2];                       -- max is 2^64
local vol = tostring(string.len(object[1])); -- max is 2^52

-- delete hash
redis.call("DEL", hex);

-- delete expire-waiter
if expire ~= "0" then
	redis.call("DEL", ":" .. hex);
end

return {1, vol, rc};
