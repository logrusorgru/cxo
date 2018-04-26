
--[[
	keys/argv: hex, incr, now
	reply:     exists, rc, access
]]--

local hex  = ARGV[1];
local incr = ARGV[2];
local now  = ARGV[3];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false}
end

local object = redis.call("HMGET", hex,
	"rc",      -- 1
	"access"); -- 2

local rc = object[1] + incr;

-- incr and touch
redis.call("HMSET", hex,
	"rc", rc,       -- incr
	"access", now); -- touch

return {1, rc, object[2]};
