
--[[
	keys/argv: hex, incr, now
	reply:     exists, val, rc, access, create
]]--

local hex  = ARGV[1];
local incr = ARGV[2];
local now  = ARGV[3];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false, false, false}
end

local object = redis.call("HMGET", hex,
	"val",     -- 1
	"rc",      -- 2
	"access",  -- 3
	"create"); -- 4

local rc = object[2] + incr;

-- incr and touch
redis.call("HMSET", hex,
	"rc", rc,       -- incr
	"access", now); -- touch

return {1, object[1], rc, object[3], object[4]};
