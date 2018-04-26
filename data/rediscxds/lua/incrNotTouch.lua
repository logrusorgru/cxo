
--[[
	keys/argv: hex, incr
	reply:     exists, rc, access
]]--

local hex  = ARGV[1];
local incr = ARGV[2];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false}
end

-- get last access time
local access = redis.call("HMGET", hex, "access");

-- incr
local rc = redis.call("HINCRBY", hex,
	"rc", incr);

return {1, rc, access};
