
--[[
	keys/argv: expire, hex, incr, now
	reply:     exists, val, rc, access, create
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local incr   = ARGV[3];
local now    = ARGV[4];

local exists = redis.call("EXISTS", hex);

-- if not exist
if exists == 0 then
	return {0, false, false, false, false};
end

-- I'm using (HINCRBY + HMGET + HSET)
-- instead of (HMGET + lua add + HSET)
-- because lua has 2^52 max integer,
-- but the rc can be 2^64

-- incr by
redis.call("HINCRBY", hex,
	"rc", incr);

-- get
local object = redis.call("HMGET", hex,
	"val",     -- 1
	"rc",      -- 2
	"access",  -- 3
	"create"); -- 4

-- touch
redis.call("HSET", hex,
	"access", now);

-- update expire (object can be removed between shutdown and start)
if expire ~= "0" then
	redis.call("SETEX", ":" .. hex, expire, 1);
end

return {1, object[1], object[2], object[3], object[4]};
