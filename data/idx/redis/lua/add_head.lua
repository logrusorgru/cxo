--
-- add_head.lua
--

-- in:  feed, head
-- out: has_feed

local feed = 'idx:feed:' .. ARGV[1];
local head = ARGV[2];

local has_feed = redis.call('EXISTS', feed);

if has_feed == 1 then
	redis.call('HSET', feed, head, 1);
end

return has_feed;
