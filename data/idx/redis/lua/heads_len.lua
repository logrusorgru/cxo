--
-- head_len.lua
--

-- in:  feed
-- out: has_feed, count

local hex        = ARGV[1];
local scan_count = ARGV[2];

local feed = 'idx:feed:' .. hex;

local has_feed = 0;
local count    = 0;

has_feed = redis.call('EXISTS', feed);

if has_feed == 0 then
	return {has_feed, count};
end

count = #redis.call('HKEYS', feed) - 1;

return {has_feed, count};
