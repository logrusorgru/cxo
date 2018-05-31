--
-- del_feed.lua
--

-- in:  feed
-- out: has_feed

local hex        = ARGV[1];              -- hex
local feed       = 'idx:feed:' .. hex;   -- idx:feed:hex
local match      = 'idx:' .. hex .. ':*' -- idx:hex:*

-- delete feed and heads
local has_feed = redis.call('DEL', feed);

if has_feed == 0 then
	return has_feed; -- 0
end

local keys = redis.call('KEYS', match);

for i, name in ipairs(keys) do
	redis.call('DEL', name);
end

return has_feed; -- 1
