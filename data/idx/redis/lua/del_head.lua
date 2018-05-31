--
-- del_head.lua
--

-- in:  feed, head
-- out: has_feed, has_head

local hex        = ARGV[1];
local feed       = 'idx:feed:' ..  hex;
local head       = ARGV[2];

-- has feed
local has_feed = redis.call('EXISTS', feed);

-- has not
if has_feed == 0 then
	return {has_feed, 0};
end

-- has head
local has_head = redis.call('HEXISTS', feed, head);

-- has not
if has_head == 0 then
	return {has_feed, has_head};
end

-- delete head (hash field)
redis.call('HDEL', feed, head);

-- match by idx:feed:head*
-- to remove idx:feed:head     (ZADD)
-- and       idx:feed:head:seq (HMSET)
local match = 'idx:' .. hex .. ':' .. head .. '*';

-- delete roots (keys)
local keys = redis.call('KEYS', match);

for _, name in ipairs(keys) do
	redis.call('DEL', name);
end


return {has_feed, has_head};
