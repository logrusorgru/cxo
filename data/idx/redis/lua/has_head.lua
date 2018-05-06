--
-- has_head.lua
--

-- in:  feed, nonce
-- out: has_feed, has_head


local hex        = ARGV[1];
local head       = ARGV[2];
local feed       = 'idx:feed:' .. hex;

-- has feed
local has_feed = 0;
local has_head = 0;

has_feed = redis.call('EXISTS', feed);

if has_feed == 1 then
	has_head = redis.call('HEXISTS', feed, head);
end

return {has_feed, has_head};
