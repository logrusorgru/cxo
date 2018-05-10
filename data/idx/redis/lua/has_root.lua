--
-- has_root.lua
--

-- in:  feed, nonce, seq
-- out: has_feed, has_head, has_root


local hex        = ARGV[1];
local head       = ARGV[2];
local seq        = ARGV[3];

local feed       = 'idx:feed:' .. hex;

-- has feed
local has_feed = 0;
local has_head = 0;
local has_root = 0;

has_feed = redis.call('EXISTS', feed);

if has_feed == 1 then
	has_head = redis.call('HEXISTS', feed, head);
end

if has_head == 1 then
	has_root = redis.call('EXISTS',
		'idx:' .. hex .. ':' .. nonce .. ':' .. seq);
end

return {has_feed, has_head, has_root};
