--
-- del_root.lua
--


-- in:  feed, head, seq
-- out: has_feed, has_head, deleted

local hex  = ARGV[1];
local head = ARGV[2];
local seq  = ARGV[3];

local has_feed = 0; -- bool
local has_head = 0; -- bool
local deleted  = 0; -- bool

local feed = 'idx:feed:' .. hex;

has_feed = redis.call('EXISTS', feed);

if has_feed == 0 then
	return {has_feed, has_head, deleted};
end

has_head = redis.call('HEXISTS', feed, head);

if has_head == 0 then
	return {has_feed, has_head, deleted};
end

deleted = redis.call('ZREM', 'idx:' .. hex .. ':' .. head, seq);

if deleted == 0 then
	return {has_feed, has_head, deleted};
end

redis.call('DEL', 'idx:' .. hex .. ':' .. head .. ':' .. seq);

return {has_feed, has_head, deleted};
