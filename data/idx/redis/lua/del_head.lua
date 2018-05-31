--
-- del_head.lua
--

-- in:  feed, head, scan_count
-- out: has_feed, has_head

-- replicate commands
redis.replicate_commands();

local hex        = ARGV[1];
local feed       = 'idx:' ..  hex;
local head       = ARGV[2];
local scan_count = ARGV[3];

local scan_no = 0;
local scan;

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
-- brak while the 'scan_no' turns to be string "0"
while scan_no ~= '0' do

	scan = redis.call('SCAN', scan_no,
		'MATCH', match,
		'COUNT', scan_count);

	scan_no = scan[1];

	for _, key in ipairs(scan[2]) do
		redis.call('DEL', key);
	end

end

return {has_feed, has_head};
