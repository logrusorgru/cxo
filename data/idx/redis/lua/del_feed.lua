--
-- del_feed.lua
--

-- in:  feed, scan_count
-- out: has_feed

local hex        = ARGV[1];              -- hex
local feed       = 'idx:feed:' .. hex;   -- idx:feed:hex
local match      = 'idx:' .. hex .. ':*' -- idx:hex:*
local scan_count = ARGV[2];              -- scan by
local scan_no    = 0;                    -- scan no
local scan;                              -- SCAN reply

-- has feed?
local has_feed = redis.call('EXISTS', feed);

if has_feed == 0 then
	return has_feed; -- 0
end

-- delete feed and heads
redis.call('DEL', feed);

-- replicate commands
redis.replicate_commands();

-- delete roots (keys)
-- brak while the 'scan_no' turns to be string '0'
while scan_no ~= '0' do

	scan = redis.call('SCAN', scan_no,
		'MATCH', match,
		'COUNT', scan_count);

	scan_no = scan[1];

	for _, key in ipairs(scan[2]) do
		redis.call('DEL', key);
	end

end

return has_feed; -- 1
