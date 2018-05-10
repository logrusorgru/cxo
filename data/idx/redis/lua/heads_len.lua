--
-- head_len.lua
--

-- in:  feed, scan_count
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

local hscan_no = 0; -- HSCAN number
local hscan;        -- HSCAN reply

local match = '[^f]*'; -- except the 'feed' key->value pair

-- break while the 'hscan_no' turns to be string '0'
while hscan_no ~= '0' do

	hscan = redis.call('HSCAN', feed, hscan_no,
		'MATCH', match,
		'COUNT', scan_count);

	hscan_no = hscan[1];
	count    = count + ((#hscan[2]) / 2);

end

return {has_feed, count};
