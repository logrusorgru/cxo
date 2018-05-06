--
-- feeds_len.lua
--

-- in:  scan_count
-- out: count (interger)

local scan_count = ARGV[1];     -- count
local scan_no    = 0;           -- number of SCAN
local scan;                     -- SCAN reply
local length     = 0;           -- total feeds length
local match      = 'idx:feed:*' -- matching pattern

-- break while the 'scan_no' turns to be string '0'
while scan_no ~= '0' do

	scan = redis.call('SCAN', scan_no,
		'MATCH', match,
		'COUNT', scan_count);

	scan_no = scan[1];
	length = length + #scan[2];

end

return length;
