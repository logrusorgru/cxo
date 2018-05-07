--
-- range_roots.lua
--

-- in:  feed, head, start, scan_by, reverse
-- out: {has_feed, has_head, {seqs}}

local hex     = ARGV[1];
local head    = ARGV[2];
local start   = ARGV[3];
local scan_by = ARGV[4];
local reverse = ARGV[5];

-- feed
local feed = 'idx:feed:' .. hex;

local has_feed = 0;
local has_head = 0;
local seqs     = {};

-- has feed
has_feed = redis.call('EXISTS', feed);

-- has not
if has_feed == 0 then
	return {has_feed, has_head, seqs};
end

-- has head
has_head = redis.call('HEXISTS', feed, head);

-- has not
if has_head == 0 then
	return {has_feed, has_head, seqs};
end

-- key for Z[REV]RANGEBYSCORE (idx:feed:head)
local zkey    = 'idx:' .. hex .. ':' .. head;

local command;
local min;
local max;

-- asc or desc
if reverse == '0' then
	-- direct
	command = 'ZRANGEBYSCORE';
	min     = start;
	max     = '+inf';
else
	-- inverse
	command = 'ZREVRANGEBYSCORE';
	min     = start;
	max     = '-inf';
end

--
local seqs = redis.call(command, zkey,
	min, max,
	'LIMIT', 0, scan_by);

return {has_feed, has_head, seqs};
