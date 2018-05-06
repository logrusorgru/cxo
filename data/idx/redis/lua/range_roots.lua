--
-- range_roots.lua
--

-- in:  feed, head, min, max
-- out: {has_feed, has_head, {seqs}}

local hex  = ARGV[1];
local head = ARGV[2];
local min  = ARGV[3];
local max  = ARGV[4];

-- feed
local feed = 'idx:feed:' .. hex;

local has_feed = 0;
local has_head = 0;
local seqs;

-- has feed
has_feed = redis.call('EXISTS', feed);

-- has not
if has_feed == 0 then
	return {has_feed, has_head, {}};
end

-- has head
has_head = redis.call('HEXISTS', feed, head);

-- has not
if has_head == 0 then
	return {has_feed, has_head, {}};
end

-- key for ZRANGEBYSCORE (idx:feed:head)
local zkey    = 'idx:' .. hex .. ':' .. head;
local command = 'ZRANGEBYSCORE';

-- asc or desc
if tonumber(min) > tonumber(max) then
	command = 'ZREVRANGEBYSCORE';
end

--
seqs = redis.call(command, zkey,
	min, max,
	'WITHSCORES');

return {has_feed, has_head, seqs};
