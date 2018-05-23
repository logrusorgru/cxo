// GENERATED BY textFileToGoConst
// GitHub:     github.com/logrusorgru/textFileToGoConst
// input file: lua/roots_len.lua
// generated:  Wed May 23 11:14:58 +03 2018

package redis

const rootsLenLua = `--
-- head_len.lua
--

-- in:  feed, head
-- out: has_feed, has_head, count

local hex        = ARGV[1];
local head       = ARGV[2];
local scan_count = ARGV[3];

local feed = 'idx:feed:' .. hex;

local has_feed = 0;
local has_head = 0;
local count    = 0;

-- has feed
has_feed = redis.call('EXISTS', feed);

if has_feed == 0 then
	return {has_feed, has_head, count};
end

-- has head
has_head = redis.call('HEXISTS', feed, head);

if has_head == 0 then
	return {has_feed, has_head, count};
end

-- count
count = redis.call('ZCOUNT', 'idx:' .. hex .. ':' .. head,
	'-inf', '+inf');

return {has_feed, has_head, count};
`
