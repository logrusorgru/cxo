--
-- get_root_not_touch.lua
--

-- in:  feed, head, seq
-- out: has_feed, has_head, hash, sig, access, create

local hex  = ARGV[1];
local head = ARGV[2];
local seq  = ARGV[3];

local has_feed = 0;   -- bool
local has_head = 0;   -- bool
local hash     = '';  -- cipher.SHA256
local sig      = '';  -- cipher.Sig
local access   = '0'; -- uint64
local create   = '0'; -- uint64

local feed = 'idx:feed:' .. hex;

has_feed = redis.call('EXISTS', feed);

if has_feed == 0 then
	return {has_feed, has_head, hash, sig, access, create};
end

has_head = redis.call('HEXISTS', feed, head);

if has_head == 0 then
	return {has_feed, has_head, hash, sig, access, create};
end

local root_key = 'idx:' .. hex .. ':' .. head .. ':' .. seq;

local root = redis.call('HMGET', root_key,
	'hash',
	'sig',
	'access',
	'create');

-- does not exist
if (not root) then
	return {has_feed, has_head, hash, sig, access, create};
end

hash   = root[1];
sig    = root[2];
access = root[3];
create = root[4];

return {has_feed, has_head, hash, sig, access, create};
