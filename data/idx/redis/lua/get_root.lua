--
-- get_root.lua
--

-- in:  feed, head, seq, now
-- out: has_feed, has_head, hash, sig, access, create

local hex  = ARGV[1];
local head = ARGV[2];
local seq  = ARGV[3];
local now  = ARGV[4];

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

-- does not exist
if redis.call('EXISTS', root_key) == 0 then
	return {has_feed, has_head, hash, sig, access, create};
end

local root = redis.call('HMGET', root_key,
	'hash',
	'sig',
	'access',
	'create');

hash   = root[1];
sig    = root[2];
access = root[3];
create = root[4];

-- touch
redis.call('HSET', root_key, 'access', now);

return {has_feed, has_head, hash, sig, access, create};
