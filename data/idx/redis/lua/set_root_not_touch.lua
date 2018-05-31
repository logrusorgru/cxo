--
-- set_root_not_touch.lua
--

-- in:  feed, head, seq, hash, sig, now
-- out: has_feed, has_head, created, access, create

local hex  = ARGV[1];
local head = ARGV[2];
local seq  = ARGV[3];
local hash = ARGV[4];
local sig  = ARGV[5];
local now  = ARGV[6];

local feed = 'idx:feed:' .. hex;

local has_feed = 0;   -- bool
local has_head = 0;   -- bool
local access   = '0'; -- uint64
local create   = '0'; -- uint64
local created  = 0;   -- bool

--
has_feed = redis.call('EXISTS', feed);

if has_feed == 0 then
	return {has_feed, has_head, created, access, create};
end

--
has_head = redis.call('HEXISTS', feed, head);

if has_head == 0 then
	return {has_feed, has_head, created, access, create};
end

--
local root_key = 'idx:' .. hex .. ':' .. head .. ':' .. seq;

-- does the root already exist?
local exists   = redis.call('EXISTS', root_key);

if exists == 0 then
	-- create new

	created = 1

	-- add root index
	redis.call('ZADD', 'idx:' .. hex .. ':' .. head,
		seq, seq);

	-- add root content
	redis.call('HMSET', root_key,
		'sig', sig,
		'hash', hash,
		'access', '0', -- never been
		'create', now);

	create = now;

else
	-- update existsing

	-- created = 0;

	-- get create and last access times
	local object = redis.call('HMGET', root_key,
		'access',
		'create');

	access = object[1];
	create = object[2];

end

return {has_feed, has_head, created, access, create};
