--
-- feeds_len.lua
--

-- in:  -
-- out: count (interger)

return #redis.call('KEYS', 'idx:feed:*');
