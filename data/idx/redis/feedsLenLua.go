// GENERATED BY textFileToGoConst
// GitHub:     github.com/logrusorgru/textFileToGoConst
// input file: lua/feeds_len.lua
// generated:  Fri Jun  1 01:09:30 +03 2018

package redis

const feedsLenLua = `--
-- feeds_len.lua
--

-- in:  -
-- out: count (interger)

return #redis.call('KEYS', 'idx:feed:*');
`