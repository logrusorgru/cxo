// GENERATED BY textFileToGoConst
// GitHub:     github.com/logrusorgru/textFileToGoConst
// input file: lua/del.lua
// generated:  Fri Apr 27 13:45:27 +03 2018

package rediscxds

const delLua = `
--[[
	keys/argv: expire, hex
	reply:     (nil)
]]--

local expire = ARGV[1];
local hex    = ARGV[2];

-- delete hash
redis.call("DEL", hex);

-- delete expire-waiter
if expire ~= 0 then
	redis.call("DEL", hex .. ".ex");
end

return;
`
