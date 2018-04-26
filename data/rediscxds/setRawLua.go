// GENERATED BY textFileToGoConst
// GitHub:     github.com/logrusorgru/textFileToGoConst
// input file: lua/setRaw.lua
// generated:  Thu Apr 26 23:05:50 +03 2018

package rediscxds

const setRawLua = `
--[[
	keys/argv: expire, hex, val, rc, access, create
	reply:     (nil)
]]--

local expire = ARGV[1];
local hex    = ARGV[2];
local val    = ARGV[3];
local rc     = ARGV[4];
local access = ARGV[5];
local create = ARGV[6];

-- create new or overwrite existing
redis.call("HMSET", hex,
	"val", val,
	"rc", rc,
	"access", access,
	"create", create);


-- update expire (object can be removed between shutdown and start)
if expire ~= 0 then
	redis.call("SETEX", hex .. ".ex", expire, 1);
end

return;
`