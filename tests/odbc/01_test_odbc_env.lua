require "config"

function check_dsn(dsn)
	assert(#dsn == 2)
	assert(type(dsn[1]) == 'string')
	assert(type(dsn[2]) == 'string')
end

function check_drv(drv)
	assert((#drv == 2)or(#drv == 1))
	assert(type(drv[1]) == 'string')
	assert((drv[2] == nil) or (type(drv[2]) == 'table'))
end

env = assert(luasql.odbc())

local t = assert(env:getdrivers())
table.foreachi(t,function(_,v)check_drv(v) end)
assert(0 == return_count(env:getdrivers(check_drv)))

local t = assert(env:getdatasources())
table.foreachi(t,function(_,v)check_dsn(v) end)
assert(0 == return_count(env:getdatasources(check_dsn)))

assert(is_dsn_exists(env, CNN_DSN[1]))


env:close()
