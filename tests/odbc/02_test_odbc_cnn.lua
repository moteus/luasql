require "config"

env = assert(luasql.odbc())

-- use env to connect
assert(not pcall(env.connect,env))
cnn,str = assert(env:driverconnect(CNN_DRV))

print("ConnectString:", str)

-- disconnect but do not destroy cnn object
assert(cnn:environment() == env)
assert(cnn:connected())
assert(cnn:disconnect())
assert(not cnn:connected())
assert(cnn:environment() == env)

-- reconnect using connection string
assert(cnn:driverconnect(str))

assert(cnn:connected())
assert(cnn:disconnect())
assert(not cnn:connected())

-- destroy cnn object
cnn:close()
-- rise errors
assert(not pcall(cnn.driverconnect, cnn, str))
assert(not pcall(cnn.environment, cnn))

-- create connection without connecting to db
cnn = assert(env:connection())
-- do not rise error
assert(not cnn:connected()) 
assert(not cnn:statement()) 
-- but we can not close env
assert(not pcall(env.close, env))

-- we can set params before connect
assert(cnn:setlogintimeout(10))
assert(cnn:driverconnect(str))
cnn:close()


-- we can set login timeout in env object
assert(env:setlogintimeout(10))
cnn = assert(env:driverconnect(str))
cnn:close()


env:close()
