require "config"

local env = assert(luasql.odbc())
local cnn = assert(env:connect(unpack(CNN_DSN)))

local TRANSACTION_LEVEL = {
  "TRANSACTION_NONE","TRANSACTION_READ_UNCOMMITTED","TRANSACTION_READ_COMMITTED",
  "TRANSACTION_REPEATABLE_READ","TRANSACTION_SERIALIZABLE"
}

print('Is txn suppurt :  ',   cnn:supportsTransactions() )
print('Default txn level:', TRANSACTION_LEVEL[ assert(cnn:getDefaultTransactionIsolation()) ])
print('Supported levels:')
local max_lvl
for lvl, name in ipairs(TRANSACTION_LEVEL)  do 
  local flag = cnn:supportsTransactionIsolationLevel(lvl)
  print('', name, ':', flag)
  if flag then max_lvl = lvl end
end
print('Current level:', TRANSACTION_LEVEL[assert(cnn:gettransactionisolation())])
if max_lvl then 
  print('Set txn level:', TRANSACTION_LEVEL[max_lvl],  cnn:settransactionisolation(max_lvl))
  print('Current level:', TRANSACTION_LEVEL[assert(cnn:gettransactionisolation())])
end

print("support prepare:", cnn:supportsPrepare())
print("support bindparam:", cnn:supportsBindParam())

cnn:close()
env:close()
os.exit()
