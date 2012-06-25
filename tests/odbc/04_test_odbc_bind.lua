require "config"

local env = assert(luasql.odbc())
local cnn = assert(env:connect(unpack(CNN_DSN)))

qrySQL = [[
call DBA.sp_TestParam(
  inIntVal=?,
  inUIntVal=?,
  inDoubleVal=?,
  inStringVal=?,
  inBinaryVal=?,
  inDateVal=?,
  inNullVal=?,
  inDefaultVal=?,
  inBitVal=?
)]]

inIntVal     = -0x7FFFFFFF
inUIntVal    = 0xFFFFFFFF
inDoubleVal  = 1234.235664879123456
inStringVal  = "Hello world"
inBinaryVal  = "\000\001\002\003"
inDateVal    = "2011-01-01"
inNullVal    = nil
inDefaultVal = 1234
inBoolVal    = true
inGuidVal    = 'b1bb49a2-b401-4413-bebb-7acd10399875'




local stmt = cnn:statement()
function EXEC_AND_ASSERT(qrySQL)
  if qrySQL then
    cur = assert(stmt:execute(qrySQL))
  else
    cur = assert(stmt:execute())
  end
  assert(cur == stmt)

  outIntVal, outUIntVal, outDoubleVal, outStringVal,
  outBinaryVal, outDateVal, outNullVal, outDefaultVal,
  outBoolVal,outGuidVal = cur:fetch()
  cur:close()

  print('IntVal     =', outIntVal      )
  print('UIntVal    =', outUIntVal     )
  print('DoubleVal  =', outDoubleVal   )
  print('StringVal  =', outStringVal   )
  print('BinaryVal  =', outBinaryVal   )
  print('DateVal    =', outDateVal     )
  print('NullVal    =', outNullVal     )
  print('DefaultVal =', outDefaultVal  )
  print('BoolVal    =', outBoolVal     )

  assert(inIntVal     == outIntVal      )
  assert(inUIntVal    == outUIntVal     )
  assert(inDoubleVal  == outDoubleVal   )
  assert(inStringVal  == outStringVal   )
  assert(inBinaryVal  == outBinaryVal   )
  assert(inDateVal    == outDateVal     )
  assert(inNullVal    == outNullVal     )
  assert(inDefaultVal == outDefaultVal  )
  assert(inBoolVal    == outBoolVal     )
  print"================================="
end

assert(stmt:prepared() == false)
assert(stmt:getcolnames() == nil)
assert(stmt:getcoltypes() == nil)
assert(stmt:getparcount() == -1)

assert(stmt:bindnum    (1,inIntVal    ))
assert(stmt:bindnum    (2,inUIntVal   ))
assert(stmt:bindnum    (3,inDoubleVal ))
assert(stmt:bindstr    (4,inStringVal ))
assert(stmt:bindbin    (5,inBinaryVal ))
assert(stmt:bindstr    (6,inDateVal   ))
assert(stmt:bindnull   (7             ))
assert(stmt:binddefault(8             ))
assert(stmt:bindbool   (9,inBoolVal   ))
assert(stmt:getparcount() == -1)
EXEC_AND_ASSERT(qrySQL)

assert(stmt:getcolnames() ~= nil)
assert(stmt:getcoltypes() ~= nil)

assert(stmt:prepare(qrySQL))
assert((stmt:getparcount() == 9) or (stmt:getparcount() == -1))
assert(stmt:prepared() == true)
assert(stmt:getcolnames() ~= nil)
assert(stmt:getcoltypes() ~= nil)

assert(stmt:bindnum    (1,inIntVal    ))
assert(stmt:bindnum    (2,inUIntVal   ))
assert(stmt:bindnum    (3,inDoubleVal ))
assert(stmt:bindstr    (4,inStringVal ))
assert(stmt:bindbin    (5,inBinaryVal ))
assert(stmt:bindstr    (6,inDateVal   ))
assert(stmt:bindnull   (7             ))
assert(stmt:binddefault(8             ))
assert(stmt:bindbool   (9,inBoolVal   ))
EXEC_AND_ASSERT()

assert(stmt:reset())
assert(stmt:getparcount() == -1)
assert(stmt:prepared() == false)
assert(stmt:reset())
assert(stmt:getparcount() == -1)
assert(stmt:prepare(qrySQL))
assert((stmt:getparcount() == 9) or (stmt:getparcount() == -1))
assert(stmt:destroy())



function get_int()
  return inIntVal;
end
function get_uint()
  return inUIntVal
end
function get_double()
  return inDoubleVal
end
function get_date()
  return inDateVal
end
function get_bool()
  return inBoolVal
end
function create_get_bin_by(str,n)
  local pos = 1
  return function()
    local data = str:sub(pos,pos+n-1)
    pos = pos + n
    return data
  end
end

stmt = assert(cnn:prepare(qrySQL))

stmt:bindnull(7)
stmt:binddefault(8)

stmt:bindnum (1,inIntVal)
stmt:bindnum (2,inUIntVal)
stmt:bindnum (3,inDoubleVal)
stmt:bindstr (4,inStringVal)
stmt:bindbin (5,inBinaryVal)
stmt:bindstr (6,inDateVal)
stmt:bindbool(9,inBoolVal)
EXEC_AND_ASSERT()

stmt:bind(1,inIntVal)
stmt:bind(2,inUIntVal)
stmt:bind(3,inDoubleVal)
stmt:bind(4,inStringVal)
stmt:bind(5,inBinaryVal)
stmt:bind(6,inDateVal)
stmt:bind(9,inBoolVal)
EXEC_AND_ASSERT()

stmt:bindnum(1,get_int)
stmt:bindnum(2,get_uint)
stmt:bindnum(3,get_double)
stmt:bindstr(4,create_get_bin_by(inStringVal,10))
stmt:bindbin(5,create_get_bin_by(inBinaryVal,10))
stmt:bindstr(6,get_date, #inDateVal)
stmt:bindbool(9,get_bool)
EXEC_AND_ASSERT()

stmt:bind(1,get_int)
stmt:bind(2,get_uint)
stmt:bind(3,get_double)
stmt:bind(4,create_get_bin_by(inStringVal,10))
stmt:bind(5,create_get_bin_by(inBinaryVal,10))
stmt:bind(6,get_date, #inDateVal)
stmt:bind(9,get_bool)
EXEC_AND_ASSERT()


stmt:destroy()
cnn:close()
env:close()

