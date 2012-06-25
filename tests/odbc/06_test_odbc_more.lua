require "config"

local env = assert(luasql.odbc())
local cnn = assert(env:connect(unpack(CNN_DSN)))

sql = [[begin 
select 1 as IntVal1, 2 as IntVal2
union all
select 11, 12;
select 'hello' as StrVal1, 'world' as StrVal2, '!!!' as StrVal3
union all
select 'some', 'other', 'row';
end]]

function FETCH_AND_ASSERT(cur)
  local cols = cur:getcolnames()
  assert(#cols == 2)
  assert(cols[1]:upper() == 'INTVAL1'and cols[2]:upper() == 'INTVAL2')
  local t = {}
  assert(cur:fetch(t,"n"))
  assert(t[1] == 1 and t[2] == 2)
  assert(cur:fetch(t,"n"))
  assert(t[1] == 11 and t[2] == 12)
  local c2 = cur:getmoreresults()
  assert(c2 == cur)

  cols = cur:getcolnames()
  assert(#cols == 3)
  assert(cols[1]:upper() == 'STRVAL1' and cols[2]:upper() == 'STRVAL2' and cols[3]:upper() == 'STRVAL3')
  assert(cur:fetch(t,"n"))
  assert(t[1] == 'hello' and t[2] == 'world' and t[3] == '!!!')
  assert(cur:fetch(t,"n"))
  assert(t[1] == 'some' and t[2] == 'other' and t[3] == 'row')

  assert(nil == cur:getmoreresults())
  assert(cur:close())
end

FETCH_AND_ASSERT(assert(cnn:execute(sql)))

local stmt = assert(cnn:statement())
FETCH_AND_ASSERT(assert(stmt:execute(sql)))
assert(stmt:destroy())

stmt = assert(cnn:prepare(sql))
FETCH_AND_ASSERT(assert(stmt:execute()))
assert(stmt:destroy())

cnn:close()
env:close()
