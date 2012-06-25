require "config"

local env = assert(luasql.odbc())
local cnn = assert(env:connect(unpack(CNN_DSN)))

function return_count(...) return select('#',...), ... end

-- foreach([fetch_mode], [autoclose] , cb)

sql = 'select 1 as ID'
for i = 2, 100 do sql = sql .. ' union all select ' .. i end

function test1(cur)
  local c = 0
  local cnt = return_count(cur:foreach(function(row) c = c + 1 assert(row[1] >= 1 and row[1] <= 100) return end))
  assert(tostring(cur):find('closed'))
  assert(c == 100)
  assert(cnt == 0)
end

function test2(cur)
  local cnt, flag, str = return_count(cur:foreach('a', function(row) if row.ID == 50 then return true, 'match' end end))
  assert(tostring(cur):find('closed'))
  assert(cnt  == 2)
  assert(flag == true)
  assert(str  == 'match')
end

function test3(cur)
  local cnt, flag = return_count(cur:foreach('a', false, function(row) if row.ID == 50 then return nil end end))
  assert(not tostring(cur):find('closed'))
  assert(cnt  == 1)
  assert(flag == nil)
  assert(cur:close())
end

function test4(cur)
  local ok, err = pcall(cur.foreach, cur, function(row) error('some error') end)
  assert(tostring(cur):find('closed'))
  assert(ok == false)
end

test1(assert(cnn:execute(sql)))
test2(assert(cnn:execute(sql)))
test3(assert(cnn:execute(sql)))
test4(assert(cnn:execute(sql)))


stmt = cnn:statement()
test1(assert(stmt:execute(sql)))
test2(assert(stmt:execute(sql)))
test3(assert(stmt:execute(sql)))
test4(assert(stmt:execute(sql)))
stmt:destroy()

cnn:close()
env:close()
