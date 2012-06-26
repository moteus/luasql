odbc = assert(require "luasql.odbc.ex")

-- [[ sybase
CNN_DRV = {
  {Driver='{Adaptive Server Anywhere 9.0}'};
  {UID='TestUser'};
  {PWD='sql'};
  {EngineName='DevelopServer'};
  {DatabaseName='EmptyDB'};
  {CommLinks='tcpip{host=127.0.0.1}'};
}

CNN_DSN = {'emptydb', 'TestUser', 'sql'}

CREATE_TABLE_RETURN_VALUE = -1
DEFINITION_KEY_TYPE_NAME = 'integer'
DEFINITION_STRING_TYPE_NAME = 'char(50)'
TEST_TABLE_NAME = 't'

--]]

--[[ firebird

CNN_DRV = {DSN="fbempty"}

CNN_DSN = {'fbempty'}
CREATE_TABLE_RETURN_VALUE = 0
DEFINITION_KEY_TYPE_NAME = 'integer'
DEFINITION_STRING_TYPE_NAME = 'VARCHAR(50)'
TEST_TABLE_NAME = 't'
--]]

---------------------------------------------------------------------
-- Shallow compare of two tables
---------------------------------------------------------------------
function table_compare(t1, t2)
  if t1 == t2 then return true; end

  for i, v in pairs(t1) do
      if t2[i] ~= v then return false; end
  end

  for i, v in pairs(t2) do
    if t1[i] ~= v then return false; end
  end

  return true
end

function get_key_no_case(t, i)
  if type(i) == "string" then
    return rawget(t, string.upper(i)) or rawget(t, string.lower(i))
  end

  return rawget(t, i)
end

function clone_upper(t)
  local o = {}
  for i,v in pairs(t) do
    if type(i) == "string" then
      i = string.upper(i)
    end
    assert(o[i] == nil)
    o[i] = v
  end
  return o
end

function table_compare_no_case(t1, t2)
  return table_compare(clone_upper(t1), clone_upper(t2))
end

function is_dsn_exists(env, dsn_name)
  local cnt, d = return_count(env:datasources(function(dsn) 
    if dsn[1]:upper() == dsn_name:upper() then
      return dsn
    end
  end))
  assert((cnt == 0) or ( d and d[1]:upper() == dsn_name:upper() ))
  return d
end

function return_count(...)
  return select('#', ...), ...
end

local function create_table (n)
  local t = {'id ' .. DEFINITION_KEY_TYPE_NAME}
  for i = 1, n do
    table.insert (t, "f"..i.." "..DEFINITION_STRING_TYPE_NAME)
  end
  return CNN:exec("create table " .. TEST_TABLE_NAME .. " ("..table.concat (t, ',')..")")
end

local function drop_table ()
  return CNN:exec("drop table " .. TEST_TABLE_NAME)
end

function suite_setup()
  print("creata test table ...")
  assert_true(CNN, 'Can not connect to database')
  drop_table()
  assert_equal(CREATE_TABLE_RETURN_VALUE, create_table(40))
  print("creata test table done!")
end

function suite_teardown()
  print("destroy test table ...")
  assert_equal(CREATE_TABLE_RETURN_VALUE, drop_table())
  print("destroy test table done!")
end

function RUN()

CNN, ERR = assert(odbc.connect(CNN_DRV))

lunatest.run(true)

CNN:destroy()

end