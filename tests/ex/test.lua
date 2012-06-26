require "config"
require "lunatest"

local function test_obj_connect(cnn)
  local hcnn = cnn:handle()
  assert_true(cnn:is_opened())
  assert_true(cnn:close())
  assert_true(not cnn:is_opened())
  assert_true(hcnn == cnn:handle() or nil == cnn:handle())
  assert_true(cnn:open(CNN_DRV))
  assert_true(cnn:is_opened())
  assert_true(hcnn == cnn:handle() or nil == cnn:handle())
  assert_true(nil == cnn:destroy())
end

local function test_obj_execute(obj)
  assert_true(CNN:is_opened())

  assert_true(obj:exec("delete from ".. TEST_TABLE_NAME))

  assert_equal(0, obj:first_value("select count(*) from " .. TEST_TABLE_NAME))
  assert_true(table_compare({0}, obj:first_irow("select count(*) from " .. TEST_TABLE_NAME)))
  assert_true(table_compare_no_case({cnt = 0}, obj:first_row("select count(*) as cnt from " .. TEST_TABLE_NAME)))

  assert_equal(1, obj:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('a', 'b')"))
  assert_equal(1, obj:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('c', 'd')"))

  assert_equal(2, obj:first_value("select count(*) from " .. TEST_TABLE_NAME))
  assert_true(table_compare({2}, obj:first_irow("select count(*) from " .. TEST_TABLE_NAME)))
  assert_true(table_compare_no_case({cnt = 2}, obj:first_row("select count(*) as cnt from " .. TEST_TABLE_NAME)))
  
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = 'a'"))
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = ?", {'a'}))
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = :F1", {F1 = 'a'}))

  assert_true(table_compare({1}, obj:first_irow("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = 'a'")))
  assert_true(table_compare({1}, obj:first_irow("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = ?", {'a'})))
  assert_true(table_compare({1}, obj:first_irow("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = :F1", {F1 = 'a'})))

  assert_true(table_compare_no_case({cnt=1}, obj:first_row("select count(*) as cnt from " .. TEST_TABLE_NAME .. " where f1 = 'a'")))
  assert_true(table_compare_no_case({cnt=1}, obj:first_row("select count(*) as cnt from " .. TEST_TABLE_NAME .. " where f1 = ?", {'a'})))
  assert_true(table_compare_no_case({cnt=1}, obj:first_row("select count(*) as cnt from " .. TEST_TABLE_NAME .. " where f1 = :F1", {F1 = 'a'})))

  assert_equal(2, obj:exec("delete from ".. TEST_TABLE_NAME))
  assert_equal(0, obj:exec("delete from ".. TEST_TABLE_NAME))
end

local function test_obj_config(obj)
  assert_true(obj:exec("delete from ".. TEST_TABLE_NAME))
  assert_equal(1, obj:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('a', 'b')"))
  assert_equal(1, obj:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('c', 'd')"))

  obj:set_config("FORCE_REPLACE_PARAMS", true)
  assert_true(obj:get_config("FORCE_REPLACE_PARAMS"))
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = 'a'"))
  assert_nil     (obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = ?", {'a'}))
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = :F1", {F1 = 'a'}))

  obj:set_config("IGNORE_NAMED_PARAMS", true)
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = 'a'"))
  assert_nil     (obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = ?", {'a'}))
  assert_nil     (obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = :F1", {F1 = 'a'}))

  obj:set_config("FORCE_REPLACE_PARAMS", false)
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = 'a'"))
  assert_equal(1, obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = ?", {'a'}))
  assert_nil     (obj:first_value("select count(*) from " .. TEST_TABLE_NAME .. " where f1 = :F1", {F1 = 'a'}))

  assert_equal(2, obj:exec("delete from ".. TEST_TABLE_NAME))
end

local function test_obj_fetch(obj)
  assert_true(obj:exec("delete from ".. TEST_TABLE_NAME))
  assert_equal(1, obj:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('a', 'b')"))
  assert_equal(1, obj:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('c', 'd')"))
  
  local nt = {{ 'a', 'b' };{ 'c', 'd' }}
  local at = {{ f1 = 'a', f2 = 'b' };{ f1 = 'c', f2 = 'd' }}
  local function cmp_iter(t)
    local i = 0
    return function(row)
      i = i + 1
      return table_compare_no_case(t[i], row)
    end
  end

  local sql = "select f1, f2 from ".. TEST_TABLE_NAME .. " order by f1"

  assert_equal('a', obj:first_value(sql))
  
  local cmp
  cmp = cmp_iter(at) for row in obj:rows(sql)  do assert_true(cmp(row)) end
  cmp = cmp_iter(nt) for row in obj:irows(sql) do assert_true(cmp(row)) end
  cmp = cmp_iter(at) obj:rows(sql, function(row) assert_true(cmp(row)) end)
  cmp = cmp_iter(nt) obj:irows(sql,function(row) assert_true(cmp(row)) end)
  local sql = "select f1, f2 from ".. TEST_TABLE_NAME .. " where f1 = ? or f1 = ? order by f1"
  local params = {'a','c'}
  cmp = cmp_iter(at) for row in obj:rows(sql,params)  do assert_true(cmp(row)) end
  cmp = cmp_iter(nt) for row in obj:irows(sql,params) do assert_true(cmp(row)) end
  cmp = cmp_iter(at) obj:rows(sql,params, function(row) assert_true(cmp(row)) end)
  cmp = cmp_iter(nt) obj:irows(sql,params,function(row) assert_true(cmp(row)) end)
end

local function check_dsn(dsn)
  assert_true(#dsn == 2)
  assert_true(type(dsn[1]) == 'string')
  assert_true(type(dsn[2]) == 'string')
end

local function check_drv(drv)
  assert_true((#drv == 2)or(#drv == 1))
  assert_true(type(drv[1]) == 'string')
  assert_true((drv[2] == nil) or (type(drv[2]) == 'table'))
end

function test_env()
  local env = odbc.environment()
  local t,err = env:drivers()
  assert_table(t,err)
  table.foreachi(t,function(_,v)check_drv(v) end)
  assert_equal(0, (return_count(env:drivers(check_drv))))

  local t,err = env:datasources()
  assert_table(t,err)
  table.foreachi(t,function(_,v)check_dsn(v) end)
  assert_equal(0, (return_count(env:datasources(check_dsn))))

  -- we can find 
  -- @see is_dsn_exists
  assert_true(is_dsn_exists(env, CNN_DSN[1]), 'test dsn does not exixts')
  assert_equal(nil, env:destroy())
end

function test_odbc_connect()
  local cnn, err = odbc.connect(CNN_DRV)
  assert_true(cnn, err)
  test_obj_connect(cnn)
end

function test_env_connect()
  local env = odbc.environment()
  local cnn = env:connect(CNN_DRV)
  test_obj_connect(cnn)
end

function test_env_connection()
  local env = odbc.environment()
  local cnn = env:connection()
  assert_true(cnn:open(CNN_DRV))
  test_obj_connect(cnn)

  local cnn = env:connection(CNN_DRV)
  assert_true(cnn:open())
  test_obj_connect(cnn)
end

function test_cnn_tables()
  assert_true(CNN:is_opened())
  assert_equal(TEST_TABLE_NAME:upper(), 
    CNN:tables(function(t)
      if t.TABLE_NAME:upper() == TEST_TABLE_NAME:upper() then
        return TEST_TABLE_NAME:upper()
      end
    end)
  )
end

function test_cnn_execute()
  local cnn = odbc.connect(CNN_DRV)
  test_obj_execute(cnn)
  assert_nil(cnn:destroy())
end

function test_qry_execute()
  local cnn = odbc.connect(CNN_DRV)
  local qry = cnn:query()
  test_obj_execute(qry)

  assert_error(function() cnn:destroy() end)
  assert_nil(qry:destroy())
  assert_nil(cnn:destroy())
end

function test_cnn_config()
  local cnn = odbc.connect(CNN_DRV)
  test_obj_config(cnn)
  assert_nil(cnn:destroy())
end

function test_qry_config()
  local cnn = odbc.connect(CNN_DRV)
  local qry = cnn:query()
  test_obj_config(qry)
  assert_error(function() cnn:destroy() end)
  assert_nil(qry:destroy())

  assert_true(qry:exec("delete from ".. TEST_TABLE_NAME))
  assert_equal(1, qry:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('a', 'b')"))
  assert_equal(1, qry:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('c', 'd')"))

  qry = cnn:query("select count(*) from " .. TEST_TABLE_NAME .. ' where f1 = ?',{'a'})
  assert_equal(1, qry:first_value())
  assert_equal(1, cnn:exec("insert into ".. TEST_TABLE_NAME .. " (f1, f2) values ('a', 'b')"))
  assert_equal(2, qry:first_value())

  qry:set_config("FORCE_REPLACE_PARAMS", true)
  assert_nil(qry:first_irow("select 1 from " .. TEST_TABLE_NAME .. ' where f1 = ?',{'a'}))

  assert_equal(3, qry:exec("delete from ".. TEST_TABLE_NAME))

  assert_nil(qry:destroy())

  assert_nil(cnn:destroy())
end

function test_cnn_config()
  local cnn = odbc.connect(CNN_DRV)
  test_obj_fetch(cnn)
  assert_nil(cnn:destroy())
end

RUN()
