--- Connection class
-- @class module
-- @name Connection


local utils       = assert(require "luasql.odbc.ex.utils")
local Environment -- late load
local Query       = assert(require "luasql.odbc.ex.Query")

local ERR_MSGS     = assert(utils.ERR_MSGS)
local OPTIONS      = assert(utils.OPTIONS)
local cursor_utils = assert(utils.cursor)
local param_utils  = assert(utils.param)


local Connection = {}
local Connection_private = {}

------------------------------------------------------------------
do -- Connection

--- ������� ����� ������ Connection.
-- 
-- <br> ��� �������� ��� �� ��������� ������ Environment.
-- ���� ������ ������������ ������ � �������� Connection.
-- <br> ��������� ������ �� ��������� � ��. ��� ����������� ���������� ������� Connection:open
-- <br> ��������� ����� ���� ����������� �����.
-- @param ... [optional] ��������� ��� ����������� � ��.
-- @return ������ Connection
-- @see Environment:connection
-- @see Environment:connection
-- @see Connection:open
function Connection:new(...)
  return Connection_private:new(nil, ...)
end

--- ���������� luasql.odbc.connection
-- 
function Connection:handle()
  return self.private_.cnn
end

--- ��������� ����������� � ��.
-- 
-- <br> ��������� ������ ���� ������������� � ����� �����. ������ ������� �������� �� � ������������, 
-- � �����/������ ��� ��������.
-- @param ... [optional] ��������� ��� ����������� � ��.
-- @return ������� ���������� �����������
-- @see Environment:connection
-- @see Environment:connection
-- @see Connection:new
function Connection:open(...)
  self:close()
  if select('#', ...) > 0 then self.private_.cnn_param = {...} end
  local cnn, cnnstr
  cnn = self.private_.cnn
  if cnn then 
    assert(cnn.connect)
    cnn, cnnstr = utils.connect(cnn, unpack(self.private_.cnn_param))
    if cnn then assert(cnn == self.private_.cnn) end
  else
    cnn, cnnstr = utils.connect(self.private_.env:handle(),unpack(self.private_.cnn_param))
  end

  if not cnn then return nil, cnnstr end
  self.private_.cnn = cnn
  self.private_.connect_string = cnnstr
  return true, cnnstr;
end

--- ��������� ����������� � ��.
-- <br> ������ �������� ��������� ��� ����������� �������������
-- @return true
function Connection:close()
  if self:is_opened() then
    assert(self.private_.cnn)
    local cur = next(self.private_.cursors)
    while(cur)do
      if cur.destroy then cur:destroy() else cur:close() end
      self.private_.cursors[cur] = nil
      cur = next(self.private_.cursors)
    end
    if self.private_.cnn.connect then
      self.private_.cnn:disconnect()
    else
      self.private_.cnn:close() self.private_.cnn = nil 
    end
  end
  return true
end

--- ���������� ������ Connection.
-- 
-- <br> ���� ������������ ������ ������������� ��� ������������� 
-- �������� � ��� �� ����������, �� ���������� ����������
function Connection:destroy()
  self:close()
  if self.private_.cnn then self.private_.cnn:close() self.private_.cnn = nil end
  if self.private_.is_own_env then
    if self.private_.env then self.private_.env:destroy() self.private_.env = nil end
  end
end

--- ���������� ������� ����������� � ��
--
--
function Connection:is_opened()
  if self.private_.cnn == nil then return false end
  if self.private_.cnn.connected then return self.private_.cnn:connected() end
  return true
end

--- ������������ ������� ����������.
-- <br> �� ����� ������ ���� autocommit=true
-- @see Connection:new
-- @see Connection:rollback
function Connection:commit()
  if self:is_opened() then
    return self.private_.cnn:commit()
  end
  return nil, ERR_MSGS.cnn_not_opened
end

--- �������� ������� ����������.
-- <br> �� ����� ������ ���� autocommit=true
-- @see Connection:new
-- @see Connection:commit
function Connection:rollback()
  if self:is_opened() then
    return self.private_.cnn:rollback()
  end
  return nil, ERR_MSGS.cnn_not_opened
end

--- ��������� ������ ������� �� ������ ���������� Recordset.
-- <br> ���� ������ ������ ������, �� �� �����������, �� �� ������������ ����� ����������
-- @param sql ����� �������
-- @param params [optional] ������� ���������� ��� �������
-- @return ���������� ������� ��������������� � �������
-- @class function
-- @name Connection:exec

--
function Connection:exec(...)
  local res, err = Connection_private.execute(self, ...)
  if not res then return nil, err end
  if 'userdata' == type(res) then
    if res.destroy then res:destroy() else res:close() end
    return nil, ERR_MSGS.ret_cursor
  end
  return res
end

--- �������� ��� �������� Recordset.
-- <br> ����������� ����
-- <br> ���� fn �� �������, �� ������������ �������� ��� generic for. ������������� �� ��� � ������ 
-- �������� ����������� ��� ����� ������� ����� ��������� �������. ������ ����������� ��� ����������
-- ����� ������.
-- <br> ���� fn ������� - ������������� �������� ������� ����� ����������� ������
-- @param sql ����� �������
-- @param params [optional] ��������� ��� �������
-- @param fn    [optional] callback 
-- @see callback_function
-- @usage# local sql = 'select ID, NAME from Clients where NAME = :NAME'
-- for row in db:rows(sql, {NAME='ALEX'}) do 
--    print(row.ID,row.NAME)
-- end
-- db:rows(sql, {NAME='ALEX'}, function(row)
--   print(row.ID,row.NAME)
-- end)
-- db:rows('select ID, NAME from Clients', function(row)
--   if row.NAME == 'ALEX' then return row end
-- end)
-- @class function
-- @name Connection:rows

--
function Connection:rows(...)       return Connection_private.rows(self, 'a',...)      end

--- �������� ��� �������� Recordset.
-- <br> ����������� ����
--
-- @see Connection:rows
function Connection:irows(...)      return Connection_private.rows(self, 'n',...)      end

--- ���������� ������ ������ Recordset.
-- <br> ������������ Connection:rows(sql,params,function(row) return row end)
-- @see Connection:rows
function Connection:first_row(...)  return Connection_private.first_row(self, 'a',...) end

--- ���������� ������ ������ Recordset.
-- <br> ������������ Connection:irows(sql,params,function(row) return row end)
-- @see Connection:irows
function Connection:first_irow(...) return Connection_private.first_row(self, 'n',...) end

--- ���������� ������ �������� ������ ������.
-- <br> ������������ (Connection:first_irow(sql,params))[1] � ������ �������� �� ������
-- @usage local cnt, err = db:first_value('select count(*) from Clients')
function Connection:first_value(...)
  local t, err = self:first_irow(...)
  if t then return t[1] end
  return nil, err
end

--- ��������� ����� ������ Query.
-- 
-- @param sql    [optional]
-- @param params [optional]
-- @return ������ Query
-- @class function
-- @name Connection:query

--
function Connection:query(...)
  return Query:private_new(self, ...)
end

--- ��������� ����� �������������� ������ Query.
-- 
-- ���� prepare ����������� � ������� - ����� Query ������������
function Connection:prepare(...)
  local q, err = Query:private_new(self)
  if not q then return nil, err end
  local ok
  ok, err = q:prepare(...)
  if not ok then q:destroy() return nil, err end
  return q
end

---
-- configure
function Connection:get_config(name )     return Connection_private.get_config_param(self, name)      end

function Connection:set_config(name, val) return Connection_private.set_config_param(self, name, val) end


end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection private

-- ���������� true ���� ����� ���������� ����������� ����������
-- ���� ���������� FORCE_REPLACE_PARAMS
-- ���� �� �������������� statement
function Connection_private:need_replace_params()
  if Connection_private.get_config_param(self, 'FORCE_REPLACE_PARAMS') then return true end
  return nil == self.private_.cnn.statement
end

function Connection_private:get_config_param(name)
  if self.private_.lib_opt then
    local val = self.private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return self.private_.env:get_config(name)
end

function Connection_private:set_config_param(name, value)
  if not self.private_.lib_opt then
    self.private_.lib_opt = {}
  end
  self.private_.lib_opt[name] = value

  return Connection_private.get_config_param(self, name)
end

-- ��������� ������
--
-- @return RowsAffected ���� ������ �� ������ RecordSet
-- @return statement ���� �� ��������������
-- @return cursor ���� ������ ��� ��� ���������� ��� statement �� ��������������
-- @return nil, err � ������ ������
function Connection_private:execute(sql, params)
  assert((type(sql) == 'string'))
  assert((params == nil)or(type(params) == 'table'))

  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end

  local cnn = self.private_.cnn
  if (params == nil) or (next(params) == nil) then
    return cnn:execute(sql)
  end

  if Connection_private.need_replace_params(self) then
    -- ����������� ��������� � ���������
    if self:get_config('IGNORE_NAMED_PARAMS') then
      return nil, ERR_MSGS.deny_named_params
    end

    local psql, err = param_utils.apply_params(sql,params)
    if not psql then return nil, err end
    return cnn:execute(psql)
  end

  -- ������� ������������ ����� statement

  -- ���� ��������� ����������� ���������, �� ����������� ������
  local psql, plst 
  if not self:get_config('IGNORE_NAMED_PARAMS') then
    psql, plst = param_utils.translate_params(sql)
    if not psql then return nil, plst end
  end

  local stmt, err = cnn:statement()
  if not stmt then return nil, err end

  if plst and (#plst > 0) then -- ���� ����������� ���������
    for i, pname in ipairs(plst) do
      local val = params[pname]
      if val == nil then
        stmt:destroy()
        return nil, ERR_MSGS.unknown_parameter .. pname
      end
      local ok, err = param_utils.bind_param(stmt, i, val)
      if not ok then 
        stmt:destroy()
        return nil, err
      end
    end
  else
    for i, v in ipairs(params) do
      if v == nil then
        stmt:destroy()
        return nil, ERR_MSGS.unknown_parameter .. pname
      end
      local ok, err = param_utils.bind_param(stmt, i, v)
      if not ok then 
        stmt:destroy()
        return nil, err
      end
    end
  end

  local ok, err = stmt:execute(psql or sql)
  if not ok then
    stmt:destroy()
    return nil, err
  end
  
  if type(ok) == 'userdata' then
    assert(ok == stmt)
    return stmt
  end
  stmt:destroy()
  return ok
end

-- ��������� ������ � ���������� ������� �������� ��� RecordSet
--
--
function Connection_private:rows_iter(fetch_mode, sql, params)
  local cur, err = Connection_private.execute(self, sql, params)
  if not cur then error(err) end
  if 'userdata' ~= type(cur) then error(ERR_MSGS.no_cursor, 0) end

  self.private_.cursors[cur] = true
  return function ()
    local res  = cur:fetch({},fetch_mode)
    if res then return res else 
      self.private_.cursors[cur] = nil
      if cur.destroy then cur:destroy() else cur:close() end
    end
  end
end

-- ��������� ������ � ���������� ������� �������� ��� RecordSet
-- ���� � ������� ��� ����������, �� ������ params ���� ������� nil.
-- fn ������ ���� �������� � ���� 4 ���������� �������.
function Connection_private:rows_cb(fetch_mode, sql, params, fn)
  local cur, err = Connection_private.execute(self, sql, params)

  if not cur then return nil, err end
  if 'userdata' ~= type(cur) then return nil, ERR_MSGS.no_cursor end

  return cursor_utils.foreach_destroy(cur, fetch_mode, fn)
end

-- �������-��������� ��� ������ rows_iter ��� rows_cb
-- (fm,sql)            rows_iter
-- (fm,sql,params)     rows_iter
-- (fm,sql,params,fn)  rows_cb
-- (fm,sql,fn)         rows_cb
function Connection_private:rows(fetch_mode,sql,p1,p2)
  assert((fetch_mode == 'n')or(fetch_mode == 'a'))
  assert(type(sql) == 'string')

  if (type(p1) == 'function') then 
    assert(p2 == nil)
    p2 = p1
    p1 = nil
  end

  if (type(p2) == 'function') then 
    return Connection_private.rows_cb(self, fetch_mode, sql, p1, p2)
  end

  assert(p2 == nil)
  return Connection_private.rows_iter(self, fetch_mode, sql, p1)
end

-- ���������� ������ ������ RecordSet
--
--
function Connection_private:first_row(fetch_mode, sql, params)
  assert((fetch_mode == 'n')or(fetch_mode == 'a'))

  local cur, err = Connection_private.execute(self, sql, params)
  if not cur then return nil, err end
  if 'userdata' ~= type(cur) then return nil, ERR_MSGS.no_cursor end

  local res, err = cur:fetch({}, fetch_mode)
  if cur.destroy then cur:destroy() else cur:close() end
  if (res == nil) and (err ~= nil) then return nil, err end

  return res or {}
end

--
-- @ctor
--
function Connection_private:new(env, ...)
  local err
  local own_env = false
  if not env then 
    Environment = Environment or require "luasql.odbc.ex.Environment"
    env, err = Environment:new()
    if not env then return nil, err end
    own_env = true
  end
  local cnn
  local henv = env:handle()
  if henv.connection then
    cnn, err = henv:connection()
    if not cnn then 
      if own_env then env:close() end
      return nil, err
    end
  end

  local t = {
    private_ = {
      cursors    = setmetatable({},{__mode='k'});
      env        = env;
      is_own_env = own_env;
      cnn_param  = {...};
      cnn        = cnn;
    }
  };
  return setmetatable(t,{__index = Connection})
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Connection ODBC specific

local function cnn_get_table_params(cb, get_fn, ...)
  if not get_fn then return nil, ERR_MSGS.not_support end
  local cur, err = get_fn(...)
  if not cur then return nil, err end

  if cb then return cursor_utils.foreach_destroy(cur, 'a', cb) end
  return cursor_utils.fetch_all_destroy(cur,'a')
end

-- ���� ������� �� ������������ �������, �� ��� ������ 
-- ����������� nil. ��� �� callback ������� (���� ��� ����)
-- ����������� �� ����� ������ � ������.
-- @return ok, callback, param_table
-- @return nil, err
local function unpack_catalog_params(cnn, ...)
  local has_catalog, err = cnn:supports_catalg_name() 
  if has_catalog == nil then return nil, err end
  local argv, argc = {...} , select('#', ...)
  local fn = argv[argc]
  if type(fn) ~= 'function' then
    fn = nil
  else
    assert(argc > 0)
    argc = argc - 1
  end

  if has_catalog then return true, fn, {unpack(argv,1,argc)} end
  return true, fn, {nil, unpack(argv,1,argc)} -- catalog must be nil
end

-- ������� ������� �������� � ������� ������ ����������
-- ���� ��� ��������.
local function make_cnn_catalog_function(fname)
  return function(self, ...)
    local ok, fn, argv = unpack_catalog_params(self, ...)
    if not ok then return nil, fn end
    return cnn_get_table_params(fn, self.private_.cnn[fname], self.private_.cnn, unpack(argv))
  end
end


--- ���������� ������ �������������� ����� ������
-- @param fn [optional] callback
-- @return ������ �������
-- @see callback_function
function Connection:typeinfo(fn)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  return cnn_get_table_params(fn, self.private_.cnn.gettypeinfo, self.private_.cnn)
end

--- ���������� ������ �������������� ����� ������
-- @param fn [optional] callback
-- @return ������ �������
-- @see callback_function
function Connection:tabletypes(fn)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  return cnn_get_table_params(fn, self.private_.cnn.gettables, self.private_.cnn, nil, nil, nil, '%')
end

--- ���������� ������ ���� ��
-- @param fn [optional] callback
-- @return ������ �������
-- @see callback_function
function Connection:schemas(fn)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  return cnn_get_table_params(fn, self.private_.cnn.gettables, self.private_.cnn, nil, '%', nil, nil)
end

--- ���������� ������ ���������
-- @param fn [optional] callback
-- @return ������ �������
-- @see callback_function
function Connection:catalogs(fn)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  return cnn_get_table_params(fn, self.private_.cnn.gettables, self.private_.cnn, '%', nil, nil, nil)
end

---
-- @param catalog   [optional] ������ ���� ������� ����������� ��������
-- @param schema    [optional]
-- @param tableName [optional]
-- @param unique    [optional] boolean
-- @param reserved  [optional] boolean
-- @param fn        [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:statistics 

--
Connection.statistics = make_cnn_catalog_function('getstatistics')


---
-- @param catalog   [optional] ������ ���� ������� ����������� ��������
-- @param schema    [optional]
-- @param tableName [optional]
-- @param types     [optional]
-- @param fn        [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:tables 

--
Connection.tables = make_cnn_catalog_function('gettables')

--
-- catalog, schema, tableName
--

---
-- @param catalog   [optional] ������ ���� ������� ����������� ��������
-- @param schema    [optional]
-- @param tableName [optional]
-- @param fn        [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:table_privileges

Connection.table_privileges = make_cnn_catalog_function('gettableprivileges')

--
-- catalog, schema, tableName
--

---
-- @param catalog   [optional] ������ ���� ������� ����������� ��������
-- @param schema    [optional]
-- @param tableName [optional]
-- @param fn        [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:primary_keys 

Connection.primary_keys = make_cnn_catalog_function('getprimarykeys')

--
-- catalog, schema, tableName
--

---
-- @param catalog   [optional] ������ ���� ������� ����������� ��������
-- @param schema    [optional]
-- @param tableName [optional]
-- @param fn        [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:index_info 

Connection.index_info = make_cnn_catalog_function('getindexinfo')

--
-- primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable
-- 

---
-- @param pc [optional] primary catalog (������ ���� ������� ����������� ��������)
-- @param ps [optional] primary schema  
-- @param pt [optional] primary table   
-- @param fc [optional] foreign catalog (������ ���� ������� ����������� ��������)
-- @param fs [optional] foreign schema  
-- @param ft [optional] foreign table   
-- @param fn [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:crossreference

--
function Connection:crossreference(...)
  local has_catalog, err = self:supports_catalg_name() 
  if has_catalog == nil then return nil, err end
  local argv, argc = {...} , select('#', ...)
  local fn = argv[argc]
  if type(fn) ~= 'function' then
    fn = nil
  else
    assert(argc > 0)
    argc = argc - 1
  end
  assert(math.mod(argc,2)) -- ��� ������� ������ ��������� ���������� ������ ���������
  local primary, foreign
  if has_catalog then 
    primary = {unpack(argv,1,argc/2)}
    foreign = {unpack(argv,1 + (argc/2), argc)}
  else
    primary = {nil, unpack(argv,1,argc/2)}
    foreign = {nil, unpack(argv,1 + (argc/2), argc)}
  end
  local primaryCatalog, primarySchema, primaryTable = unpack(primary)
  local foreignCatalog, foreignSchema, foreignTable = unpack(foreign)
  return cnn_get_table_params(fn, self.private_.cnn.crossreference, self.private_.cnn, 
    primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable
  )
end



--
-- catalog, schema, tableName, columnName
--

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param tableName        [optional] 
-- @param columnName       [optional] 
-- @param fn               [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:columns 

Connection.columns = make_cnn_catalog_function('getcolumns')

--
-- catalog, schema, tableName
--

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param tableName        [optional] 
-- @param fn               [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:special_columns 

Connection.special_columns = make_cnn_catalog_function('getspecialcolumns')

--
-- catalog, schema, procName
--

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param procName         [optional]  
-- @param fn               [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:procedures 

Connection.procedures = make_cnn_catalog_function('getprocedures')

--
-- catalog, schema, procName, colName
--

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param procName         [optional]   
-- @param colName          [optional]   
-- @param fn               [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:procedure_columns 

Connection.procedure_columns = make_cnn_catalog_function('getprocedurecolumns')

--
-- catalog, schema, tableName, columnName
--

---
-- @param catalog          [optional] ������ ���� ������� ����������� ��������
-- @param schema           [optional] 
-- @param tableName        [optional] 
-- @param columnName       [optional] 
-- @param fn               [optional] callback
-- @return ������ �������
-- @see callback_function
-- @class function
-- @name Connection:column_privileges 

Connection.column_privileges = make_cnn_catalog_function('getcolumnprivileges')

--- ���������� �������� ����.
--
--
function Connection:dbmsname()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getdbmsname then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getdbmsname()
end

---
--
--
function Connection:drvname()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getdrvname then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getdrvname()
end

--- ���������� ������ ��������.
--
--
function Connection:drvver()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getdrvver then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getdrvver()
end

--- ���������� ������ ODBC � ���� ������.
--
--
function Connection:odbcver()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getodbcver then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getodbcver()
end

--- ���������� ������ ODBC � ���� ���� �����.
--
--
function Connection:odbcvermm()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getodbcvermm then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getodbcvermm()
end

--- ���������� �������� ������������.
--
--
function Connection:username()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getUserName then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getUserName()
end

---
--
--
function Connection:set_autocommit(value)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.setautocommit then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:setautocommit(value)
end

---
--
--
function Connection:get_autocommit()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getautocommit then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getautocommit()
end

---
--
--
function Connection:set_catalog(value)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.setcatalog then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:setcatalog(value)
end

---
--
--
function Connection:get_catalog()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getcatalog then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getcatalog()
end

---
--
--
function Connection:set_readonly(value)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.setreadonly then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:setreadonly(value)
end

---
--
--
function Connection:get_readonly()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getreadonly then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:getreadonly()
end

---
--
--
function Connection:set_trace_file(value)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.settracefile then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:settracefile(value)
end

---
--
--
function Connection:get_trace_file()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.gettracefile then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:gettracefile()
end


---
--
--
function Connection:set_trace(value)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.settrace then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:settrace(value)
end

---
--
--
function Connection:get_trace()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.gettrace then return nil, ERR_MSGS.not_support end
  return self.private_.cnn:gettrace()
end

---
--
--
function Connection:supports_catalg_name()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.isCatalogName then return nil, ERR_MSGS.not_support end
  
  -- or supportsCatalogsInDataManipulation ???
  return self.private_.cnn:isCatalogName()
  
end

--
--
--
local TRANSACTION_LEVEL = {
  "TRANSACTION_NONE","TRANSACTION_READ_UNCOMMITTED","TRANSACTION_READ_COMMITTED",
  "TRANSACTION_REPEATABLE_READ","TRANSACTION_SERIALIZABLE"
}
for i = 1, #TRANSACTION_LEVEL do TRANSACTION_LEVEL[ TRANSACTION_LEVEL[i] ] = i end

--- ��������� ������������ �� ������� ����������.
-- <br> �������� �������� ������������� ������ ��������
-- @param lvl [optional] ������� �������� ���������� (�����/������)
-- @see transaction_level
function Connection:supports_transaction(lvl)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.supportsTransactions then return nil, ERR_MSGS.not_support end
  if lvl == nil then return self.private_.cnn:supportsTransactions() end
  if type(lvl) == 'string' then 
    local lvl_n = TRANSACTION_LEVEL[lvl] 
    if not lvl_n then return nil, ERR_MSGS.unknown_txn_lvl .. lvl end
    lvl = lvl_n
  end

  assert(type(lvl) == 'number')
  return self.private_.cnn:supportsTransactionIsolationLevel(lvl)
end

--- ���������� ������� ���������� �� ��������� ��� �����������.
-- @return �������� �������� 
-- @return ��������� ��������
function Connection:default_transaction()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.getDefaultTransactionIsolation then return nil, ERR_MSGS.not_support end
  local lvl, err = self.private_.cnn:getDefaultTransactionIsolation()
  if not lvl then return nil, err end
  return lvl, TRANSACTION_LEVEL[lvl]
end

--- ������������� ������� ������� ���������� ��� �����������.
-- @param lvl [optional] ������� �������� ���������� (�����/������). ���� nil, �� ������������ �������� �� ���������.
-- 
function Connection:set_transaction_level(lvl)
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.settransactionisolation then return nil, ERR_MSGS.not_support end

  local err 
  if lvl == nil then
    lvl, err = self:default_transaction()
    if not lvl then return nil, err end;
  end

  if type(lvl) == 'string' then 
    local lvl_n = TRANSACTION_LEVEL[lvl] 
    if not lvl_n then return nil, ERR_MSGS.unknown_txn_lvl .. lvl end
    lvl = lvl_n
  end

  assert(type(lvl) == 'number')
  return self.private_.cnn:settransactionisolation(lvl)
end

--- ���������� ������� ������� ���������� ��� �����������.
-- @return �������� �������� 
-- @return ��������� ��������
function Connection:get_transaction_level()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.gettransactionisolation then return nil, ERR_MSGS.not_support end

  local lvl, err = self.private_.cnn:gettransactionisolation()
  if not lvl then return nil, err end
  return lvl, TRANSACTION_LEVEL[lvl]
end

--- ��������� ������������ �� ������� �������� ���������� ��������.
--
function Connection:supports_bind_param()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.supportsBindParam then return false end
  return self.private_.cnn:supportsBindParam()
end

--- ��������� ������������ �� ������� �������������� �������.
--
function Connection:supports_prepare()
  if not self:is_opened() then return nil, ERR_MSGS.cnn_not_opened end
  if not self.private_.cnn.supportsPrepare then return false end
  return self.private_.cnn:supportsPrepare()
end

end
------------------------------------------------------------------


Connection.private_new = Connection_private.new

return Connection;