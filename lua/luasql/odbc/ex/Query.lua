--- Implement query class
--

local utils = require "luasql.odbc.ex.utils"

local ERR_MSGS     = assert(utils.ERR_MSGS)
local OPTIONS      = assert(utils.OPTIONS)
local cursor_utils = assert(utils.cursor)
local param_utils  = assert(utils.param)


local Query = {}
local Query_private = {}

------------------------------------------------------------------
do -- Query

--- ���������� ������ Query
--
-- @see Connection:query
-- @see Connection:prepare
function Query:destroy()
  self:close()
  if self.private_.stmt then
    self.private_.stmt:destroy()
    self.private_.stmt = nil
  end
end

--- ������������� ����� �������.
-- <br> ����� ��������� SQL ��� ��������� ���������� �����������.
-- <br> ������ �� ������ ���� ������ ��� �����������.
-- @param sql ����� �������
-- @return true � ������ �����
function Query:set_sql(sql)
  assert(type(sql) == 'string')

  if self:is_prepared() then return nil, ERR_MSGS.query_prepared end
  if self:is_opened()   then return nil, ERR_MSGS.query_opened end

  local psql, plst
  -- ���� �������������� ��������� � ������� � ��� ���������

  if not Query_private.need_replace_params(self) then
    -- ��� ��������� ����������� ���������� ���������� ������������� ������
    -- �������������� ����������� ����������
    if not self:get_config('IGNORE_NAMED_PARAMS') then
      psql, plst = param_utils.translate_params(sql)
      if not psql then return nil, plst end 
    end
  end

  if self.private_.stmt then self.private_.stmt:reset() end

  self.private_.sql                     = sql

  --
  self.private_.translated_sql          = psql

  -- ������ ��� ������������ ������ ��������� - �����
  -- ���� �� ������������ ����� bind, �� ��� ������� �� �����������
  self.private_.translated_param_list   = plst

  -- ������������ ��� �������� �������� ���������� ��� ��
  -- ���������� �����������. ���� ������������ ����� bind, 
  -- �� ��� ������� �� �����������
  self.private_.params                  = nil

  return true
end

--- Prepare query.
-- ���� ���������� ��� ������� �� ������������ �������������� �������, ��
-- ������������ ����������� ���������� � ����� ��� ����������. ��� ���� 
-- ������� ���������� �����, �� Query:is_prepared ���������� false.
-- @param sql [optional] ����� �������
-- @return true � ������ �����
function Query:prepare(sql)
  local ok, err 
  if sql then
    ok, err = self:set_sql(sql)
    if not ok then return nil, err end
  end

  -- ���� �������������� �������������� ������� � ��� ���������
  if (self.private_.stmt) and (not Query_private.get_config_param(self,'FORCE_REPLACE_PARAMS')) then
    ok, err = self.private_.stmt:prepare(self.private_.translated_sql or self.private_.sql)
    if not ok then return nil, err end
  end

  -- �������� ������ �������� �� ����� ������ 
  return true
end

--- ���������� ������� ���� ��� ������ �����������
--
--
function Query:is_prepared()
  return (self.private_.stmt ~= nil) and (self.private_.stmt:prepared())
end

--- Unprepare query.
-- 
--
function Query:unprepare()
  self:close() -- or error 
  if self:is_prepared() then
    self.private_.stmt:reset()
  end
end

--- ����������� �������� ���������.
-- <br> � �������� �������� ����� ��������� 2 ����������� ��������� PARAM_NULL � PARAM_DEFAULT.
-- @param paramID ����� ��������� (������� � 1) ��� ��� ���������
-- @param value   �������� ���������. 
-- @class function 
-- @name Query:bind[1]

--- ����������� �������� ���������.
-- <br> ��� ������� ����� ������������� ������ ���� ����������������� ������� �������������� ���������.
-- <br> � �������� �������� ����� ��������� 2 ����������� ��������� PARAM_NULL � PARAM_DEFAULT.
-- @param paramID   ����� ��������� (������� � 1) ��� ��� ���������
-- @param func  ������� ������������ ��� ��������� ������ � ������ ���������� �������.
--            <br>��� ����� ����� �������� ������ ��������. ����� ������ ��������� ���� � ������ �������� nil ���� 
--            �� ������������� �������� len, ���� ����� �������� len ���� ���� ��� ������ �������� len.
-- @param len [optional] �������� ������ ������.
-- @class function 
-- @name Query:bind[2]

--- ����������� �������� ����������.
-- @param params - ������� ����������(��������/����� => ��������)
-- @class function 
-- @name Query:bind[3]

--
function Query:bind(paramID, val, ...)
  local paramID_type = type(paramID)
  assert((paramID_type == 'string')or(paramID_type == 'number')or(paramID_type == 'table'))

  if self:is_opened() then  return nil, ERR_MSGS.query_opened end

  -- ��� ��������� ������ bind ��� ���������� ������������ �����������
  if Query_private.need_replace_params(self) then 
    -- �������������� ������ ����������� ���������
    self.private_.params = self.private_.params or {}
    if Query_private.get_config_param(self,'IGNORE_NAMED_PARAMS') then
      return nil, ERR_MSGS.deny_named_params
    end

    -- ��������� ��� ��������� ��� ���������� �����������
    if paramID_type == 'table' then
      for k, v in pairs(paramID) do self.private_.params[k] = v  end
    else
      if paramID_type == 'number' then
        return nil, ERR_MSGS.pos_params_unsupport
      end
      self.private_.params[ paramID ] = val
    end
    return true
  end

  assert(self.private_.stmt)
  assert(self.private_.stmt.bind)

  if 'number' == paramID_type then
    return param_utils.bind_param(self.private_.stmt, paramID, val, ...)
  end

  if paramID_type == 'table' then
    local k, v = next(paramID)
    if (type(k) == 'string') and (Query_private.get_config_param(self,'IGNORE_NAMED_PARAMS')) then
      -- � ����� ������� ������ ��������� ����������� � ����������� ���������
      -- ������� ���������� ��� ���������� �� ������ �����
      return nil, ERR_MSGS.deny_named_params
    end

    if type(k) == 'number' then
      -- ����� ��������� �� ��� ������ ���������, ������� �� ���������� ipairs
      for k, v in pairs(paramID)do
        local ok, err = param_utils.bind_param(self.private_.stmt, k, v)
        if not ok then return nil, err, k end
      end
    elseif type(k) == 'string' then
      -- ��� �������� ���������� ������ ���� ��� �� ������ ���������
      -- �� ������� ��������� �� ������������ ��������� �� �������� ������
      -- �������� ����� ���������������� ���������
      if not self.private_.translated_param_list then 
        return nil, ERR_MSGS.unknown_parameter .. k
      end

      for k, v in pairs(paramID) do
        for i, name in ipairs(self.private_.translated_param_list) do
          if name == k then
            local ok, err = param_utils.bind_param(self.private_.stmt, i, v)
            if not ok then return nil, err, i end
          end
        end
      end
    end
    return true
  end

  assert(paramID_type == 'string')
  if Query_private.get_config_param(self,'IGNORE_NAMED_PARAMS') then
    return nil, ERR_MSGS.deny_named_params
  end

  if (not self.private_.translated_param_list) or (not param_utils.ifind(paramID, self.private_.translated_param_list)) then 
    -- � ������� ��� ������������ ��������� � ����� ������
    return nil, ERR_MSGS.unknown_parameter .. paramID
  end

  -- �������� � ����� ������ ����� ���������� ��������� ���
  for i, name in ipairs(self.private_.translated_param_list) do
    if name == paramID then
      local ok, err = param_utils.bind_param(self.private_.stmt, i, val, ...)
      if not ok then return nil, err, i end
    end
  end

  return true
end

--- ��������� ������.
-- @param sql   [optional] ����� �������
-- @param param [optional] ��������� �������
-- @see Query:is_opened
-- @see Query:close
-- @see Query:fetch
-- @see Query:ifetch
-- @see Query:next_resultset
function Query:open(sql, param)
  -- ������ ����������� ������
  -- �������� ������ �������� ���������� ���� ��� autocommit
  
  if self:is_opened() then return nil, ERR_MSGS.query_opened end
  if param == nil then
    if type(sql) == 'table' then 
      param = sql
      sql   = nil
    end
  end

  local cur, err = Query_private.execute(self, sql, param)
  if not cur then return nil, err end
  if "userdata" ~= type(cur) then return nil, ERR_MSGS.no_cursor end
  self.private_.cursor = cur
  return true
end

--- ���������� ������ �������.
-- 
-- @see Query:open
-- @see Query:close
-- @see Query:fetch
-- @see Query:ifetch
-- @see Query:next_resultset
function Query:is_opened()
  return self.private_.cursor ~= nil
end

--- ��������� ������.
-- 
-- @see Query:open
-- @see Query:is_opened
-- @see Query:fetch
-- @see Query:ifetch
-- @see Query:next_resultset
function Query:close()
  if self.private_.cursor then
    self.private_.cursor:close()
    self.private_.cursor = nil
  end
  return true
end

--- ���������� ��������� ������.
-- @param t [optional] ������� ���� �������� ���������
--
-- @see Query:open
-- @see Query:is_opened
-- @see Query:close
-- @see Query:ifetch
-- @see Query:next_resultset
function Query:fetch(t)  return Query_private.fetch(self, 'a', t) end

--- ���������� ��������� ������.
-- @param t [optional] ������� ���� �������� ���������
--
-- @see Query:open
-- @see Query:is_opened
-- @see Query:close
-- @see Query:fetch
-- @see Query:next_resultset
function Query:ifetch(t) return Query_private.fetch(self, 'n', t) end

--- ����������� ������ �� ��������� Recordset.
-- <br>������ ������ ���� ������.
-- @return ������ ���� ��� ���������� Recordset
-- @return ������ ���� ���� ��������� Recordset
-- @see Query:open
-- @see Query:is_opened
-- @see Query:close
-- @see Query:fetch
-- @see Query:ifetch
function Query:next_resultset() 
  if not self:is_opened() then return nil, ERR_MSGS.query_not_opened end
  if not self.private_.cursor.getmoreresults then return nil, ERR_MSGS.not_support end
  return self.private_.cursor:getmoreresults()
end


--- ��������� ������ ������� �� ������ ���������� Recordset.
-- <br> ���� ������ ������ ������, �� �� �����������, �� �� ������������ ����� ����������
-- @param sql    [optional] ����� �������
-- @param params [optional] ��������� ��� �������
-- @return ���������� ������� ��������������� � �������,
-- @see Connection:query
-- @see Connection:prepare
-- @see Query:set_sql
-- @see Query:prepare
function Query:exec(sql, params)
  if type(sql) == 'table' then
    assert(params == nil)
    params = sql
    sql = nil
  end

  local res, err = Query_private.execute(self, sql, params)
  if not res then return nil, err end
  if 'userdata' == type(res) then
    if res.destroy then res:destroy() else res:close() end
    return nil, ERR_MSGS.ret_cursor
  end
  return res
end


---
-- @param sql
-- @param params
-- @param fn
-- @see Connection:rows
function Query:rows(...)       return Query_private.rows(self, 'a', ...) end

---
-- @param sql
-- @param params
-- @param fn
-- @see Connection:irows
function Query:irows(...)      return Query_private.rows(self, 'n', ...) end

---
-- @param sql
-- @param params
-- @see Connection:first_row
function Query:first_row(...)  return Query_private.first_row(self, 'a', ...) end

---
-- @param sql
-- @param params
-- @see Connection:first_irow
function Query:first_irow(...) return Query_private.first_row(self, 'n', ...) end

---
-- @param sql
-- @param params
-- @see Connection:first_value
function Query:first_value(...)
  local t, err = self:first_irow(...)
  if t then return t[1] end
  return nil, err
end

---
--
--
function Query:coltypes()
  if self:is_prepared() then return self.private_.stmt:getcoltypes() end
  if self:is_opened() then return self.private_.cursor:getcoltypes() end
  return nil, ERR_MSGS.query_not_opened
end

---
--
--
function Query:colnames()
  if self:is_prepared() then return self.private_.stmt:getcolnames() end
  if self:is_opened() then return self.private_.cursor:getcolnames() end
  return nil, ERR_MSGS.query_not_opened
end


--
-- configure
--

function Query:get_config(name )     return Query_private.get_config_param(self, name)      end

function Query:set_config(name, val) return Query_private.set_config_param(self, name, val) end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query private

function Query_private:need_replace_params()
  if Query_private.get_config_param(self,'FORCE_REPLACE_PARAMS') then
    return true
  end
  if self.private_.stmt == nil then return true end
  return false
end

function Query_private:get_config_param(name)
  if self.private_.lib_opt then
    local val = self.private_.lib_opt[name]
    if val ~= nil then return val end
  end
  return self.private_.cnn:get_config(name)
end

function Query_private:set_config_param(name, value)
  if not self.private_.lib_opt then
    self.private_.lib_opt = {}
  end
  self.private_.lib_opt[name] = value

  return Query_private.get_config_param(self, name)
end

function Query_private:execute(sql, params)
  assert((sql == nil) or type(sql) == 'string')
  assert((params == nil) or type(params) == 'table')

  if sql then 
    local ok, err = self:set_sql(sql)
    if not ok then return nil, err end
  end

  if not self.private_.sql then return nil, ERR_MSGS.no_sql_text end

  if type(params) == 'table' then
    ok, err = self:bind(params)
    if not ok then return nil, err end
  end

  if not Query_private.need_replace_params(self) then
    -- 1. ��������� ����� ODBC statement'��
    -- ������ �����������
    if self:is_prepared() then return self.private_.stmt:execute() end

    -- 2. ��������� ����� ODBC statement'��
    -- ������ �� �����������
    assert(self.private_.stmt)
    return self.private_.stmt:execute(self.private_.translated_sql or self.private_.sql)
  end

  -- 3. ��������� � �������
  if (self.private_.params) and (not self:get_config('IGNORE_NAMED_PARAMS')) then
    local q = param_utils.apply_params(self.private_.sql, self.private_.params)
    if self.private_.stmt then return self.private_.stmt:execute(q) end
    return self.cnn.private_cnn:execute(q)
  end

  -- 4. ������ ��� ����������
  if self.private_.stmt then return self.private_.stmt:execute(self.private_.sql) end
  return self.private_.cnn.private_.cnn:execute(self.private_.sql)
end

function Query_private:rows_iter(fetch_mode, sql, params)
  local cur, err = Query_private.execute(self, sql, params)
  if not cur then error(err) end
  if 'userdata' ~= type(cur) then error(ERR_MSGS.no_cursor, 0) end
  self.private_.cursor = cur

  return function ()
    local res  = cur:fetch({},fetch_mode)
    if res then return res
    else 
      -- close �� ���������� stmt ���� �� ����.
      self.private_.cursor = nil
      cur:close()
    end
  end
end

function Query_private:rows_cb(fetch_mode, sql, params, fn)
  local cur, err = Query_private.execute(self, sql, params)

  if not cur then return nil, err end
  if 'userdata' ~= type(cur) then return nil, ERR_MSGS.no_cursor end

  return cursor_utils.foreach_close(cur, fetch_mode, fn)
end

local Query_rows_switch= {
  --()
  nnn = function(self,fm,p1,p2,p3) return Query_private.rows_iter(self, fm, nil, nil)     end;
  -- (sql)
  snn = function(self,fm,p1,p2,p3) return Query_private.rows_iter(self, fm, p1,  nil)     end;
  --(params)
  tnn = function(self,fm,p1,p2,p3) return Query_private.rows_iter(self, fm, nil, p1)      end;
  --(cb)
  fnn = function(self,fm,p1,p2,p3) return Query_private.rows_cb  (self, fm, nil, nil, p1) end;
  --(sql,cb)
  sfn = function(self,fm,p1,p2,p3) return Query_private.rows_cb  (self, fm, p1,  nil, p2) end;
  --(params, cb)
  tfn = function(self,fm,p1,p2,p3) return Query_private.rows_cb  (self, fm, nil, p1,  p2) end;
  --(sql, params, cb)
  stf = function(self,fm,p1,p2,p3) return Query_private.rows_cb  (self, fm, p1,  p2,  p3) end;
}

function Query_private:rows(fetch_mode,p1,p2,p3)
  local mnemo = type(p1):sub(1,1)..type(p2):sub(1,1)..type(p3):sub(1,1)
  return (assert(Query_rows_switch[mnemo]))(self, fetch_mode, p1, p2, p3)
end

function Query_private:fetch(fetch_mode, res)
  assert((fetch_mode == 'n')or(fetch_mode == 'a'))
  assert((res==nil) or ('table' == type(res)))

  if not self:is_opened() then
    return nil, ERR_MSGS.query_not_opened;
  end

  local t = res or {}

  -- ���������� ������� �.�. ����� ������ ����� ���������� ��������� ��������
  local r, err  = self.private_.cursor:fetch(t, fetch_mode);
  if r == nil then
    if err == nil then -- ������ ��������
      self.private_.cursor:close()
      self.private_.cursor=nil
      return
    end
    return nil, err
  end

  assert(r == t)
  if not res then return unpack(r) end -- ����� ����� �������� ��� nil �� NULL?

  assert(r == res)
  return r
end

function Query_private:first_row(fetch_mode, sql, params)
  if type(sql) == 'table' then
    assert(params == nil)
    params = sql
    sql = nil
  end
  local cur, err = Query_private.execute(self, sql, params)
  if not cur then return nil, err end
  if 'userdata' ~= type(cur) then return nil, ERR_MSGS.no_cursor end

  local res, err = cur:fetch({}, fetch_mode)
  cur:close()
  if (res == nil) and (err ~= nil) then return nil, err end

  return res or {}
end

function Query_private:new(connection, sql, params)

  local cnn = assert(connection:handle())
  local stmt,err
  if cnn.statement then
    stmt, err = cnn:statement()
    if not stmt then return nil, err end
  end

  local t = {
    private_ = {
      cnn = connection;
      stmt = stmt;
    }
  };
  setmetatable(t,{__index = Query})
  if sql then
    local ok, err = t:set_sql(sql)
    if not ok then t:destroy() return nil, err end
    if params then
      ok, err = t:bind(params)
      if not ok then t:destroy() return nil, err end
    end
  end
  return t
end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Query ODBC Specific

--- ���������� ������� �� ���������� �������.
--
--
function Query:get_timeout()
  if (not self.private_.stmt) or (not self.private_.stmt.getquerytimeout) then
    return ERR_MSGS.not_support
  end
  return self.private_.stmt:getquerytimeout()
end

--- ������������� ������� �� ���������� �������.
--
--
function Query:set_timeout(value)
  if (not self.private_.stmt) or (not self.private_.stmt.setquerytimeout) then
    return ERR_MSGS.not_support
  end
  return self.private_.stmt:setquerytimeout(value)
end

--- ���������� ������������ ��������� ������� Resultset.
--
--
function Query:get_maxrows()
  if (not self.private_.stmt) or (not self.private_.stmt.getmaxrows) then
    return ERR_MSGS.not_support
  end
  return self.private_.stmt:getmaxrows()
end

--- ������������� ������������ ��������� ������� Resultset.
--
--
function Query:set_maxrows(value)
  if (not self.private_.stmt) or (not self.private_.stmt.setmaxrows) then
    return ERR_MSGS.not_support
  end
  return self.private_.stmt:setmaxrows(value)
end

---
--
--
function Query:get_maxfieldsize()
  if (not self.private_.stmt) or (not self.private_.stmt.getmaxfieldsize) then
    return ERR_MSGS.not_support
  end
  return self.private_.stmt:getmaxfieldsize()
end

---
--
--
function Query:set_maxfieldsize(value)
  if (not self.private_.stmt) or (not self.private_.stmt.setmaxfieldsize) then
    return ERR_MSGS.not_support
  end
  return self.private_.stmt:setmaxfieldsize(value)
end

end
------------------------------------------------------------------

Query.private_new = Query_private.new

return Query