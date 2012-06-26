local luasql = require "luasql.odbc"
local driver = assert(luasql.odbc)

local OPTIONS = {
  -- всегда замен€ть параметры подстановкой строки
  FORCE_REPLACE_PARAMS = false;
  
  -- не пытатс€ преобразовывать именованные параметры
  -- это необходимо дл€ предотвращени€ изменени€ текста SQL перед выполнением
  -- при этом параметры будут поддерживатс€ только если проддерживаетс€ bind(будут использоватьс€ только '?')
  IGNORE_NAMED_PARAMS = false;
};

local ERR_MSGS = {
  unsolved_parameter   = 'unsolved name of parameter: ';
  unknown_parameter    = 'unknown parameter: ';
  no_cursor            = 'query has not returned a cursor';
  ret_cursor           = 'query has returned a cursor';
  query_opened         = 'query is already opened';
  cnn_not_opened       = 'connection is not opened';
  query_not_opened     = 'query is not opened';
  query_prepared       = 'query is already prepared';
  deny_named_params    = 'named parameters are denied';
  no_sql_text          = 'SQL text was not set';
  pos_params_unsupport = 'positional parameters are not supported';
  not_support          = 'not supported';
  unknown_txn_lvl      = 'unknown transaction level: '; 
};

local function val_if_empty(val, ...)
  if select('#', ...) == 0 then return val end
  return ...
end

local function clone(t) 
  local o = {} 
  for k,v in pairs(t) do o[k]=v end 
  return o
end

local function collector(t) 
  return function(row) table.insert(t,clone(row)) end 
end

local cursor_utils = {} do

function cursor_utils.foreach(cur, fetch_mode, autoclose, fn)
  if cur.foreach then return cur:foreach(fetch_mode, autoclose, fn) end

  local do_ = function()
    local res, err = {}
    while(1)do
      res, err = cur:fetch(res, fetch_mode)
      if res == nil then 
        if err ~= nil then return {nil, err} end
        return {}
      end
      local t = {fn(res)}
      if next(t) then return t end
    end
  end

  local ok, t 
  if autoclose then
    ok, t = pcall(do_)
    cur:close()
    if not ok then error(t) end
  else t = do_() end

  return unpack(t)
end

function cursor_utils.fetch_all(cur, fetch_mode)
  local res, err = cur:fetch({},fetch_mode)
  if (res == nil) and (err ~= nil) then 
    cur:close() 
    return nil, err
  end
  local t = {}
  while res do
    table.insert(t,res)
    res, err = cur:fetch({},fetch_mode)
  end
  if err then return nil, err, t end
  return t
end

function cursor_utils.foreach_destroy(cur, fetch_mode, fn)
  local t = {pcall(cursor_utils.foreach, cur, fetch_mode, false, fn)}
  if cur.destroy then cur:destroy() else cur:close() end
  if not t[1] then error(t[2], 0) end
  -- return val_if_empty(true, unpack(t,2))
  return unpack(t,2)
end

function cursor_utils.foreach_close(cur, fetch_mode, fn)
  -- return val_if_empty(true, cursor_utils.foreach(cur, fetch_mode, true, fn))
  return cursor_utils.foreach(cur, fetch_mode, true, fn)
end

function cursor_utils.fetch_all_destroy(cur, fetch_mode)
  local t = {cursor_utils.fetch_all(cur, fetch_mode)}
  if cur.destroy then cur:destroy() else cur:close() end
  return t[1], t[2]
end

function cursor_utils.fetch_all_close(cur, fetch_mode)
  local t = {cursor_utils.fetch_all(cur, fetch_mode)}
  cur:close()
  return t[1], t[2]
end

end

local param_utils = {} do

--
-- заключает строку в ковычки
--
function param_utils.quoted (s,q) return (q .. string.gsub(s, q, q..q) .. q) end

--
--
--
function param_utils.bool2sql(v) return v and 1 or 0 end

--
-- 
--
function param_utils.num2sql(v)  return tostring(v) end

--
-- 
--
function param_utils.str2sql(v, q) return param_utils.quoted(v, q or "'") end

--
-- возвращает индекс значени€ val в массиве t
--
function param_utils.ifind(val,t)
  for i,v in ipairs(t) do
    if v == val then
      return i
    end
  end
end

--
-- паттерн дл€ происка именованных параметров в запросе
--
param_utils.param_pattern = "[:]([^%d][%a%d_]+)"

--
-- ѕодставл€ет именованные параметры
--
-- @param sql      - текст запроса
-- @param params   - таблица значений параметров
-- @return         - новый текст запроса
--
function param_utils.apply_params(sql, params)
  params = params or {}
  -- if params[1] ~= nil then return nil, ERR_MSGS.pos_params_unsupport end

  local err
  local str = string.gsub(sql,param_utils.param_pattern,function(param)
    local v = params[param]
    local tv = type(v)
    if    ("number"      == tv)then return param_utils.num2sql (v)
    elseif("string"      == tv)then return param_utils.str2sql (v)
    elseif("boolean"     == tv)then return param_utils.bool2sql(v)
    elseif(PARAM_NULL    ==  v)then return 'NULL'
    elseif(PARAM_DEFAULT ==  v)then return 'DEFAULT'
    end
    err = ERR_MSGS.unknown_parameter .. param
  end)
  if err then return nil, err end
  return str
end

--
-- ODBC Specific 
--

--
-- ѕреобразует именованные параметры в ?
-- 
-- @param sql      - текст запроса
-- @param parnames - таблица разрешонных параметров
--                 - true - разрешены все имена
-- @return  новый текст запроса
-- @return  массив имен параметров. »ндекс - номер по пор€дку данного параметра
--
function param_utils.translate_params(sql,parnames)
  if parnames == nil then parnames = true end
  assert(type(parnames) == 'table' or (parnames == true))
  local param_list={}
  local err
  local function replace()
    local function t1(param)
      -- assert(type(parnames) == 'table')
      if not param_utils.ifind(param, parnames) then
        err = ERR_MSGS.unsolved_parameter .. param
        return
      end
      table.insert(param_list, param)
      return '?'
    end

    local function t2(param)
      -- assert(parnames == true)
      table.insert(param_list, param)
      return '?'
    end

    return (parnames == true) and t2 or t1
  end

  local str = string.gsub(sql,param_utils.param_pattern,replace())
  if err then return nil, err end
  return str, param_list;
end

function param_utils.bind_param(stmt, i, val, ...)
  if     val == PARAM_NULL    then return stmt:bindnull(i)
  elseif val == PARAM_DEFAULT then return stmt:binddefault(i)
  else return stmt:bind(i, val, ...) end
end

end

--
-- ќткрывает подключение к Ѕƒ
-- @par obj - LUASQL.Environment/LUASQL.Connection
-- @par dsn - название DSN
-- @par lgn - логин дл€ подключени€ к Ѕƒ
-- @par lgn - пароль дл€ подключени€ к Ѕƒ
-- @par autocommit - признак автоподтверждени€ транзакций (по умолчанию true)
--
-- ƒл€ ODBC возможно
-- @par dsn - таблица параметров дл€ формировани€ строки соединени€
-- @par lgn - autocommit
--
-- @return - luasql::connection 
-- @return - —трока подключени€ - только дл€ ODBC
-- 
local function do_connect(obj, dsn, lgn, pwd, autocommit)
  local cnndrv_params
  if type(dsn) == 'table' then
    if not obj.driverconnect then
      return nil, ERR_MSGS.not_support
    end
    cnndrv_params = dsn
    autocommit = lgn
  else
    if type(lgn) == 'boolean' then
      assert(pwd == nil)
      assert(autocommit == nil)
      autocommit = lgn
      lgn = nil
    elseif type(pwd) == 'boolean' then
      assert(autocommit == nil)
      autocommit = pwd
      pwd = nil
    end
  end

  if autocommit == nil then autocommit = true end

  local cnn, err
  if cnndrv_params then cnn, err = obj:driverconnect(cnndrv_params)
  else cnn, err  = obj:connect(dsn, lgn or "", pwd or "") end

  if not cnn then return nil, err end
  cnn:setautocommit(autocommit)

  return cnn, err
end

return {

  OPTIONS = OPTIONS;

  ERR_MSGS = ERR_MSGS;

  cursor = cursor_utils;

  param  = param_utils;

  connect = do_connect;

  driver = driver;
}
