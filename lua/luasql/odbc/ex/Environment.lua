--- Implement environment class
-- @class module
-- @name Environment

local utils = require "luasql.odbc.ex.utils"
local Connection = require "luasql.odbc.ex.Connection"

local ERR_MSGS = assert(utils.ERR_MSGS)
local OPTIONS  = assert(utils.OPTIONS)
local DRIVER   = assert(utils.driver)

local Environment = {}
local Environment_private = {}

------------------------------------------------------------------
do -- Environment

--- Создает новый объект environment.
-- 
-- @return объект Environment
function Environment:new()
  local env, err = DRIVER()
  if not env then return nil, err end
  if env.setv3 then env:setv3() end

  local t = {
    private_ = {
      env = env
    }
  };

  return setmetatable(t,{__index = self})
end

--- Возвращает luasql.odbc.environment
-- 
function Environment:handle()
  return self.private_.env
end

--- Уничтожает объект environment.
-- 
-- <br> Если уничтожаемый объект использовался для распределения 
-- подключений и они не уничтожены, то вызывается исключение
function Environment:destroy()
  assert(self.private_.env)
  self.private_.env:close()
  self.private_.env = nil
end

--- Возвращает список установленных в системе драйверов ODBC
-- 
-- @param fn [optional] callback 
-- @return список драйверов 
-- @see callback_function
-- @see driverinfo 
function Environment:drivers(fn)
  assert(self.private_.env)
  if not self.private_.env.getdrivers then return nil, ERR_MSGS.not_support end
  if fn then return self.private_.env:getdrivers(fn) end
  return self.private_.env:getdrivers()
end

--- Возвращает массив DSN 
-- 
-- @param fn [optional] callback 
-- @return - массив таблиц (dsninfo)
-- @see callback_function
-- @see dsninfo 
function Environment:datasources(fn)
  assert(self.private_.env)
  if not self.private_.env.getdatasources then return nil, ERR_MSGS.not_support end
  if fn then return self.private_.env:getdatasources(fn) end
  return self.private_.env:getdatasources()
end

--- Устанавливает таймоут для подключения к БД
-- 
-- @param ms [optional] - интервал в миллисекундах. nil не устанавливать.
-- @return - true
function Environment:set_login_timeout(ms)
  assert(self.private_.env)
  assert((type(ms) == 'number') or (ms == nil))
  if not self.private_.env.setlogintimeout then return nil, ERR_MSGS.not_support end
  if ms == nil then ms = -1 end
  self.private_.env:setlogintimeout(ms)
  return true
end

--- Возвращает таймоут для подключения к БД
-- 
-- @return - интервал в миллисекундах. nil не устанавленно
function Environment:get_login_timeout()
  assert(self.private_.env)
  assert((type(ms) == 'number') or (ms == nil))
  if not self.private_.env.getlogintimeout then return nil, ERR_MSGS.not_support end
  local ms = self.private_.env:getlogintimeout()
  if ms == -1 then ms = nil end
  return ms
end

--- Создает новый объект Connection.
--
-- @class function
-- @name Environment:connection
-- @param dsn [optional]  название DSN
-- @param login [optional] 
-- @param password [optional] 
-- @param autocommit [optional] 
-- @return объект Connection
-- @usage local db = env:connection('demo','DBA','sql',false)
-- @usage local db = env:connection('demo',false) --Логин и пароль опущены, но autocommit установлен

--- Создает новый объект Connection.
-- 
-- @class function
-- @name Environment:connection
-- @param params таблица для формирования строки подключения
-- @param autocommit
-- @return объект Connection 
-- @return строка подключения
-- @usage local db = env:connection{DSN='demo',UID='DBA',PWD='sql'}


--
function Environment:connection(...)
  return Connection:private_new(self, ...)
end

--- Создает новый объект Connection и открывает подключение.
-- <br>Эквивалентно cnn = env:connection(...) cnn:open().
-- <br>Если не удалось подключится к БД, то объект Connection уничтожается.
-- @class function
-- @name Environment:connect
-- @see Environment:connection
-- @see Environment:connection
-- @see Connection:open


function Environment:connect(...)
  local cnn, err = self:connection(...);
  if not cnn then return nil, err end
  local ok, err = cnn:open()
  if not ok then 
    cnn:destroy()
    return nil, err
  end
  return cnn
end

function Environment:get_config(name )     return Environment_private.get_config_param(self, name)      end

function Environment:set_config(name, val) return Environment_private.set_config_param(self, name, val) end

end
------------------------------------------------------------------

------------------------------------------------------------------
do -- Environment private

function Environment_private:get_config_param(name)
  if self.private_.lib_opt then
    local val = self.private_.lib_opt[name]
    if val ~= nil then return val end
  end
  local val = OPTIONS[name]
  assert(val ~= nil)
  return val
end

function Environment_private:set_config_param(name, value)
  if not self.private_.lib_opt then
    self.private_.lib_opt = {}
  end
  self.private_.lib_opt[name] = value

  return Environment_private.get_config_param(self, name)
end

--
-- Создает и открывает подключение к БД
function Environment_private:connect(...)
  assert(self.private_.env)
  return do_connect(self.private_.env, ...)
end

end
------------------------------------------------------------------

return Environment