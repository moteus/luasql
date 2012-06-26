--- Модуль-расширение luasql.odbc
-- @class module
-- @name odbc.ex
--[=[-------------------------------------------------------------
@usage
local odbc = require "lausql.odbc.ex"
local db = assert(odbc.connect{dsn='demodb'})

sql_text = [[...]]
params = { ... }
db:rows(sql_text,params,function(row) ... end)


local qry = db:prepare([[insert ...]])
for ... do
  qry:exec{ some params }
end
qry:destroy()


db:destroy()
--]=]-------------------------------------------------------------


--- Таблица параметров библиотеки.
-- Каждый параметр может быть установлен на уровне Environment, Connection или Query
--
-- @class table
-- @name OPTIONS
-- @field FORCE_REPLACE_PARAMS всегда заменять параметры подстановкой строки. 
-- Этот параметр может использоватся например для выполнения batch запросов с параметрами.
-- @field IGNORE_NAMED_PARAMS  не пытатся преобразовывать именованные параметры
-- это необходимо для предотвращения изменения текста SQL перед выполнением
-- при этом параметры будут поддерживатся только если проддерживается bind(будут использоваться только '?')
-- @usage# local sql = [[begin if :ID > 5 then select 5 else selct 0 end if end]]
-- qry:set_config('FORCE_REPLACE_PARAMS', true)
-- qry:rows({ID=10}, print)
-- @usage# local sql = [[select 'hello :world']]
-- qry:set_config('IGNORE_NAMED_PARAMS', true)
-- qry:rows(print)

local utils       = require "luasql.odbc.ex.utils"
local Query       = require "luasql.odbc.ex.Query"
local Environment = require "luasql.odbc.ex.Environment"
local Connection  = require "luasql.odbc.ex.Connection"



--- callback функция для перебора запесей набора.
-- <br> Если функция возвращает любое значение, то перебор прекращается
-- и возвращенные(все) значения становятся результатом функции, которая осуществляет перебор.
-- <br> В функцию может передаватся одна и таже таблица с разными значениями.
-- @class function
-- @name callback_function
-- @param row - очередная запись
-- @see Environment:drivers
-- @see Connection:tables
-- @see Connection:rows
-- @see Query:rows

--- Структура, описывающая отдельный драйвер.
-- @class table
-- @name driverinfo
-- @field 1 название драйвера
-- @field 2 набор параметров 
-- @see Environment:drivers

--- Структура, описывающая отдельный драйвер
-- @class table
-- @name dsninfo
-- @field 1 название DSN
-- @field 2 название драйвера
-- @see Environment:datasources

--- Уровни изоляции транзакций
-- @class table
-- @name transaction_level
-- @field 1 "TRANSACTION_NONE"
-- @field 2 "TRANSACTION_READ_UNCOMMITTED"
-- @field 3 "TRANSACTION_READ_COMMITTED"
-- @field 4 "TRANSACTION_REPEATABLE_READ"
-- @field 5 "TRANSACTION_SERIALIZABLE"

if lunatest then
function test_odbc_ex_internal()

end
end

local luasql = require "luasql.odbc"
luasql.odbc = setmetatable({
  PARAM_NULL    = PARAM_NULL    ;
  NULL          = PARAM_NULL    ;
  PARAM_DEFAULT = PARAM_DEFAULT ;
  DEFAULT       = PARAM_DEFAULT ;

  connection    = function() return Connection:new()  end;
  environment   = function() return Environment:new() end;

  connect = function (...)
    local cnn = Connection:new(...)
    local ok, err = cnn:open()
    if not ok then
      cnn:destroy()
      return nil, err
    end
    return cnn
  end
},{__call=luasql.odbc})

return luasql.odbc
