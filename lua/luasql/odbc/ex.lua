--- ������-���������� luasql.odbc
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


--- ������� ���������� ����������.
-- ������ �������� ����� ���� ���������� �� ������ Environment, Connection ��� Query
--
-- @class table
-- @name OPTIONS
-- @field FORCE_REPLACE_PARAMS ������ �������� ��������� ������������ ������. 
-- ���� �������� ����� ������������� �������� ��� ���������� batch �������� � �����������.
-- @field IGNORE_NAMED_PARAMS  �� ������� ��������������� ����������� ���������
-- ��� ���������� ��� �������������� ��������� ������ SQL ����� �����������
-- ��� ���� ��������� ����� ������������� ������ ���� ��������������� bind(����� �������������� ������ '?')
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



--- callback ������� ��� �������� ������� ������.
-- <br> ���� ������� ���������� ����� ��������, �� ������� ������������
-- � ������������(���) �������� ���������� ����������� �������, ������� ������������ �������.
-- <br> � ������� ����� ����������� ���� � ���� ������� � ������� ����������.
-- @class function
-- @name callback_function
-- @param row - ��������� ������
-- @see Environment:drivers
-- @see Connection:tables
-- @see Connection:rows
-- @see Query:rows

--- ���������, ����������� ��������� �������.
-- @class table
-- @name driverinfo
-- @field 1 �������� ��������
-- @field 2 ����� ���������� 
-- @see Environment:drivers

--- ���������, ����������� ��������� �������
-- @class table
-- @name dsninfo
-- @field 1 �������� DSN
-- @field 2 �������� ��������
-- @see Environment:datasources

--- ������ �������� ����������
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
