luasql = assert(require "luasql.odbc")

CNN_DRV = {
  {Driver='{Adaptive Server Anywhere 9.0}'};
  {UID='TestUser'};
  {PWD='sql'};
  {EngineName='DevelopServer'};
  {DatabaseName='EmptyDB'};
  {CommLinks='tcpip{host=127.0.0.1}'};
}

CNN_DSN = {'emptydb', 'TestUser', 'sql'}

function is_dsn_exists(env, dsn_name)
  local cnt, d = return_count(env:getdatasources(function(dsn) 
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

