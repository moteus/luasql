#ifndef _LS_ODBC_CONFIG_H_
#define _LS_ODBC_CONFIG_H_

#if defined(_WIN32)
#include <windows.h>
#include <sqlext.h>
#elif defined(INFORMIX)
#include "infxcli.h"
#elif defined(UNIXODBC)
#include "sql.h"
#include "sqltypes.h"
#include "sqlext.h"
#endif

#include "lua.h"
#include "lauxlib.h"
#include "luasql.h"

#if defined LUA_NUMBER_DOUBLE
#define LUASQL_C_NUMBER SQL_C_DOUBLE
#define LUASQL_NUMBER SQL_DOUBLE
#define LUASQL_NUMBER_SIZE 0
#define LUASQL_NUMBER_DIGEST 0
#elif defined LUA_NUMBER_INEGER
#define LUASQL_C_NUMBER SQL_C_INT
#define LUASQL_NUMBER SQL_INTEGER
#define LUASQL_NUMBER_SIZE 0
#define LUASQL_NUMBER_DIGEST 0
#else
#error need to specify LUASQL_C_NUMBER
#endif

#define LUASQL_MIN_PAR_BUFSIZE 64
#define LUASQL_USE_DRIVERINFO
// #define LUASQL_USE_DRIVERINFO_SUPPORTED_FUNCTIONS

#define LUASQL_ODBCVER ODBCVER

//*******************************************************************

#if LUASQL_ODBCVER >= 0x0300
#   define LUASQL_ODBC3_C(odbc3_value,old_value) odbc3_value
#else
#   define LUASQL_ODBC3_C(odbc3_value,old_value) old_value
#endif

#define LUASQL_ALLOCATE_ERROR(L) luaL_error((L), LUASQL_PREFIX"memory allocation error.")

#define STATIC_ASSERT(x) {static char arr[(x)?1:0];}

#ifndef LUASQL_USE_DRIVERINFO 
#  undef LUASQL_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
#endif

#define TYPE_BIGINT        1
#define TYPE_BINARY        2
#define TYPE_BIT           3
#define TYPE_CHAR          4
#define TYPE_DATE          5
#define TYPE_DECIMAL       6
#define TYPE_DOUBLE        7
#define TYPE_FLOAT         8
#define TYPE_INTEGER       9
#define TYPE_LONGVARBINARY 10
#define TYPE_LONGVARCHAR   11
#define TYPE_NUMERIC       12
#define TYPE_REAL          13
#define TYPE_SMALLINT      14
#define TYPE_TIME          15
#define TYPE_TIMESTAMP     16
#define TYPE_TINYINT       17
#define TYPE_VARBINARY     18
#define TYPE_VARCHAR       19
#if (LUASQL_ODBCVER >= 0x0300)
#define TYPE_WCHAR         20
#define TYPE_WLONGVARCHAR  21
#define TYPE_WVARCHAR      22
#define TYPE_GUID          23
#define TYPES_COUNT        23
#else
#define TYPES_COUNT        19
#endif

#endif