/*
** LuaSQL, ODBC driver
** Authors: Pedro Rabinovitch, Roberto Ierusalimschy, Diego Nehab,
** Tomas Guisasola
** See Copyright Notice in license.html
** $Id: ls_odbc.c,v 1.39 2009/02/07 23:16:23 tomas Exp $
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "ls_odbc_config.h"

#define LUASQL_ENVIRONMENT_ODBC "ODBC environment"
#define LUASQL_CONNECTION_ODBC "ODBC connection"
#define LUASQL_CURSOR_ODBC "ODBC cursor"
#define LUASQL_STMT_ODBC "ODBC statement"

static void PREDEFINE_TESTS(void){
  STATIC_ASSERT(sizeof(SQLUINTEGER) <= sizeof(lua_Number));
  STATIC_ASSERT(sizeof(SQLUINTEGER) <= sizeof(lua_Integer));
}

static const char 
    *LT_STRING  = "string",
    *LT_NUMBER  = "number",
    *LT_BOOLEAN = "boolean",
    *LT_BINARY  = "binary";

typedef struct {
    short      closed;
    int        conn_counter;
    int        login_timeout;
    SQLHENV    henv;               /* environment handle */
} env_data;

#define supportedFunctionsSize (sizeof(SQLUSMALLINT) * LUASQL_ODBC3_C(SQL_API_ODBC3_ALL_FUNCTIONS_SIZE,100))
typedef struct {
    // odbc version
    unsigned char majorVersion_;
    unsigned char minorVersion_;

    SQLUINTEGER getdataExt_;
    SQLUINTEGER cursorMask_;

#if LUASQL_ODBCVER >= 0x0300
    SQLUINTEGER forwardOnlyA2_;
    SQLUINTEGER staticA2_;
    SQLUINTEGER keysetA2_;
    SQLUINTEGER dynamicA2_;
#endif

    SQLUINTEGER  concurMask_;

#ifdef LUASQL_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
    SQLUSMALLINT supportedFunctions_[supportedFunctionsSize];
#endif

} drvinfo_data;

#define LUASQL_CONN_SUPPORT_TXN     0
#define LUASQL_CONN_SUPPORT_PREPARE 1
#define LUASQL_CONN_SUPPORT_BINDPARAM 2
#define LUASQL_CONN_SUPPORT_NUMPARAMS 3
#define LUASQL_CONN_SUPPORT_MAX     4

typedef struct {
    short      closed;
    int        cur_counter;
    int        env;                /* reference to environment */
    SQLHDBC    hdbc;               /* database connection handle */

#ifdef LUASQL_USE_DRIVERINFO
    drvinfo_data *di;
#endif

    signed char supports[LUASQL_CONN_SUPPORT_MAX];
} conn_data;

typedef struct {
    short      closed;
    int        conn;               /* reference to connection */
    int        numcols;            /* number of columns */
    int        coltypes, colnames; /* reference to column information tables */
    SQLHSTMT   hstmt;              /* statement handle */
    int        autoclose;          /* true = fetch last row close cursor (luasql compat)*/
} cur_data;

typedef struct par_data_tag{
    union{
        struct{
            SQLPOINTER           buf;
            SQLULEN              bufsize;
        }          strval;
        lua_Number numval;
        char       boolval;
    }                    value;
    SQLSMALLINT          sqltype;
    SQLULEN              parsize;
    SQLSMALLINT          digest;
    SQLLEN               ind;
    int                  get_cb;   /* reference to callback */
    struct par_data_tag* next;
} par_data;

typedef struct {
    cur_data      cur;
    int           numpars;            /* number of params */
    unsigned char destroyed;
    par_data*     par;
    unsigned char prepared;
    unsigned char resultsetno;       /* current number of rs */
} stmt_data;

/* if prepared and (numpars >= 0) then 
      ODBC driver support SQLNumParams and we check index on bind
  if numpars < 0 then 
        ODBC driver do not support SQLNumParams or stmt not prepared
        so we can not check index on bind
*/

//{ libodbc  enum TransactionIsolation 
/** The data source does not support transactions */
#define TRANSACTION_NONE ((SQLUINTEGER)1)
/** Dirty reads, non-repeatable reads and phantom reads can occur. */
#define TRANSACTION_READ_UNCOMMITTED ((SQLUINTEGER)2)
/** Non-repeatable and phantom reads can occur */
#define TRANSACTION_READ_COMMITTED ((SQLUINTEGER)3)
/** Phantom reads can occur */
#define TRANSACTION_REPEATABLE_READ ((SQLUINTEGER)4)
/** Simply no problems */
#define TRANSACTION_SERIALIZABLE ((SQLUINTEGER)5)
//}

//{ libodbc enum ResultSet type constants
/** The result set only goes forward. */
#define RS_TYPE_FORWARD_ONLY ((SQLUINTEGER)1)
/** The result set is scrollable, but the data in it is not
* affected by changes in the database.
*/
#define RS_TYPE_SCROLL_INSENSITIVE ((SQLUINTEGER)2)
/** The result set is scrollable and sensitive to database changes */
#define RS_TYPE_SCROLL_SENSITIVE ((SQLUINTEGER)3)
//}

#define IS_VALID_RS_TYPE(RS) (((RS) == RS_TYPE_FORWARD_ONLY) || \
                             ((RS) == RS_TYPE_SCROLL_INSENSITIVE) || \
                             ((RS) == RS_TYPE_SCROLL_SENSITIVE))

//{ enum ResultSet concurrency constants.
/** The ResultSet is read only */
#define RS_CONCUR_READ_ONLY  1
/** The ResultSet is updatable */
#define RS_CONCUR_UPDATABLE  2
//}

//{ enum
#define UPDATES 1
#define INSERTS 2
#define DELETES 3
//}

/* we are lazy */
#define hENV SQL_HANDLE_ENV
#define hSTMT SQL_HANDLE_STMT
#define hDBC SQL_HANDLE_DBC
#define error(a) ((a) != SQL_SUCCESS && (a) != SQL_SUCCESS_WITH_INFO) 


LUASQL_API int luaopen_luasql_odbc (lua_State *L);

//-----------------------------------------------------------------------------
// Lua casts
//{----------------------------------------------------------------------------

/*
** Check for valid environment.
*/
static env_data *getenvironment (lua_State *L) {
    env_data *env = (env_data *)luaL_checkudata (L, 1, LUASQL_ENVIRONMENT_ODBC);
    luaL_argcheck (L, env != NULL, 1, LUASQL_PREFIX"environment expected");
    luaL_argcheck (L, !env->closed, 1, LUASQL_PREFIX"environment is closed");
    return env;
}

/*
** Check for valid connection.
*/
static conn_data *getconnection_at (lua_State *L, int i) {
    conn_data *conn = (conn_data *)luaL_checkudata (L, i, LUASQL_CONNECTION_ODBC);
    luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
    luaL_argcheck (L, !conn->closed, 1, LUASQL_PREFIX"connection is closed");
    return conn;
}
#define getconnection(L) getconnection_at((L),1)

/*
** Check for valid cursor.
*/
static cur_data *getcursor (lua_State *L) {
    cur_data *cursor = (cur_data *)luaL_checkudata (L, 1, LUASQL_CURSOR_ODBC);
    luaL_argcheck (L, cursor != NULL, 1, LUASQL_PREFIX"cursor expected");
    luaL_argcheck (L, !cursor->closed, 1, LUASQL_PREFIX"cursor is closed");
    return cursor;
}

/*
** Check for valid stmt.
*/
static stmt_data *getstmt (lua_State *L) {
    stmt_data *stmt = (stmt_data *)luaL_checkudata (L, 1, LUASQL_STMT_ODBC);
    luaL_argcheck (L, stmt != NULL, 1, LUASQL_PREFIX"statement expected");
    luaL_argcheck (L, !stmt->destroyed, 1, LUASQL_PREFIX"statement is destroyed");
    return stmt;
}

//}----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Helpers
//{----------------------------------------------------------------------------

static void luasql_stackdump( lua_State* L ) {

    int top= lua_gettop(L);
    int i;

    fprintf( stderr, "\n\tDEBUG STACK:\n" );

    if (top==0)
        fprintf( stderr, "\t(none)\n" );

    for( i=1; i<=top; i++ ) {
        int type= lua_type( L, i );

        fprintf( stderr, "\t[%d]= (%s) ", i, lua_typename(L,type) );

        // Print item contents here...
        //
        // Note: this requires 'tostring()' to be defined. If it is NOT,
        //       enable it for more debugging.
        //

        lua_getglobal( L, "tostring" );
        //
        // [-1]: tostring function, or nil
        if (!lua_isfunction(L,-1)) {
             fprintf( stderr, "('tostring' not available)" );
        } else {
            lua_pushvalue( L, i );
            lua_call( L, 1 /*args*/, 1 /*retvals*/ );

            // Don't trust the string contents
            //                
            fprintf( stderr, "%s", lua_tostring(L,-1) );
        }
        lua_pop(L,1);
        fprintf( stderr, "\n" );
    }
    fprintf( stderr, "\n" );
} 

/*
** Pushes true and returns 1
*/
static int pass(lua_State *L) {
    lua_pushboolean (L, 1);
    return 1;
}


static int push_diagnostics(lua_State *L,  const SQLSMALLINT type, const SQLHANDLE handle) {
    SQLCHAR State[6];
    SQLINTEGER NativeError;
    SQLSMALLINT MsgSize, i;
    SQLRETURN ret;
    char Msg[SQL_MAX_MESSAGE_LENGTH];
    luaL_Buffer b;

    luaL_buffinit(L, &b);
    i = 1;
    while (1) {
        ret = SQLGetDiagRec(type, handle, i, State, &NativeError, Msg, sizeof(Msg), &MsgSize);
        if (ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) break;
        if(i > 1) luaL_addchar(&b, '\n');
        luaL_addlstring(&b, Msg, MsgSize);
        luaL_addchar(&b, '\n');
        luaL_addlstring(&b, State, 5);
        i++;
    } 
    luaL_pushresult(&b);
    return 1;
}

/*
** Fails with error message from ODBC
** Inputs: 
**   type: type of handle used in operation
**   handle: handle used in operation
*/
static int fail(lua_State *L,  const SQLSMALLINT type, const SQLHANDLE handle) {
  lua_pushnil(L);
  push_diagnostics(L, type, handle);
  return 2;
}

static int is_fail(lua_State *L, int nresult){
  if(nresult == 0)return 0;
  return (lua_type(L, nresult) == LUA_TNIL);
}

// no result or error
static int is_unknown(lua_State *L, int nresult){
  if(nresult == 0)return 1;
  return (lua_type(L, nresult) == LUA_TNIL);
}

/*
** Check if is SQL type.
*/
static int issqltype (const SQLSMALLINT type) {
    switch (type) {
        case SQL_UNKNOWN_TYPE: case SQL_CHAR: case SQL_VARCHAR: 
        case SQL_TYPE_DATE: case SQL_TYPE_TIME: case SQL_TYPE_TIMESTAMP: 
        case SQL_DATE: case SQL_INTERVAL: case SQL_TIMESTAMP: 
        case SQL_LONGVARCHAR:
        case SQL_GUID:

        case SQL_NUMERIC: case SQL_DECIMAL: 
        case SQL_FLOAT: case SQL_REAL: case SQL_DOUBLE:
        case SQL_BIGINT: case SQL_TINYINT: case SQL_INTEGER: case SQL_SMALLINT:


        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:

        case SQL_INTERVAL_MONTH:
        case SQL_INTERVAL_YEAR:
        case SQL_INTERVAL_YEAR_TO_MONTH:
        case SQL_INTERVAL_DAY:
        case SQL_INTERVAL_HOUR:
        case SQL_INTERVAL_MINUTE:
        case SQL_INTERVAL_SECOND:
        case SQL_INTERVAL_DAY_TO_HOUR:
        case SQL_INTERVAL_DAY_TO_MINUTE:
        case SQL_INTERVAL_DAY_TO_SECOND:
        case SQL_INTERVAL_HOUR_TO_MINUTE:
        case SQL_INTERVAL_HOUR_TO_SECOND:
        case SQL_INTERVAL_MINUTE_TO_SECOND:
       
        case SQL_BINARY: case SQL_VARBINARY: case SQL_LONGVARBINARY:
        case SQL_BIT:
            return 1;
        default:
            return 0;
    }
}


/*
** Returns the name of an equivalent lua type for a SQL type.
*/
static const char *sqltypetolua (const SQLSMALLINT type) {
    switch (type) {
        case SQL_UNKNOWN_TYPE: case SQL_CHAR: case SQL_VARCHAR: 
        case SQL_TYPE_DATE: case SQL_TYPE_TIME: case SQL_TYPE_TIMESTAMP: 
        case SQL_DATE: case SQL_INTERVAL: case SQL_TIMESTAMP: 
        case SQL_LONGVARCHAR:
        case SQL_GUID:
            return LT_STRING;

        case SQL_NUMERIC: case SQL_DECIMAL: 
        case SQL_FLOAT: case SQL_REAL: case SQL_DOUBLE:
        case SQL_BIGINT: case SQL_TINYINT: case SQL_INTEGER: case SQL_SMALLINT:
            return LT_NUMBER;

        case SQL_INTERVAL_MONTH:          case SQL_INTERVAL_YEAR: 
        case SQL_INTERVAL_YEAR_TO_MONTH:  case SQL_INTERVAL_DAY:
        case SQL_INTERVAL_HOUR:           case SQL_INTERVAL_MINUTE:
        case SQL_INTERVAL_SECOND:         case SQL_INTERVAL_DAY_TO_HOUR:
        case SQL_INTERVAL_DAY_TO_MINUTE:  case SQL_INTERVAL_DAY_TO_SECOND:
        case SQL_INTERVAL_HOUR_TO_MINUTE: case SQL_INTERVAL_HOUR_TO_SECOND:
        case SQL_INTERVAL_MINUTE_TO_SECOND:  
            return LT_NUMBER; // ???

        case SQL_WCHAR: case SQL_WVARCHAR:case SQL_WLONGVARCHAR:
        case SQL_BINARY: case SQL_VARBINARY: case SQL_LONGVARBINARY:
            return LT_BINARY;	/* !!!!!! nao seria string? */

        case SQL_BIT:
            return LT_BOOLEAN;
        default:
            assert(0);
            return NULL;

    }
}


static int push_column_value(lua_State *L, SQLHSTMT hstmt, SQLUSMALLINT i, const char type){
    int top = lua_gettop(L);

    switch (type) {/* deal with data according to type */
        case 'u': { /* nUmber */
            lua_Number num;
            SQLINTEGER got;
            SQLRETURN rc = SQLGetData(hstmt, i, LUASQL_C_NUMBER, &num, 0, &got);
            if (error(rc)) return fail(L, hSTMT, hstmt);
            if (got == SQL_NULL_DATA) lua_pushnil(L);
            else lua_pushnumber(L, num);
            break;
        }
        case 'o': { /* bOol */
            unsigned char b;
            SQLINTEGER got;
            SQLRETURN rc = SQLGetData(hstmt, i, SQL_C_BIT, &b, 0, &got);
            if (error(rc)) return fail(L, hSTMT, hstmt);
            if (got == SQL_NULL_DATA) lua_pushnil(L);
            else lua_pushboolean(L, b);
            break;
        }
        case 't': case 'i': {/* sTring, bInary */
            SQLSMALLINT stype = (type == 't') ? SQL_C_CHAR : SQL_C_BINARY;
            SQLINTEGER got;
            char *buffer;
            luaL_Buffer b;
            SQLRETURN rc;
            luaL_buffinit(L, &b);
            buffer = luaL_prepbuffer(&b);
            rc = SQLGetData(hstmt, i, stype, buffer, LUAL_BUFFERSIZE, &got);
            if (got == SQL_NULL_DATA){
                lua_pushnil(L);
                break;
            }
            while (rc == SQL_SUCCESS_WITH_INFO) {/* concat intermediary chunks */
                if (got >= LUAL_BUFFERSIZE || got == SQL_NO_TOTAL) {
                    got = LUAL_BUFFERSIZE;
                    /* get rid of null termination in string block */
                    if (stype == SQL_C_CHAR) got--;
                }
                luaL_addsize(&b, got);
                buffer = luaL_prepbuffer(&b);
                rc = SQLGetData(hstmt, i, stype, buffer, LUAL_BUFFERSIZE, &got);
            }
            if (rc == SQL_SUCCESS) {/* concat last chunk */
                if (got >= LUAL_BUFFERSIZE || got == SQL_NO_TOTAL) {
                    got = LUAL_BUFFERSIZE;
                    /* get rid of null termination in string block */
                    if (stype == SQL_C_CHAR) got--;
                }
                luaL_addsize(&b, got);
            }
            if error(rc) return fail(L, hSTMT, hstmt);
            /* return everything we got */
            luaL_pushresult(&b);
            break;
        }
        default:{
          // unsupported type ?
          assert(0);
        }
    }

    assert(1 == (lua_gettop(L)-top));
    return 0;
}

/*
** Retrieves data from the i_th column in the current row
** Input
**   types: index in sack of column types table
**   hstmt: statement handle
**   i: column number
** Returns:
**   0 if successfull, non-zero otherwise;
*/
static int push_column(lua_State *L, int coltypes, const SQLHSTMT hstmt, 
        SQLUSMALLINT i) {
    const char *tname;
    char type;
    lua_rawgeti (L, coltypes, i);   /* typename of the column */
    tname = lua_tostring(L, -1);
    if (!tname) return luasql_faildirect(L, "invalid type in table.");
    type = tname[1];
    lua_pop(L, 1); /* pops type name */
    return push_column_value(L,hstmt,i,type);
}


/*
** Creates two tables with the names and the types of the columns.
*/
static void create_colinfo (lua_State *L, cur_data *cur) {
    SQLCHAR buffer[256];
    SQLSMALLINT namelen, datatype, i;
    SQLRETURN ret;
    int types, names;

    lua_newtable(L);
    types = lua_gettop (L);
    lua_newtable(L);
    names = lua_gettop (L);
    for (i = 1; i <= cur->numcols; i++) {
        ret = SQLDescribeCol(cur->hstmt, i, buffer, sizeof(buffer), 
                &namelen, &datatype, NULL, NULL, NULL);
        // if (error(ret)) return fail(L, hSTMT, cur->hstmt);
        lua_pushstring (L, buffer);
        lua_rawseti (L, names, i);
        lua_pushstring(L, sqltypetolua(datatype));
        lua_rawseti (L, types, i);
    }
    cur->colnames = luaL_ref (L, LUA_REGISTRYINDEX);
    cur->coltypes = luaL_ref (L, LUA_REGISTRYINDEX);
}

typedef SQLRETURN (SQL_API*get_attr_ptr)(SQLHANDLE, SQLUSMALLINT, SQLPOINTER);
typedef SQLRETURN (SQL_API*set_attr_ptr)(SQLHANDLE, SQLUSMALLINT, SQLULEN);

static get_attr_ptr select_get_attr(SQLSMALLINT HandleType){
    switch(HandleType){
        case hDBC:  return SQLGetConnectOption;
        case hSTMT: return SQLGetStmtOption;
    }
    assert(0);
    return NULL;
}

static set_attr_ptr select_set_attr(SQLSMALLINT HandleType){
    switch(HandleType){
        case hDBC:  return SQLSetConnectOption;
        case hSTMT: return SQLSetStmtOption;
    }
    assert(0);
    return NULL;
}

typedef SQLRETURN (SQL_API*get_attr_ptr_v3)(SQLHANDLE, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER*);
typedef SQLRETURN (SQL_API*set_attr_ptr_v3)(SQLHANDLE, SQLINTEGER, SQLPOINTER, SQLINTEGER);

static get_attr_ptr_v3 select_get_attr_v3(SQLSMALLINT HandleType){
    switch(HandleType){
        case hENV:  return SQLGetEnvAttr;
        case hDBC:  return SQLGetConnectAttr;
        case hSTMT: return SQLGetStmtAttr;
    }
    assert(0);
    return NULL;
}

static set_attr_ptr_v3 select_set_attr_v3(SQLSMALLINT HandleType){
    switch(HandleType){
        case hENV:  return SQLSetEnvAttr;
        case hDBC:  return SQLSetConnectAttr;
        case hSTMT: return SQLSetStmtAttr;
    }
    assert(0);
    return NULL;
}


static int get_uint_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, SQLINTEGER optnum){
    SQLUINTEGER res;
    SQLRETURN ret;
#if (LUASQL_ODBCVER >= 0x0300)
    SQLINTEGER dummy;
    ret = select_get_attr_v3(HandleType)(Handle, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
#else 
    if(HandleType == hENV) return luasql_faildirect(L, "not supported."); 
    ret = select_get_attr   (HandleType)(Handle, optnum, (SQLPOINTER)&res);
#endif

    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, HandleType, Handle);
    lua_pushnumber(L,res);
    return 1;
}

static int get_str_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, SQLINTEGER optnum){
#if (LUASQL_ODBCVER >= 0x0300)
    SQLINTEGER got;
    char buffer[256];
#else 
    char buffer[SQL_MAX_OPTION_STRING_LENGTH+1];
#endif
    SQLRETURN ret;

#if (ODBCVER >= 0x0300)
    ret = select_get_attr_v3(HandleType)(Handle, optnum, (SQLPOINTER)buffer, 255, &got);
#else 
    if(HandleType == hENV) return luasql_faildirect(L, "not supported."); 
    ret = select_get_attr   (HandleType)(Handle, optnum, (SQLPOINTER)&res);
#endif

    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, HandleType, Handle);

#if (LUASQL_ODBCVER >= 0x0300)
    if(got > 255){
        char* tmp = malloc(got+1);
        if(!tmp)
            return LUASQL_ALLOCATE_ERROR(L);
        ret = select_get_attr_v3(HandleType)(Handle, optnum, (SQLPOINTER)tmp, got, &got);
        if(error(ret)){
            free(tmp);
            if(ret == SQL_NO_DATA) return 0;
            return fail(L, HandleType, Handle);
        }
        lua_pushstring(L, tmp);
        free(tmp);
    }
    else 
#endif
    lua_pushstring(L, buffer);

    return 1;
}

static int set_uint_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, 
    SQLINTEGER optnum, SQLUINTEGER value)
{
    SQLRETURN ret; 
#if (LUASQL_ODBCVER >= 0x0300)
    ret = select_set_attr_v3(HandleType)(Handle,optnum,(SQLPOINTER)value,SQL_IS_UINTEGER);
#else 
    if(HandleType == hENV) return luasql_faildirect(L, "not supported."); 
    ret = select_set_attr   (HandleType)(Handle,optnum,value);
#endif

    if(error(ret)) return fail(L, HandleType, Handle);
    return pass(L);
}

static int set_str_attr_(lua_State*L, SQLSMALLINT HandleType, SQLHANDLE Handle, 
    SQLINTEGER optnum, const char* value, size_t len)
{
    SQLRETURN ret;
#if (LUASQL_ODBCVER >= 0x0300)
    ret = select_set_attr_v3(HandleType)(Handle,optnum,(SQLPOINTER)value,len);
#else 
    if(HandleType == hENV) return luasql_faildirect(L, "not supported."); 
    ret = select_set_attr   (HandleType)(Handle,optnum,(SQLUINTEGER)value);
#endif
    if(error(ret)) return fail(L, HandleType, Handle);
    return pass(L);
}
//}----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Driver info
//{----------------------------------------------------------------------------
// port DriverInfo from libodbc++ Manush Dodunekov <manush@stendahls.net>

#if LUASQL_ODBCVER >= 0x0300

#define DI_IMPLEMENT_SUPPORTS(SUFFIX,VALUE_3,VALUE_2) \
int di_supports_##SUFFIX(const drvinfo_data *di, int ct){   \
    int r = 0;                                        \
    assert(di != NULL);                               \
    if(di->majorVersion_>=3) {                        \
        switch(ct) {                                  \
        case SQL_CURSOR_FORWARD_ONLY:                 \
            r=(di->forwardOnlyA2_&VALUE_3)?1:0;       \
            break;                                    \
                                                      \
        case SQL_CURSOR_STATIC:                       \
            r=(di->staticA2_&VALUE_3)?1:0;            \
            break;                                    \
                                                      \
        case SQL_CURSOR_KEYSET_DRIVEN:                \
            r=(di->keysetA2_&VALUE_3)?1:0;            \
            break;                                    \
                                                      \
        case SQL_CURSOR_DYNAMIC:                      \
            r=(di->dynamicA2_&VALUE_3)?1:0;           \
            break;                                    \
                                                      \
        default:                                      \
            assert(0);                                \
            break;                                    \
        }                                             \
    } else {                                          \
        r=(di->concurMask_&VALUE_2)?1:0;              \
    }                                                 \
    return r;                                         \
}                                                   

#else

#define DI_IMPLEMENT_SUPPORTS(SUFFIX,VALUE_3,VALUE_2) \
int di_supports_##SUFFIX(const drvinfo_data *di, int ct){   \
    return (di->concurMask_&VALUE_2)?1:0;             \
}

#endif

DI_IMPLEMENT_SUPPORTS(readonly,SQL_CA2_READ_ONLY_CONCURRENCY,SQL_SCCO_READ_ONLY)
DI_IMPLEMENT_SUPPORTS(lock,SQL_CA2_LOCK_CONCURRENCY,SQL_SCCO_LOCK)
DI_IMPLEMENT_SUPPORTS(rowver,SQL_CA2_OPT_ROWVER_CONCURRENCY,SQL_SCCO_OPT_ROWVER)
DI_IMPLEMENT_SUPPORTS(values,SQL_CA2_OPT_VALUES_CONCURRENCY,SQL_SCCO_OPT_VALUES)

#undef DI_IMPLEMENT_SUPPORTS

int di_supports_forwardonly(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_FORWARD_ONLY)?1:0;
}

int di_supports_static(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_STATIC)?1:0;
}

int di_supports_keyset(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_KEYSET_DRIVEN)?1:0;
}

int di_supports_dynamic(const drvinfo_data *di){
    return (di->cursorMask_&SQL_SO_DYNAMIC)?1:0;
}

int di_supports_scrollsensitive(const drvinfo_data *di){
    return
        di_supports_dynamic(di) || 
        di_supports_keyset(di);
}

// assumes that di_supportsScrollSensitive(di)==true
int di_getscrollsensitive(const drvinfo_data *di){
    if(di_supports_dynamic(di)){
        return SQL_CURSOR_DYNAMIC;
    } else {
        return SQL_CURSOR_KEYSET_DRIVEN;
    }
}

int di_supports_updatable(const drvinfo_data *di, int ct){
    return
        di_supports_lock  (di, ct) ||
        di_supports_rowver(di, ct) ||
        di_supports_values(di, ct);
}

int di_getupdatable(const drvinfo_data *di, int ct){
    // assumes supportsUpdatable(ct) returns true
    if(di_supports_rowver(di, ct)) {
        return SQL_CONCUR_ROWVER;
    } else if(di_supports_values(di, ct)) {
        return SQL_CONCUR_VALUES;
    } else if(di_supports_lock(di, ct)) {
        return SQL_CONCUR_LOCK;
    }
    return SQL_CONCUR_READ_ONLY;
}

int di_supports_getdata_anyorder(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_ANY_ORDER)?1:0;
};

int di_supports_getdata_anycolumn(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_ANY_COLUMN)?1:0;
};

int di_supports_getdata_block(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_BLOCK)?1:0;
};

int di_supports_getdata_bound(const drvinfo_data *di){
    return (di->getdataExt_ & SQL_GD_BOUND)?1:0;
};

#ifdef LUASQL_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
int di_supports_function(const drvinfo_data *di, int funcId){
    return SQL_TRUE == LUASQL_ODBC3_C(
        SQL_FUNC_EXISTS(di->supportedFunctions_,funcId),
        di->supportedFunctions_[funcId]
    );
}
#endif

//}----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Cursor
//{----------------------------------------------------------------------------

static int cur_get_uint_attr_(lua_State*L, cur_data *cur, SQLINTEGER optnum){
    return get_uint_attr_(L, hSTMT, cur->hstmt, optnum);
}

static int cur_get_str_attr_(lua_State*L, cur_data *cur, SQLINTEGER optnum){
    return get_str_attr_(L, hSTMT, cur->hstmt, optnum);
}

static int cur_set_uint_attr_(lua_State*L, cur_data *cur, SQLINTEGER optnum, SQLINTEGER value){
    return set_uint_attr_(L, hSTMT, cur->hstmt, optnum, value);
}

static int cur_set_str_attr_(lua_State*L, cur_data *cur, SQLINTEGER optnum, 
    const char* value, size_t len)
{
    return set_str_attr_(L, hSTMT, cur->hstmt, optnum, value, len);
}

static int cur_get_uint_attr(lua_State*L){
  cur_data *cur = getcursor (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return cur_get_uint_attr_(L, cur, optnum);
}

static int cur_get_str_attr(lua_State*L){
  cur_data *cur = getcursor (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return cur_get_str_attr_(L, cur, optnum);
}

static int cur_set_uint_attr(lua_State*L){
  cur_data *cur = getcursor (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return cur_set_uint_attr_(L, cur, optnum,luaL_checkinteger(L,3));
}

static int cur_set_str_attr(lua_State*L){
  cur_data *cur = getcursor (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  size_t len;
  const char *str = luaL_checklstring(L,3,&len);
  return cur_set_str_attr_(L, cur, optnum, str, len);
}

/*
** Get another row of the given cursor.
*/
static int cur_close(lua_State *L);
static int cur_fetch_raw (lua_State *L, cur_data *cur) {
    SQLHSTMT hstmt = cur->hstmt;
    int ret, i, alpha_mode = -1,digit_mode = -1; 
    SQLRETURN rc = SQLFetch(cur->hstmt); 
    if (rc == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) {
        if(cur->autoclose){
            cur_close(L);
        }
        return 0;
    } else if (error(rc)) return fail(L, hSTMT, hstmt);

    if (lua_istable (L, 2)) {// stack: cur, row, fetch_mode?
        alpha_mode = 0;
        if(lua_isstring(L,3)){// fetch mode
            const char *opt = lua_tostring(L, 3);
            alpha_mode = strchr(opt,'a')?1:0;
            digit_mode = strchr(opt,'n')?1:0;
            lua_remove(L,3);
        }
        if (lua_gettop(L) > 2){
            luaL_error(L, LUASQL_PREFIX"too many arguments");
        }
    }
    else if (lua_gettop(L) > 1){ // stack: cur
        luaL_error(L, LUASQL_PREFIX"too many arguments");
    }

    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->coltypes); // stack: cur, row?, coltypes

    if(alpha_mode >= 0){ // stack: cur, row, coltypes
        if(alpha_mode){
            lua_rawgeti (L, LUA_REGISTRYINDEX, cur->colnames); // stack: cur, row, coltypes, colnames
            lua_insert  (L, -2);                               // stack: cur, row, colnames, coltypes
        }

        for (i = 1; i <= cur->numcols; i++) { // fill the row
            // stack: cur, row, colnames?, coltypes
            if(ret = push_column (L, -1, hstmt, i))
                return ret;
            if (alpha_mode) {// stack: cur, row, colnames, coltypes, value
                if (digit_mode){
                    lua_pushvalue(L,-1);    // stack: cur, row, colnames, coltypes, value, value
                    lua_rawseti (L, -5, i); // stack: cur, row, colnames, coltypes, value
                }
                lua_rawgeti(L, -3, i);  // stack: cur, row, colnames, coltypes, value, colname
                lua_insert(L, -2);      // stack: cur, row, colnames, coltypes, colname, value
                lua_rawset(L, -5);      /* table[name] = value */
                                        // stack: cur, row, colnames
            }
            else{            // stack: cur, row, coltypes, value
                lua_rawseti (L, -3, i);
            }
            // stack: cur, row, colnames?, coltypes
        }
        lua_pushvalue(L,2);
        return 1;
    }

    // stack: cur, coltypes
    luaL_checkstack (L, cur->numcols, LUASQL_PREFIX"too many columns");
    for (i = 1; i <= cur->numcols; i++) { 
        // stack: cur, coltypes, ...
        if(ret = push_column (L, 2, hstmt, i))
            return ret;
    }
    return cur->numcols;
}

static int cur_foreach_raw(lua_State *L, cur_data *cur, lua_CFunction close_fn){
#define FOREACH_RETURN(N) {\
  if(autoclose){\
  int top = lua_gettop(L);\
  close_fn(L);\
  lua_settop(L, top);\
  }\
  return (N);}

  unsigned char alpha = 0; 
  unsigned char autoclose = 1;
  int top;
  if(lua_isstring(L,2)){// fetch mode
    const char *opt = lua_tostring(L, 2);
    alpha = (opt[0] == 'a')?1:0;
    lua_remove(L,2);
  }
  if(lua_isboolean(L,2)){// autoclose
    autoclose = lua_toboolean(L,2)?1:0;
    lua_remove(L,2);
  }
  if(!lua_isfunction(L,2))return luaL_error(L, LUASQL_PREFIX"no function was provided");
  if(lua_gettop(L)>2)     return luaL_error(L, LUASQL_PREFIX"too many arguments");

  // stack: cur, func
  if(alpha){
    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->colnames); // stack: cur, func, colnames
    lua_insert(L,-2); // stack: cur, colnames, func
  }
  lua_newtable(L);                                   // stack: cur, colnames?, func, row
  lua_rawgeti (L, LUA_REGISTRYINDEX, cur->coltypes); // stack: cur, colnames?, func, row, coltypes
  lua_insert  (L, -3);                               // stack: cur, colnames?, coltypes, func, row

  top = lua_gettop(L);
  while(1){
    SQLHSTMT hstmt = cur->hstmt;
    SQLRETURN rc = SQLFetch(hstmt);
    int ret,i;

    assert(top == lua_gettop(L));

    if (rc == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)){
      FOREACH_RETURN(0);
    } else if (error(rc)){
      FOREACH_RETURN(fail(L, hSTMT, hstmt));
    }

    for (i = 1; i <= cur->numcols; i++) { // fill the row
      // stack: cur, colnames?, coltypes, func, row
      if(ret = push_column (L, -3, hstmt, i))
        return ret;
      if (alpha) {             // stack: cur, colnames, coltypes, func, row, value
        lua_rawgeti(L, -5, i); // stack: cur, colnames, coltypes, func, row, value, colname
        lua_insert(L, -2);     // stack: cur, colnames, coltypes, func, row, colname, value
        lua_rawset(L, -3);     /* table[name] = value */
      }
      else{                    // stack: cur, coltypes, func, row, value
        lua_rawseti (L, -2, i);
      }
      // stack: cur, colnames?, coltypes, func, row
    }

    assert(lua_isfunction(L,-2));      // stack: cur, colnames?, coltypes, func, row
    lua_pushvalue(L, -2);              // stack: cur, colnames?, coltypes, func, row, func
    lua_pushvalue(L, -2);              // stack: cur, colnames?, coltypes, func, row, func, row
    if(autoclose) ret = lua_pcall(L,1,LUA_MULTRET,0);
    else ret = 0,lua_call(L,1,LUA_MULTRET);

    // stack: cur, colnames?, coltypes, func, row, ...
    assert(lua_gettop(L) >= top);
    if(ret){ // error
      int top = lua_gettop(L);
      assert(autoclose);
      close_fn(L);
      lua_settop(L, top);
      return lua_error(L);
    }
    else if(lua_gettop(L) > top){
      FOREACH_RETURN(lua_gettop(L) - top);
    }
  }
  assert(0); // we never get here
  return 0;
#undef RETURN
}

static int cur_close(lua_State *L);
static int cur_foreach(lua_State *L){
  cur_data *cur = getcursor (L);
  return cur_foreach_raw(L, cur, cur_close);
}

static int cur_fetch (lua_State *L) {
    cur_data *cur = getcursor (L);
    return cur_fetch_raw(L,cur);
}

static int cur_moreresults(lua_State *L){
  cur_data *cur = getcursor (L);
  SQLHSTMT hstmt  = cur->hstmt;
  SQLRETURN ret;

  if((cur->numcols == 0) || (cur->closed != 0))
    return luaL_error (L, LUASQL_PREFIX"there are no open cursor");

  ret = SQLMoreResults(hstmt);
  if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
  if(error(ret)) return fail(L, hSTMT, hstmt);

  {
    SQLSMALLINT numcols;
    luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);
    cur->colnames  = LUA_NOREF;
    cur->coltypes  = LUA_NOREF;
    cur->numcols   = 0;
    ret = SQLNumResultCols (hstmt, &numcols);
    if (error(ret)) return fail(L, hSTMT, hstmt);
    cur->numcols = numcols;
    if(numcols) create_colinfo(L, cur);
  }
  return 1;
}

/*
** Closes a cursor.
*/
static int cur_close (lua_State *L) {
    conn_data *conn;
    cur_data *cur = (cur_data *) luaL_checkudata (L, 1, LUASQL_CURSOR_ODBC);
    SQLRETURN ret;
    int top;
    int ret_count = 0;
    luaL_argcheck (L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
    if (cur->closed) {
        lua_pushboolean(L, 0);
        return 1;
        //return pass(L);
    }
    if(cur->numcols == 0){
      return luasql_faildirect(L, "try close not cursor.");
    }
    
    // always pass 
    top = lua_gettop(L);
    ret_count = pass(L);

    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->conn);
    conn = lua_touserdata (L, -1);
    lua_pop(L,1);
    assert(top == (lua_gettop(L) - ret_count));

    ret = SQLCloseCursor(cur->hstmt);
    // SQLMoreResults can close cursor and here we get error
    if (error(ret)) ret_count+=push_diagnostics(L, hSTMT, cur->hstmt);
    assert(top == (lua_gettop(L) - ret_count));
    ret = SQLFreeHandle(hSTMT, cur->hstmt);
    // what we can do?
    if (error(ret)) ret_count+=push_diagnostics(L, hSTMT, cur->hstmt);
    assert(top == (lua_gettop(L) - ret_count));

    /* Decrement cursor counter on connection object */
    cur->hstmt  = 0;
    cur->closed = 1;
    conn->cur_counter--;
    luaL_unref (L, LUA_REGISTRYINDEX, cur->conn);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);
    assert(top == (lua_gettop(L) - ret_count));
    return ret_count;
}

/*
** Returns the table with column names.
*/
static int cur_colnames (lua_State *L) {
    cur_data *cur = (cur_data *) getcursor (L);
    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->colnames);
    return 1;
}

/*
** Returns the table with column types.
*/
static int cur_coltypes (lua_State *L) {
    cur_data *cur = (cur_data *) getcursor (L);
    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->coltypes);
    return 1;
}

/*
** is cursor opened
*/
static int cur_opened (lua_State *L) {
    cur_data *cur = (cur_data *) luaL_checkudata (L, 1, LUASQL_CURSOR_ODBC);
    lua_pushboolean(L, cur->closed?0:1);
    return 1;
}

static int cur_get_autoclose(lua_State *L) {
    cur_data *cur = getcursor(L);
    lua_pushboolean(L, cur->autoclose?1:0);
    return 1;
}

static int cur_set_autoclose(lua_State *L) {
    cur_data *cur = getcursor(L);
    cur->autoclose = lua_toboolean(L,2)?1:0;
    return 1;
}

/*
** Creates a cursor table and leave it on the top of the stack.
*/
static int cur_create (lua_State *L, int o, conn_data *conn, 
    const SQLHSTMT hstmt, const SQLSMALLINT numcols) {
    cur_data *cur = (cur_data *) lua_newuserdata(L, sizeof(cur_data));
    luasql_setmeta (L, LUASQL_CURSOR_ODBC);

    conn->cur_counter++;
    /* fill in structure */
    cur->closed = 0;
    cur->conn = LUA_NOREF;
    cur->numcols = numcols;
    cur->autoclose = 1;
    cur->colnames = LUA_NOREF;
    cur->coltypes = LUA_NOREF;
    cur->hstmt = hstmt;
    lua_pushvalue (L, o);
    cur->conn = luaL_ref (L, LUA_REGISTRYINDEX);

    /* make and store column information table */
    create_colinfo (L, cur);

    return 1;
}

//}----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Connect
//{----------------------------------------------------------------------------

//{ get / set attr

static int conn_get_uint_attr_(lua_State*L, conn_data *conn, SQLINTEGER optnum){
    return get_uint_attr_(L, hDBC, conn->hdbc, optnum);
}

static int conn_get_str_attr_(lua_State*L, conn_data *conn, SQLINTEGER optnum){
    return get_str_attr_(L, hDBC, conn->hdbc, optnum);
}

static int conn_set_uint_attr_(lua_State*L, conn_data *conn, SQLINTEGER optnum, SQLINTEGER value){
    return set_uint_attr_(L, hDBC, conn->hdbc, optnum, value);
}

static int conn_set_str_attr_(lua_State*L, conn_data *conn, SQLINTEGER optnum, 
    const char* value, size_t len)
{
    return set_str_attr_(L, hDBC, conn->hdbc, optnum, value, len);
}

//}

//{ get / set info

static int conn_get_uint32_info_(lua_State*L, conn_data *conn, SQLUSMALLINT optnum){
    SQLUINTEGER res;
    SQLUSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hDBC, conn->hdbc);
    lua_pushnumber(L,res);
    return 1;
}

static int conn_get_uint16_info_(lua_State*L, conn_data *conn, SQLUSMALLINT optnum){
    SQLUSMALLINT res;
    SQLUSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hDBC, conn->hdbc);
    lua_pushnumber(L,res);
    return 1;
}

static int conn_get_str_info_(lua_State*L, conn_data *conn, SQLUSMALLINT optnum){
    SQLUSMALLINT got;
    char buffer[256];
    SQLRETURN ret;

    ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)buffer, 255, &got);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hDBC, conn->hdbc);
    
    if(got > 255){
        char* tmp = malloc(got+1);
        if(!tmp)
            return LUASQL_ALLOCATE_ERROR(L);
        ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)tmp, got, &got);
        if(error(ret)){
            free(tmp);
            if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
            return fail(L, hDBC, conn->hdbc);
        }
        lua_pushstring(L, tmp);
        free(tmp);
    }
    else lua_pushstring(L, buffer);
    return 1;
}

static int conn_get_equal_char_info_(lua_State*L, conn_data *conn, SQLUSMALLINT optnum, 
    char value
){
    SQLUSMALLINT got;
    char buffer[2];
    SQLRETURN ret;

    ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)buffer, 2, &got);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hDBC, conn->hdbc);

    lua_pushboolean(L, (buffer[0] == value)?1:0);
    return 1;
}

static int conn_get_equal_uint32_info_(lua_State*L, conn_data *conn, SQLUSMALLINT optnum,
    SQLUINTEGER value
){
    SQLUINTEGER res;
    SQLUSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hDBC, conn->hdbc);

    lua_pushboolean(L, (res == value)?1:0);
    return 1;
}

static int conn_get_and_uint32_info_(lua_State*L, conn_data *conn, SQLUSMALLINT optnum,
    SQLUINTEGER value
){
    SQLUINTEGER res;
    SQLUSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hDBC, conn->hdbc);

    lua_pushboolean(L, (res & value)?1:0);
    return 1;
}

static int conn_get_equal_uint16_info_(lua_State*L, conn_data *conn, SQLUSMALLINT optnum,
    SQLUSMALLINT value
){
    SQLUSMALLINT res;
    SQLUSMALLINT dummy;
    SQLRETURN ret = SQLGetInfo(conn->hdbc, optnum, (SQLPOINTER)&res, sizeof(res), &dummy);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hDBC, conn->hdbc);

    lua_pushboolean(L, (res == value)?1:0);
    return 1;
}

//}

//{ some attributes 

/*
** Sets the auto commit mode
*/
static int conn_setautocommit (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_set_uint_attr_(L,conn,SQL_ATTR_AUTOCOMMIT,
      lua_toboolean (L, 2)?SQL_AUTOCOMMIT_ON:SQL_AUTOCOMMIT_OFF
    );
}

static int conn_getautocommit(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    int ret = conn_get_uint_attr_(L,conn,SQL_ATTR_AUTOCOMMIT);
    if(is_fail(L,ret)) return ret;
    if(0 == ret){
        lua_pushboolean(L, SQL_AUTOCOMMIT_ON == SQL_AUTOCOMMIT_DEFAULT);
        return 1;
    }
    assert(1 == ret);
    lua_pushboolean(L, SQL_AUTOCOMMIT_ON == lua_tointeger(L,-1));
    lua_remove(L,-2);
    return 1;
}

static int conn_setlogintimeout (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_set_uint_attr_(L,conn,SQL_ATTR_LOGIN_TIMEOUT,lua_tointeger (L, 2));
}

static int conn_getlogintimeout (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_uint_attr_(L,conn,SQL_ATTR_LOGIN_TIMEOUT);
}

static int conn_setcatalog(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    size_t len;
    const char *str = luaL_checklstring(L,2,&len);
    return conn_set_str_attr_(L, conn, SQL_ATTR_CURRENT_CATALOG, str, len);
}

static int conn_getcatalog(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_str_attr_(L,conn,SQL_ATTR_CURRENT_CATALOG);
}

static int conn_setreadonly (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_set_uint_attr_(L,conn,SQL_ACCESS_MODE,
      lua_toboolean (L, 2)?SQL_MODE_READ_ONLY:SQL_MODE_READ_WRITE
    );
}

static int conn_getreadonly(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    int ret = conn_get_uint_attr_(L,conn,SQL_ACCESS_MODE);
    if(is_fail(L,ret)) return ret;
    if(0 == ret){
        lua_pushboolean(L, SQL_MODE_READ_ONLY == SQL_MODE_DEFAULT);
        return 1;
    }
    assert(1 == ret);
    lua_pushboolean(L, SQL_MODE_READ_ONLY == lua_tointeger(L,-1));
    lua_remove(L,-2);
    return 1;
}

static int conn_settrace (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_set_uint_attr_(L,conn,SQL_ATTR_TRACE,
      lua_toboolean (L, 2)?SQL_OPT_TRACE_ON:SQL_OPT_TRACE_OFF
    );
}

static int conn_gettrace(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    int ret = conn_get_uint_attr_(L,conn,SQL_ATTR_TRACE);
    if(is_fail(L,ret)) return ret;
    if(0 == ret){
        lua_pushboolean(L, SQL_OPT_TRACE_ON == SQL_OPT_TRACE_DEFAULT);
        return 1;
    }
    assert(1 == ret);
    lua_pushboolean(L, SQL_OPT_TRACE_ON == lua_tointeger(L,-1));
    lua_remove(L,-2);
    return 1;
}

static int conn_supportsTransactions(lua_State *L);

static int conn_gettransactionisolation(lua_State *L) {
  int ret;
  conn_data *conn = (conn_data *) getconnection (L);
  ret = conn_supportsTransactions(L);
  if(is_unknown(L, ret))return ret;
  assert(ret == 1);
  if(lua_toboolean(L, -1)){
    SQLUINTEGER lvl;
    lua_pop(L,1);
    ret = conn_get_uint_attr_(L, conn, 
      (LUASQL_ODBC3_C(SQL_ATTR_TXN_ISOLATION,SQL_TXN_ISOLATION)));
    if(ret == 0) return TRANSACTION_NONE;
    if(ret == 2) return ret;
    assert(ret == 1);
    lvl = (SQLUINTEGER)lua_tonumber(L,-1);
    lua_pop(L,1);

    switch(lvl) {
      case SQL_TXN_READ_UNCOMMITTED:
        lua_pushinteger(L,TRANSACTION_READ_UNCOMMITTED);
        return 1;
      case SQL_TXN_READ_COMMITTED:
        lua_pushinteger(L,TRANSACTION_READ_COMMITTED);
        return 1;
      case SQL_TXN_REPEATABLE_READ:
        lua_pushinteger(L,TRANSACTION_REPEATABLE_READ);
        return 1;
      case SQL_TXN_SERIALIZABLE:
#if LUASQL_ODBCVER < 0x0300 && defined SQL_TXN_VERSIONING
      case SQL_TXN_VERSIONING:
#endif
        lua_pushinteger(L,TRANSACTION_SERIALIZABLE);
        return 1;
    }
  }
  lua_pushinteger(L,TRANSACTION_NONE);
  return 1;
}

static int conn_settransactionisolation(lua_State *L){
  int ret;
  conn_data *conn = (conn_data *) getconnection (L);
  int i = luaL_checkinteger(L, 2);
  ret = conn_supportsTransactions(L);
  if(is_unknown(L, ret))return ret;
  if(lua_toboolean(L, -1)){
    SQLUINTEGER lvl;
    lua_pop(L,1);
    switch(i) {
      case TRANSACTION_READ_UNCOMMITTED:
        lvl = SQL_TXN_READ_UNCOMMITTED;
        break;

      case TRANSACTION_READ_COMMITTED:
        lvl = SQL_TXN_READ_COMMITTED;
        break;

      case TRANSACTION_REPEATABLE_READ:
        lvl = SQL_TXN_REPEATABLE_READ;
        break;

      case TRANSACTION_SERIALIZABLE:
        lvl = SQL_TXN_SERIALIZABLE;
        break;

      default:
        lua_pushnil(L);
        lua_pushliteral(L,"Invalid transaction isolation");
        return 2;
    }
    return conn_set_uint_attr_(L,conn,(LUASQL_ODBC3_C(SQL_ATTR_TXN_ISOLATION,SQL_TXN_ISOLATION)),lvl);
  }
  if(i == TRANSACTION_NONE) return pass(L);

  lua_pushnil(L);
  lua_pushliteral(L,"Data source does not support transactions");
  return 2;
}

static int conn_settracefile(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    size_t len;
    const char *str = luaL_checklstring(L,2,&len);
    return conn_set_str_attr_(L, conn, SQL_ATTR_TRACEFILE, str, len);
}

static int conn_gettracefile(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_str_attr_(L,conn,SQL_ATTR_TRACEFILE);
}

static int conn_connected(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    int ret = conn_get_uint_attr_(L,conn,SQL_ATTR_CONNECTION_DEAD);
    if(!is_fail(L,ret)){
        lua_pushboolean(L, SQL_CD_FALSE == lua_tointeger(L,-1));
        lua_remove(L,-2);
    };
    return ret;
}

static int conn_get_uint_attr(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return conn_get_uint_attr_(L, conn, optnum);
}

static int conn_get_str_attr(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return conn_get_str_attr_(L, conn, optnum);
}

static int conn_set_uint_attr(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  return conn_set_uint_attr_(L, conn, optnum,luaL_checkinteger(L,3));
}

static int conn_set_str_attr(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  SQLINTEGER optnum = luaL_checkinteger(L,2);
  size_t len;
  const char *str = luaL_checklstring(L,3,&len);
  return conn_set_str_attr_(L, conn, optnum, str, len);
}

static int conn_get_uint16_info(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  SQLUSMALLINT optnum = luaL_checkinteger(L,2);
  return conn_get_uint16_info_(L, conn, optnum);
}

static int conn_get_uint32_info(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  SQLUSMALLINT optnum = luaL_checkinteger(L,2);
  return conn_get_uint16_info_(L, conn, optnum);
}

static int conn_get_str_info(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  SQLUSMALLINT optnum = luaL_checkinteger(L,2);
  return conn_get_str_info_(L, conn, optnum);
}

static int conn_has_prepare(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  lua_pushboolean(L, conn->supports[LUASQL_CONN_SUPPORT_PREPARE]);
  return 1;
}

static int conn_has_bindparam(lua_State*L){
  conn_data *conn = (conn_data *) getconnection (L);
  lua_pushboolean(L, conn->supports[LUASQL_CONN_SUPPORT_BINDPARAM]);
  return 1;
}

//}

//{ driver metadata

static int conn_getdbmsname(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_str_info_(L,conn,SQL_DBMS_NAME);
}

static int conn_getdbmsver(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_str_info_(L,conn,SQL_DBMS_VER);
}

static int conn_getdrvname(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_str_info_(L,conn,SQL_DRIVER_NAME);
}

static int conn_getdrvver(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_str_info_(L,conn,SQL_DRIVER_VER);
}

static int conn_getodbcver(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    return conn_get_str_info_(L, conn, SQL_DRIVER_ODBC_VER);
}

static int conn_getodbcvermm_(lua_State *L, conn_data *conn){
    int ret = conn_get_str_info_(L, conn, SQL_DRIVER_ODBC_VER);
    size_t len;
    const char*str;
    int minor,major;
    if(ret != 1)
        return ret;
    str = lua_tolstring(L,-1,&len);
    if((!str)||(len != 5)||(str[2] != '.')){
        lua_pushnil(L);
        return 2;
    }
    major = atoi(&str[0]);
    minor = atoi(&str[3]);
    lua_pop(L,1);

    lua_pushnumber(L, major);
    lua_pushnumber(L, minor);
    return 2;
}

static int conn_getodbcvermm(lua_State *L){
  conn_data *conn = (conn_data *) getconnection (L);
  return conn_getodbcvermm_(L, conn);
}


#ifdef LUASQL_USE_DRIVERINFO

static int conn_init_di_(lua_State *L, conn_data *conn){

#define CLEANUP() free(di)
#define UINT_INFO(VAR, WHAT, DEF) \
    ret = conn_get_uint32_info_(L, conn, (WHAT));\
    if(is_fail(L,ret)){CLEANUP(); return ret;}\
    if(ret == 0) (VAR) = (DEF);\
    else{\
        (VAR) = luaL_checkint(L,-1);\
        lua_pop(L, 1);\
    }

    SQLRETURN ret;
    drvinfo_data *di;
    if(conn->di) return 0;
    
    di = malloc(sizeof(drvinfo_data));
    if(!di)
        return LUASQL_ALLOCATE_ERROR(L);

    ret = conn_getodbcvermm_(L, conn);
    if((ret != 2) || (!lua_isnumber(L,-1)) || (!lua_isnumber(L,-2))) 
        return ret;
    di->minorVersion_ = lua_tointeger(L, -1);
    di->majorVersion_ = lua_tointeger(L, -2);
    lua_pop(L, 2);
 
    UINT_INFO(di->getdataExt_, SQL_GETDATA_EXTENSIONS,0);
    UINT_INFO(di->cursorMask_, SQL_SCROLL_OPTIONS,0);

#if LUASQL_ODBCVER >= 0x0300
    if(di->majorVersion_ >= 3) {
        if(di->cursorMask_&SQL_SO_FORWARD_ONLY) {
            UINT_INFO(di->forwardOnlyA2_, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,0);
        }
        if(di->cursorMask_&SQL_SO_STATIC) {
            UINT_INFO(di->staticA2_, SQL_STATIC_CURSOR_ATTRIBUTES2,0);
        }
        if(di->cursorMask_&SQL_SO_KEYSET_DRIVEN) {
            UINT_INFO(di->keysetA2_, SQL_KEYSET_CURSOR_ATTRIBUTES2,0);
        }
        if(di->cursorMask_&SQL_SO_DYNAMIC) {
            UINT_INFO(di->dynamicA2_, SQL_DYNAMIC_CURSOR_ATTRIBUTES2,0);
        }
    } else {
#endif
    // use the ODBC2 way of finding out things
    UINT_INFO(di->concurMask_, SQL_SCROLL_CONCURRENCY,0);
#if ODBCVER >= 0x0300
    }
#endif

#ifdef LUASQL_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
    ret = SQLGetFunctions(conn->hdbc,
                            LUASQL_ODBC3_C(
                                SQL_API_ODBC3_ALL_FUNCTIONS,
                                SQL_API_ALL_FUNCTIONS
                            ),
                            di->supportedFunctions_
    );

    if(error(ret)){
        CLEANUP();
        return fail(L, hDBC, conn->hdbc);
    }
#endif

    conn->di = di;
    return 0;

#undef UINT_INFO
#undef CLEANUP
}

#endif // LUASQL_USE_DRIVERINFO

//}

//{ luasql interface

/*
** Closes a connection.
*/
static int conn_close (lua_State *L) {
    SQLRETURN ret;
    env_data *env;
    conn_data *conn = (conn_data *)luaL_checkudata(L,1,LUASQL_CONNECTION_ODBC);
    int ret_count;
    luaL_argcheck (L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
#ifdef LUASQL_USE_DRIVERINFO
    if(conn->di){
        free(conn->di);
        conn->di = NULL;
    }
#endif

    if (conn->closed) {
        lua_pushboolean (L, 0);
        return 1;
    }
    if (conn->cur_counter > 0)
        return luaL_error (L, LUASQL_PREFIX"there are open cursors");

    ret_count = pass(L);
    /* Decrement connection counter on environment object */
    lua_rawgeti (L, LUA_REGISTRYINDEX, conn->env);
    env = lua_touserdata (L, -1);
    lua_pop(L,1);

    /* Nullify structure fields. */
    env->conn_counter--;
    ret = SQLDisconnect(conn->hdbc);
    if (error(ret)) ret_count+=push_diagnostics(L, hDBC, conn->hdbc);
    ret = SQLFreeHandle(hDBC, conn->hdbc);
    if (error(ret)) ret_count+=push_diagnostics(L, hDBC, conn->hdbc);
    conn->closed = 1;
    luaL_unref (L, LUA_REGISTRYINDEX, conn->env);
    return ret_count;
}

/*
** Executes a SQL statement.
** Returns
**   cursor object: if there are results or
**   row count: number of rows affected by statement if no results
*/
static int conn_execute (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    const char *statement = luaL_checkstring(L, 2);
    SQLHDBC hdbc = conn->hdbc;
    SQLHSTMT hstmt;
    SQLSMALLINT numcols;
    SQLRETURN ret;
    ret = SQLAllocHandle(hSTMT, hdbc, &hstmt);
    if (error(ret))
        return fail(L, hDBC, hdbc);

    /* execute the statement */
    ret = SQLExecDirect (hstmt, (char *) statement, SQL_NTS);
    if (error(ret)) {
        ret = fail(L, hSTMT, hstmt);
        SQLFreeHandle(hSTMT, hstmt);
        return ret;
    }

    /* determine the number of results */
    ret = SQLNumResultCols (hstmt, &numcols);
    if (error(ret)) {
        ret = fail(L, hSTMT, hstmt);
        SQLFreeHandle(hSTMT, hstmt);
        return ret;
    }

    if (numcols > 0)
        /* if there is a results table (e.g., SELECT) */
        return cur_create (L, 1, conn, hstmt, numcols);
    else {
        /* if action has no results (e.g., UPDATE) */
        SQLINTEGER numrows;
        ret = SQLRowCount(hstmt, &numrows);
        if (error(ret)) {
            ret = fail(L, hSTMT, hstmt);
            SQLFreeHandle(hSTMT, hstmt);
            return ret;
        }
        lua_pushnumber(L, numrows);
        SQLFreeHandle(hSTMT, hstmt);
        return 1;
    }
}

/*
** Rolls back a transaction. 
*/
static int conn_commit (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    SQLRETURN ret = SQLEndTran(hDBC, conn->hdbc, SQL_COMMIT);
    if (error(ret))
        return fail(L, hSTMT, conn->hdbc);
    else
        return pass(L);
}

/*
** Rollback the current transaction. 
*/
static int conn_rollback (lua_State *L) {
    conn_data *conn = getconnection (L);
    SQLRETURN ret = SQLEndTran(hDBC, conn->hdbc, SQL_ROLLBACK);
    if (error(ret))
        return fail(L, hSTMT, conn->hdbc);
    else
        return pass(L);
}

/*
** Create a new Connection object and push it on top of the stack.
*/
static int conn_create (lua_State *L, int o, env_data *env, SQLHDBC hdbc) {
    int top = lua_gettop(L);
    conn_data *conn = (conn_data *) lua_newuserdata(L, sizeof(conn_data));
    luasql_setmeta (L, LUASQL_CONNECTION_ODBC);
    assert(1 == (lua_gettop(L)-top));

    /* fill in structure */
    conn->closed = 0;
    conn->cur_counter = 0;
    conn->env = LUA_NOREF;
    conn->hdbc = hdbc;
    lua_pushvalue (L, o);
    conn->env = luaL_ref (L, LUA_REGISTRYINDEX);
    env->conn_counter++;
    memset(&conn->supports[0],0,LUASQL_CONN_SUPPORT_MAX * sizeof(conn->supports[0]));
    assert(1 == (lua_gettop(L)-top));

#ifdef LUASQL_USE_DRIVERINFO
    conn->di = NULL;
#endif
    assert(1 == (lua_gettop(L)-top));

    return 1;
}

//}

//{ connect
/*
** init metainfo
*/
static int conn_after_connect(lua_State *L){
    conn_data *conn = getconnection (L);
    int top = lua_gettop(L);
    memset(&conn->supports[0],0,LUASQL_CONN_SUPPORT_MAX * sizeof(conn->supports[0]));

    {SQLSMALLINT val = 0;
    SQLRETURN ret = SQLGetFunctions(conn->hdbc,SQL_API_SQLPREPARE,&val);
    if(!error(ret)) conn->supports[LUASQL_CONN_SUPPORT_PREPARE] = val?1:0;}

    {SQLSMALLINT val = 0;
    SQLRETURN ret = SQLGetFunctions(conn->hdbc,SQL_API_SQLBINDPARAMETER,&val);
    if(!error(ret)) conn->supports[LUASQL_CONN_SUPPORT_BINDPARAM] = val?1:0;}

    {SQLSMALLINT val = 0;
    SQLRETURN ret = SQLGetFunctions(conn->hdbc,SQL_API_SQLNUMPARAMS,&val);
    if(!error(ret)) conn->supports[LUASQL_CONN_SUPPORT_NUMPARAMS] = val?1:0;}

    {int ret;
      lua_pushvalue(L,1);
      lua_insert(L,1);
      ret = conn_supportsTransactions(L);
      if(!is_unknown(L, ret)) conn->supports[LUASQL_CONN_SUPPORT_TXN] = lua_toboolean(L, -1);
      lua_pop(L,ret);
      lua_remove(L,1);
    }

    /* luasql compatability */
    if(conn->supports[LUASQL_CONN_SUPPORT_TXN])/* set auto commit mode */
        SQLSetConnectAttr(conn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, 0);
    assert(lua_gettop(L) == top);
    
    lua_pushvalue(L, 1);
    return 1; // self
}

static int conn_disconnect (lua_State *L) {
    conn_data *conn = getconnection (L);
    SQLRETURN ret;
    if (conn->cur_counter > 0)
        return luaL_error (L, LUASQL_PREFIX"there are open cursors");
    ret = SQLDisconnect(conn->hdbc);
    if (error(ret)) return fail(L, hDBC, conn->hdbc);
    return pass(L);
}

static int conn_connect (lua_State *L) {
    conn_data *conn = getconnection (L);
    const char *sourcename = luaL_checkstring (L, 2);
    const char *username = luaL_optstring (L, 3, NULL);
    const char *password = luaL_optstring (L, 4, NULL);
    SQLRETURN ret = SQLConnect (conn->hdbc, (char *) sourcename, SQL_NTS, 
        (char *) username, SQL_NTS, (char *) password, SQL_NTS);
    if (error(ret)) return fail(L, hDBC, conn->hdbc);

    return conn_after_connect(L);
}

void add_keyval_table(lua_State *L, luaL_Buffer *b, int t){
  int top = lua_gettop(L);
  assert(t>0);
  assert(lua_istable(L,t));
  lua_pushnil(L);
  while (lua_next(L, t) != 0) {
    /* uses 'key' (at index -2) and 'value' (at index -1) */
    if(lua_isnumber(L,-2)){
      lua_pop(L,1);
      continue;
    }
    lua_pushvalue(L,-2);
    luaL_addvalue(b);
    luaL_addchar(b, '=');
    luaL_addvalue(b);
    luaL_addchar(b, ';');
  }
  assert(top == lua_gettop(L));
}

void table_to_cnnstr(lua_State *L, int t){
    int top = lua_gettop(L);
    luaL_Buffer b;
    int i = 1;
    assert(t>0);
    assert(lua_istable(L,t));
    luaL_buffinit(L, &b);

    lua_rawgeti(L,t,i);
    while(!lua_isnil(L,-1)){
      i++;
      if(lua_isstring(L,-1)){
        luaL_addvalue(&b);
      }
      else if(lua_istable(L,-1)){
        add_keyval_table(L, &b, lua_gettop(L));
        lua_pop(L,1);
      }
      lua_rawgeti(L,t,i);
    }
    lua_pop(L,1);
    assert(top == lua_gettop(L));
    add_keyval_table(L,&b,t);

    lua_remove(L,t);
    luaL_pushresult(&b);
    lua_insert(L,t);

    assert(top == lua_gettop(L));
}

static int conn_driverconnect(lua_State *L){
    const MIN_CONNECTSTRING_SIZE = 1024; // msdn say

    conn_data *conn = getconnection (L);
    size_t connectStringSize;
    const char *connectString = lua_istable(L,2)?table_to_cnnstr(L,2),luaL_checklstring(L, 2, &connectStringSize):
                                                                      luaL_checklstring(L, 2, &connectStringSize);
    SQLUSMALLINT drvcompl = SQL_DRIVER_NOPROMPT;
    char *buf;
    SQLSMALLINT bufSize;
    SQLHDBC hdbc = conn->hdbc;
    SQLRETURN ret;

    buf = malloc(MIN_CONNECTSTRING_SIZE);
    if(!buf) return LUASQL_ALLOCATE_ERROR(L);

    ret = SQLDriverConnect(hdbc,0,(SQLPOINTER)connectString,connectStringSize,
                               buf,MIN_CONNECTSTRING_SIZE,&bufSize,drvcompl);
    /// @todo check if buf size too smaal
    if(error(ret)){
        free(buf);
        return fail(L, hDBC, hdbc);
    }

    ret = conn_after_connect(L);
    lua_pushstring(L,buf);
    free(buf);
    return ret + 1;
}

//}

//{ create STMT interface

static int stmt_create_ (lua_State *L, int o, conn_data *conn, stmt_data **pstmt);
static int stmt_prepare_ (lua_State *L, stmt_data *stmt, const char *statement);
static void stmt_destroy_ (lua_State *L, stmt_data *stmt);

/*
** Prepare a SQL statement.
** Returns - stmt object
*/
static int conn_create_stmt (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    return stmt_create_(L, 1, conn, NULL);
}

/*
** Prepare a SQL statement.
** Returns - stmt object
*/
static int conn_prepare_stmt (lua_State *L) {
    conn_data *conn = (conn_data *) getconnection (L);
    const char *statement = luaL_checkstring(L, 2);
    SQLRETURN ret;
    stmt_data *stmt = NULL;

    ret = stmt_create_(L, 1, conn, &stmt);
    if(!stmt)
        return ret;

    ret = stmt_prepare_(L, stmt, statement);
    if(ret){
        stmt_destroy_(L, stmt);
        return ret;
    }

    return 1;
}

//}

//{ database catalog

#define CONN_BEFORE_CALL() \
    conn_data *conn = (conn_data *) getconnection (L);\
    SQLHSTMT hstmt;\
    SQLSMALLINT numcols;\
    SQLRETURN ret = SQLAllocHandle(hSTMT, conn->hdbc, &hstmt);\
    if (error(ret)) return fail(L, hDBC, conn->hdbc);

#define CONN_AFTER_CALL() \
    if (error(ret)){\
        ret = fail(L, hSTMT, hstmt);\
        SQLFreeHandle(hSTMT, hstmt);\
        return ret;\
    }\
    ret = SQLNumResultCols (hstmt, &numcols);\
    if (error(ret)) {\
      ret = fail(L, hSTMT, hstmt);\
      SQLFreeHandle(hSTMT, hstmt);\
      return ret;\
    }\
    if (numcols > 0)\
        return cur_create (L, 1, conn, hstmt, numcols);\
    else { \
        SQLINTEGER numrows;\
        ret = SQLRowCount(hstmt, &numrows);\
        if (error(ret)) {\
            ret = fail(L, hSTMT, hstmt);\
            SQLFreeHandle(hSTMT, hstmt);\
            return ret;\
        }\
        lua_pushnumber(L, numrows);\
        SQLFreeHandle(hSTMT, hstmt);\
        return 1;\
    }

static int conn_gettypeinfo(lua_State *L) {
    SQLSMALLINT sqltype = SQL_ALL_TYPES;
    CONN_BEFORE_CALL();

    if(lua_gettop(L)>1)
      sqltype = luaL_checkint(L,2);

    ret = SQLGetTypeInfo(hstmt, sqltype);

    CONN_AFTER_CALL();
}

static int conn_gettables(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName, *types;
    size_t scatalog, sschema, stableName, stypes;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
    types      = luaL_optlstring(L,5,EMPTY_STRING,&stypes);

    ret = SQLTables(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)tableName, stableName, (SQLPOINTER)types, stypes);

    CONN_AFTER_CALL();
}

static int conn_getstatistics(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;
    SQLUSMALLINT unique, reserved;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
    unique     = lua_toboolean(L,5)?SQL_INDEX_UNIQUE:SQL_INDEX_ALL;
    reserved   = lua_toboolean(L,6)?SQL_QUICK:SQL_ENSURE;

    ret = SQLStatistics(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)tableName, stableName, unique, reserved);

    CONN_AFTER_CALL();
}

static int conn_gettableprivileges(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;
    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret = SQLTablePrivileges(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)tableName, stableName);

    CONN_AFTER_CALL();
}

static int conn_getcolumnprivileges(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName, *columnName;
    size_t scatalog, sschema, stableName, scolumnName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
    columnName = luaL_optlstring(L,5,EMPTY_STRING,&scolumnName);

    ret = SQLColumnPrivileges(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)tableName, stableName, (SQLPOINTER)columnName, scolumnName);

    CONN_AFTER_CALL();
}

static int conn_getprimarykeys(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret = SQLPrimaryKeys(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)tableName, stableName);

    CONN_AFTER_CALL();
}

static int conn_getindexinfo(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret = SQLStatistics(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)tableName, stableName,
      lua_toboolean(L,5)?SQL_INDEX_UNIQUE:SQL_INDEX_ALL,
      lua_toboolean(L,6)?SQL_QUICK:SQL_ENSURE
    );

    CONN_AFTER_CALL();
}

static int conn_crossreference(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *pc, *ps, *pt, *fc, *fs, *ft;
    size_t spc, sps, spt, sfc, sfs, sft;

    CONN_BEFORE_CALL();

    pc = luaL_optlstring(L,2,EMPTY_STRING,&spc);  // primaryCatalog
    ps = luaL_optlstring(L,3,EMPTY_STRING,&sps);  // primarySchema 
    pt = luaL_optlstring(L,4,EMPTY_STRING,&spt);  // primaryTable
    fc = luaL_optlstring(L,5,EMPTY_STRING,&sfc);  // foreignCatalog
    fs = luaL_optlstring(L,6,EMPTY_STRING,&sfs);  // foreignSchema
    ft = luaL_optlstring(L,7,EMPTY_STRING,&sft);  // foreignTable    
                                             
    ret = SQLForeignKeys(hstmt, 
      (SQLPOINTER)pc, spc, 
      (SQLPOINTER)ps, sps, 
      (SQLPOINTER)pt, spt, 
      (SQLPOINTER)fc, sfc, 
      (SQLPOINTER)fs, sfs, 
      (SQLPOINTER)ft, sft
    );

    CONN_AFTER_CALL();
}

static int conn_getprocedures(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *procName;
    size_t scatalog, sschema, sprocName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    procName   = luaL_optlstring(L,4,EMPTY_STRING,&sprocName);

    ret = SQLProcedures(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)procName, sprocName);

    CONN_AFTER_CALL();
}

static int conn_getprocedurecolumns(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *procName, *colName;
    size_t scatalog, sschema, sprocName, scolName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    procName   = luaL_optlstring(L,4,EMPTY_STRING,&sprocName);
    colName    = luaL_optlstring(L,5,EMPTY_STRING,&scolName);

    ret = SQLProcedureColumns(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)procName, sprocName, (SQLPOINTER)colName, scolName);

    CONN_AFTER_CALL();
}

static int conn_getspecialcolumns(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName;
    size_t scatalog, sschema, stableName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);

    ret=SQLSpecialColumns(hstmt,lua_tointeger(L,5)/*wat*/,
                                (SQLPOINTER)catalog,scatalog,
                                (SQLPOINTER)schema,sschema,
                                (SQLPOINTER)tableName,stableName,
                                lua_tointeger(L,6)/*scope*/,lua_tointeger(L,7)/*nullable*/);


    CONN_AFTER_CALL();
}

static int conn_getcolumns(lua_State *L){
    static const char* EMPTY_STRING=NULL;
    const char *catalog, *schema, *tableName, *columnName;
    size_t scatalog, sschema, stableName, scolumnName;

    CONN_BEFORE_CALL();

    catalog    = luaL_optlstring(L,2,EMPTY_STRING,&scatalog);
    schema     = luaL_optlstring(L,3,EMPTY_STRING,&sschema);
    tableName  = luaL_optlstring(L,4,EMPTY_STRING,&stableName);
    columnName = luaL_optlstring(L,5,EMPTY_STRING,&scolumnName);

    ret = SQLColumns(hstmt, (SQLPOINTER)catalog, scatalog, (SQLPOINTER)schema, sschema, 
      (SQLPOINTER)tableName, stableName, (SQLPOINTER)columnName, scolumnName);

    CONN_AFTER_CALL();
}

#undef CONN_BEFORE_CALL
#undef CONN_AFTER_CALL

//}

//{ database metadata
// port DatabaseMetadata from libodbc++

  // returns the actual ODBC cursor type this datasource would
  // use for a given ResultSet type
static int getODBCCursorTypeFor(int rsType, const drvinfo_data* di){
    int r;
    assert(IS_VALID_RS_TYPE(rsType));
    switch(rsType) {
        case RS_TYPE_FORWARD_ONLY:
            r = SQL_CURSOR_FORWARD_ONLY;
            break;
        case RS_TYPE_SCROLL_INSENSITIVE:
            r = SQL_CURSOR_STATIC;
            break;
        case RS_TYPE_SCROLL_SENSITIVE:
            if(di->cursorMask_  & SQL_SO_DYNAMIC){
                r = SQL_CURSOR_DYNAMIC;
            } else {
                r = SQL_CURSOR_KEYSET_DRIVEN;
            }
            break;
        default:
            assert(0);
  }

  return r;
}

#if LUASQL_ODBCVER >= 0x0300
static int getCursorAttributes1For(int odbcType){
    int infoType;
    switch(odbcType) {
        case SQL_CURSOR_FORWARD_ONLY:
            infoType = SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1;
            break;
        case SQL_CURSOR_STATIC:
            infoType = SQL_STATIC_CURSOR_ATTRIBUTES1;
            break;
        case SQL_CURSOR_KEYSET_DRIVEN:
            infoType = SQL_KEYSET_CURSOR_ATTRIBUTES1;
            break;
        case SQL_CURSOR_DYNAMIC:
        default:
            infoType = SQL_DYNAMIC_CURSOR_ATTRIBUTES1;
            break;
    }
    return infoType;
}

static int getCursorAttributes2For(int odbcType){
    int infoType;
    switch(odbcType) {
        case SQL_CURSOR_FORWARD_ONLY:
            infoType = SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2;
            break;
        case SQL_CURSOR_STATIC:
            infoType = SQL_STATIC_CURSOR_ATTRIBUTES2;
            break;
        case SQL_CURSOR_KEYSET_DRIVEN:
            infoType = SQL_KEYSET_CURSOR_ATTRIBUTES2;
            break;
        case SQL_CURSOR_DYNAMIC:
        default:
            infoType = SQL_DYNAMIC_CURSOR_ATTRIBUTES2;
            break;
    }
    return infoType;
}

#endif // ODBCVER >= 0x0300


#define DEFINE_GET_STRING_INFO(FNAME, WHAT)\
static int conn_##FNAME(lua_State *L){\
    conn_data *conn = (conn_data *) getconnection (L); \
    return conn_get_str_info_(L,conn,(WHAT));\
}

#define DEFINE_GET_UINT16_AS_BOOL_INFO(FNAME, WHAT, TVALUE)\
  static int conn_##FNAME(lua_State *L){\
  conn_data *conn = (conn_data *) getconnection (L);\
  return conn_get_equal_uint16_info_(L, conn, (WHAT), (TVALUE));\
}

#define DEFINE_GET_UINT32_AS_MASK_INFO(FNAME, WHAT, TVALUE)\
  static int conn_##FNAME(lua_State *L){\
  conn_data *conn = (conn_data *) getconnection (L);\
  return conn_get_and_uint32_info_(L, conn, (WHAT), (TVALUE));\
}

#define DEFINE_GET_UINT16_AS_NOTBOOL_INFO(FNAME, WHAT, TVALUE)\
  static int conn_##FNAME(lua_State *L){\
  conn_data *conn = (conn_data *) getconnection (L);\
  int ret = conn_get_equal_uint16_info_(L, conn, (WHAT), (TVALUE));\
  if(is_unknown(L,ret)) return ret;\
  assert(ret == 1);\
  lua_pushboolean(L, lua_toboolean(L, -1)?0:1);\
  lua_remove(L, -2);\
  return 1;\
}

#define DEFINE_GET_CHAR_AS_BOOL_INFO(FNAME, WHAT, TVALUE)\
  static int conn_##FNAME(lua_State *L){\
  conn_data *conn = (conn_data *) getconnection (L);\
  return conn_get_equal_char_info_(L, conn, (WHAT), (TVALUE));\
}

#define DEFINE_GET_CHAR_AS_NOTBOOL_INFO(FNAME, WHAT, TVALUE)\
  static int conn_##FNAME(lua_State *L){\
  conn_data *conn = (conn_data *) getconnection (L);\
  int ret = conn_get_equal_char_info_(L, conn, (WHAT), (TVALUE));\
  if(is_unknown(L,ret)) return ret;\
  assert(ret == 1);\
  lua_pushboolean(L, lua_toboolean(L, -1)?0:1);\
  lua_remove(L, -2);\
  return 1;\
}

#define GET_UINT32_INFO(VAR, WHAT) \
    (VAR) = conn_get_uint32_info_(L, conn, (WHAT));\
    if((VAR) != 1){return (VAR);}\
    (VAR) = luaL_checkinteger(L,-1);\
    lua_pop(L, 1);

#define GET_UINT16_INFO(VAR, WHAT) \
    (VAR) = conn_get_uint16_info_(L, conn, (WHAT));\
    if((VAR) != 1){return (VAR);}\
    (VAR) = luaL_checkinteger(L,-1);\
    lua_pop(L, 1);

#define DEFINE_GET_UINT32_INFO(FNAME, WHAT)\
  static int conn_##FNAME(lua_State *L){\
  conn_data *conn = (conn_data *) getconnection (L);\
  return conn_get_uint32_info_(L, conn, (WHAT));\
}

#define DEFINE_GET_UINT16_INFO(FNAME, WHAT)\
  static int conn_##FNAME(lua_State *L){\
  conn_data *conn = (conn_data *) getconnection (L);\
  return conn_get_uint32_info_(L, conn, (WHAT));\
}



DEFINE_GET_STRING_INFO(getIdentifierQuoteString,
    SQL_IDENTIFIER_QUOTE_CHAR
)

DEFINE_GET_STRING_INFO(getCatalogTerm,
    LUASQL_ODBC3_C(SQL_CATALOG_TERM,SQL_QUALIFIER_TERM)
)

DEFINE_GET_STRING_INFO(getSchemaTerm,
    LUASQL_ODBC3_C(SQL_SCHEMA_TERM,SQL_OWNER_TERM)
)

DEFINE_GET_STRING_INFO(getTableTerm,
    LUASQL_ODBC3_C(SQL_SCHEMA_TERM,SQL_OWNER_TERM)
)

DEFINE_GET_STRING_INFO(getProcedureTerm,
    SQL_PROCEDURE_TERM
)

DEFINE_GET_STRING_INFO(getUserName,
    SQL_USER_NAME
)

DEFINE_GET_STRING_INFO(getCatalogSeparator,
    LUASQL_ODBC3_C(SQL_CATALOG_NAME_SEPARATOR, SQL_QUALIFIER_NAME_SEPARATOR)
)

DEFINE_GET_CHAR_AS_BOOL_INFO(isCatalogName,
    SQL_CATALOG_NAME,
    'Y'
)

DEFINE_GET_UINT16_AS_BOOL_INFO(isCatalogAtStart,
    LUASQL_ODBC3_C(SQL_CATALOG_LOCATION,SQL_QUALIFIER_LOCATION),
    SQL_QL_START
)

DEFINE_GET_STRING_INFO(getSQLKeywords,
    SQL_KEYWORDS
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsTransactions,
  SQL_TXN_CAPABLE, SQL_TC_NONE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsDataDefinitionAndDataManipulationTransactions,
    SQL_TXN_CAPABLE,
    SQL_TC_ALL
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsDataManipulationTransactionsOnly,
    SQL_TXN_CAPABLE, SQL_TC_DML
)

DEFINE_GET_UINT16_AS_BOOL_INFO(dataDefinitionCausesTransactionCommit,
    SQL_TXN_CAPABLE, SQL_TC_DDL_COMMIT
)

DEFINE_GET_UINT16_AS_BOOL_INFO(dataDefinitionIgnoredInTransactions,
    SQL_TXN_CAPABLE, SQL_TC_DDL_IGNORE
)

static int conn_getDefaultTransactionIsolation(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    int top = lua_gettop(L);
    SQLUINTEGER r;
    GET_UINT32_INFO(r, SQL_DEFAULT_TXN_ISOLATION);
    switch(r) {
        case SQL_TXN_READ_UNCOMMITTED:
            lua_pushnumber(L, TRANSACTION_READ_UNCOMMITTED);
            break;
        case SQL_TXN_READ_COMMITTED:
            lua_pushnumber(L, TRANSACTION_READ_COMMITTED);
            break;
        case SQL_TXN_REPEATABLE_READ:
            lua_pushnumber(L, TRANSACTION_REPEATABLE_READ);
            break;
        case SQL_TXN_SERIALIZABLE:
        #if defined(SQL_TXN_VERSIONING)
        case SQL_TXN_VERSIONING:
        #endif
            lua_pushnumber(L, TRANSACTION_SERIALIZABLE);
            break;
        default:
            lua_pushnumber(L, TRANSACTION_NONE);
    }
    assert(1 == (lua_gettop(L) - top));
    return 1;
}

static int conn_supportsTransactionIsolationLevel(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER r;
    SQLUINTEGER ret=0;
    SQLUINTEGER lev = luaL_checkint(L, 2);
    int top = lua_gettop(L);

    GET_UINT32_INFO(r, SQL_TXN_ISOLATION_OPTION);
    switch(lev){
        case TRANSACTION_READ_UNCOMMITTED:
            ret = r&SQL_TXN_READ_UNCOMMITTED;
            break;
        case TRANSACTION_READ_COMMITTED:
            ret = r&SQL_TXN_READ_COMMITTED;
            break;
        case TRANSACTION_REPEATABLE_READ:
            ret = r&SQL_TXN_REPEATABLE_READ;
            break;
        case TRANSACTION_SERIALIZABLE:
            ret=(r&SQL_TXN_SERIALIZABLE)
        #if defined(SQL_TXN_VERSIONING)
              || (r&SQL_TXN_VERSIONING)
        #endif
              ;
            break;
    }
    lua_pushboolean(L, (ret!=0)?1:0);
    assert(1 == (lua_gettop(L) - top));
    return 1;
}


DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenCursorsAcrossCommit,
    SQL_CURSOR_COMMIT_BEHAVIOR, SQL_CB_PRESERVE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenStatementsAcrossCommit,
    SQL_CURSOR_COMMIT_BEHAVIOR, SQL_CB_PRESERVE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenCursorsAcrossRollback,
    SQL_CURSOR_ROLLBACK_BEHAVIOR, SQL_CB_PRESERVE
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsOpenStatementsAcrossRollback,
    SQL_CURSOR_ROLLBACK_BEHAVIOR, SQL_CB_PRESERVE
)

#ifdef LUASQL_USE_DRIVERINFO

static int conn_supportsResultSetType(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER type = luaL_checkint(L, 2);
    int ret = conn_init_di_(L, conn);
    const drvinfo_data *di = conn->di;
    if(is_fail(L, ret)) return ret;

    switch(type) {
        case RS_TYPE_FORWARD_ONLY:
            lua_pushboolean(L, di_supports_forwardonly(di));
            break;

        case RS_TYPE_SCROLL_INSENSITIVE:
            lua_pushboolean(L, di_supports_static(di));
            break;

        case RS_TYPE_SCROLL_SENSITIVE:
            lua_pushboolean(L, di_supports_scrollsensitive(di));
            break;
        default:
            return luasql_faildirect(L, "Invalid ResultSet type");
    }
    return 1;
}


static int conn_supportsResultSetConcurrency(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER type = luaL_checkint(L, 2);
    SQLUINTEGER concurrency = luaL_checkint(L, 3);
    int ct = 0;
    int ret = conn_init_di_(L, conn);
    const drvinfo_data *di = conn->di;
    if(is_fail(L, ret)) return ret;

    switch(type) {
        case RS_TYPE_SCROLL_SENSITIVE:
            if(!di_supports_scrollsensitive(di)){
                lua_pushboolean(L, 0);
                return 1;
            }
            ct = di_getscrollsensitive(di);
            break;

        case RS_TYPE_SCROLL_INSENSITIVE:
            if(!di_supports_static(di)){
                lua_pushboolean(L, 0);
                return 1;
            }
            ct=SQL_CURSOR_STATIC;
            break;

        case RS_TYPE_FORWARD_ONLY:
            if(!di_supports_forwardonly(di)){
                lua_pushboolean(L, 0);
                return 1;
            }
            // forward only cursors are read-only by definition
            lua_pushboolean(L, (concurrency == RS_CONCUR_READ_ONLY)?1:0);
            return 1;

        default:
            return luasql_faildirect(L, "Invalid ResultSet type");
    }

    switch(concurrency) {
        case RS_CONCUR_READ_ONLY:
            lua_pushboolean(L, di_supports_readonly(di, ct));
            break;
        case RS_CONCUR_UPDATABLE:
            lua_pushboolean(L, di_supports_updatable(di, ct));
            break;
        default:
            return luasql_faildirect(L, "Invalid ResultSet concurrency");
    }
    return 1;
}

#endif

DEFINE_GET_UINT16_AS_BOOL_INFO(nullPlusNonNullIsNull,
    SQL_CONCAT_NULL_BEHAVIOR, SQL_CB_NULL
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsColumnAliasing,
    SQL_COLUMN_ALIAS, 'Y'
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsAlterTableWithAddColumn,
    SQL_ALTER_TABLE, SQL_AT_ADD_COLUMN
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsAlterTableWithDropColumn,
    SQL_ALTER_TABLE, SQL_AT_DROP_COLUMN
)

DEFINE_GET_STRING_INFO(getExtraNameCharacters,
    SQL_SPECIAL_CHARACTERS
)

DEFINE_GET_STRING_INFO(getSearchStringEscape,
    SQL_SEARCH_PATTERN_ESCAPE
)

static int conn_getNumericFunctions(lua_State *L){
    static struct {
        SQLUINTEGER funcId;
        const char* funcName;
    } fmap[] = {
        { SQL_FN_NUM_ABS,      ("ABS") },
        { SQL_FN_NUM_ACOS,     ("ACOS") },
        { SQL_FN_NUM_ASIN,     ("ASIN") },
        { SQL_FN_NUM_ATAN,     ("ATAN") },
        { SQL_FN_NUM_ATAN2,    ("ATAN2") },
        { SQL_FN_NUM_CEILING,  ("CEILING") },
        { SQL_FN_NUM_COS,      ("COS") },
        { SQL_FN_NUM_COT,      ("COT") },
        { SQL_FN_NUM_DEGREES,  ("DEGREES") },
        { SQL_FN_NUM_EXP,      ("EXP") },
        { SQL_FN_NUM_FLOOR,    ("FLOOR") },
        { SQL_FN_NUM_LOG,      ("LOG") },
        { SQL_FN_NUM_LOG10,    ("LOG10") },
        { SQL_FN_NUM_MOD,      ("MOD") },
        { SQL_FN_NUM_PI,       ("PI") },
        { SQL_FN_NUM_POWER,    ("POWER") },
        { SQL_FN_NUM_RADIANS,  ("RADIANS") },
        { SQL_FN_NUM_RAND,     ("RAND") },
        { SQL_FN_NUM_ROUND,    ("ROUND") },
        { SQL_FN_NUM_SIGN,     ("SIGN") },
        { SQL_FN_NUM_SIN,      ("SIN") },
        { SQL_FN_NUM_SQRT,     ("SQRT") },
        { SQL_FN_NUM_TAN,      ("TAN") },
        { SQL_FN_NUM_TRUNCATE, ("TRUNCATE") },

#if LUASQL_ODBCVER >= 0x0300 
        { SQL_SNVF_BIT_LENGTH,       ("BIT_LENGTH") },
        { SQL_SNVF_CHAR_LENGTH,      ("CHAR_LENGTH") },
        { SQL_SNVF_CHARACTER_LENGTH, ("CHARACTER_LENGTH") },
        { SQL_SNVF_EXTRACT,          ("EXTRACT") },
        { SQL_SNVF_OCTET_LENGTH,     ("OCTET_LENGTH") },
        { SQL_SNVF_POSITION,         ("POSITION") },
#endif  /* ODBCVER >= 0x0300 */

        { 0, NULL }
    };
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER r;
    int i = 0;
    int n = 0;
    int top = lua_gettop(L);

    GET_UINT32_INFO(r, SQL_NUMERIC_FUNCTIONS);
    lua_newtable(L);
    for(; fmap[i].funcId > 0; i++){
        if(r & (fmap[i].funcId)){
            lua_pushstring(L, fmap[i].funcName);
            lua_rawseti(L,-2,++n);
        }
    }
    assert(1 == (lua_gettop(L) - top));
    return 1;
}

static int conn_getStringFunctions(lua_State *L){
    static struct {
        SQLUINTEGER funcId;
        const char* funcName;
    } fmap[] = {
    #if LUASQL_ODBCVER>=0x0300
        { SQL_FN_STR_BIT_LENGTH,       ("BIT_LENGTH") },
        { SQL_FN_STR_CHAR_LENGTH,      ("CHAR_LENGTH") },
        { SQL_FN_STR_CHARACTER_LENGTH, ("CHARACTER_LENGTH") },
        { SQL_FN_STR_OCTET_LENGTH,     ("OCTET_LENGTH") },
        { SQL_FN_STR_POSITION,         ("POSITION") },
    #endif
        { SQL_FN_STR_ASCII,            ("ASCII") },
        { SQL_FN_STR_CHAR,             ("CHAR") },
        { SQL_FN_STR_CONCAT,           ("CONCAT") },
        { SQL_FN_STR_DIFFERENCE,       ("DIFFERENCE") },
        { SQL_FN_STR_INSERT,           ("INSERT") },
        { SQL_FN_STR_LCASE,            ("LCASE") },
        { SQL_FN_STR_LEFT,             ("LEFT") },
        { SQL_FN_STR_LENGTH,           ("LENGTH") },
        { SQL_FN_STR_LOCATE,           ("LOCATE") },
        { SQL_FN_STR_LOCATE_2,         ("LOCATE_2") },
        { SQL_FN_STR_LTRIM,            ("LTRIM") },
        { SQL_FN_STR_REPEAT,           ("REPEAT") },
        { SQL_FN_STR_REPLACE,          ("REPLACE") },
        { SQL_FN_STR_RIGHT,            ("RIGHT") },
        { SQL_FN_STR_RTRIM,            ("RTRIM") },
        { SQL_FN_STR_SOUNDEX,          ("SOUNDEX") },
        { SQL_FN_STR_SPACE,            ("SPACE") },
        { SQL_FN_STR_SUBSTRING,        ("SUBSTRING") },
        { SQL_FN_STR_UCASE,            ("UCASE") },
        { 0, NULL }
    };
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER r;
    int i = 0;
    int n = 0;
    int top = lua_gettop(L);
    GET_UINT32_INFO(r, SQL_STRING_FUNCTIONS);
    lua_newtable(L);

    for(; fmap[i].funcId > 0; i++){
        if(r & (fmap[i].funcId)){
            lua_pushstring(L, fmap[i].funcName);
            lua_rawseti(L,-2,++n);
        }
    }
    assert(1 == (lua_gettop(L) - top));
    return 1;
}

static int conn_getSystemFunctions(lua_State *L){
    static struct {
        SQLUINTEGER funcId;
        const char* funcName;
    } fmap[] = {
        { SQL_FN_SYS_DBNAME,   ("DBNAME") },
        { SQL_FN_SYS_IFNULL,   ("IFNULL") },
        { SQL_FN_SYS_USERNAME, ("USERNAME") },
        { 0, NULL }
    };
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER r;
    int i = 0;
    int n = 0;
    int top = lua_gettop(L);
    GET_UINT32_INFO(r, SQL_SYSTEM_FUNCTIONS);
    lua_newtable(L);

    for(; fmap[i].funcId > 0; i++){
        if(r & (fmap[i].funcId)){
            lua_pushstring(L, fmap[i].funcName);
            lua_rawseti(L,-2,++n);
        }
    }
    assert(1 == (lua_gettop(L) - top));
    return 1;
}

static int conn_getTimeDateFunctions(lua_State *L){
    static struct {
        SQLUINTEGER funcId;
        const char* funcName;
    } fmap[] = {
    #if LUASQL_ODBCVER>=0x0300
        { SQL_FN_TD_CURRENT_DATE,      ("CURRENT_DATE") },
        { SQL_FN_TD_CURRENT_TIME,      ("CURRENT_TIME") },
        { SQL_FN_TD_CURRENT_TIMESTAMP, ("CURRENT_TIMESTAMP") },
        { SQL_FN_TD_EXTRACT,           ("EXTRACT") },
    #endif
        { SQL_FN_TD_CURDATE,           ("CURDATE") },
        { SQL_FN_TD_CURTIME,           ("CURTIME") },
        { SQL_FN_TD_DAYNAME,           ("DAYNAME") },
        { SQL_FN_TD_DAYOFMONTH,        ("DAYOFMONTH") },
        { SQL_FN_TD_DAYOFWEEK,         ("DAYOFWEEK") },
        { SQL_FN_TD_DAYOFYEAR,         ("DAYOFYEAR") },
        { SQL_FN_TD_HOUR,              ("HOUR") },
        { SQL_FN_TD_MINUTE,            ("MINUTE") },
        { SQL_FN_TD_MONTH,             ("MONTH") },
        { SQL_FN_TD_MONTHNAME,         ("MONTHNAME") },
        { SQL_FN_TD_NOW,               ("NOW") },
        { SQL_FN_TD_QUARTER,           ("QUARTER") },
        { SQL_FN_TD_SECOND,            ("SECOND") },
        { SQL_FN_TD_TIMESTAMPADD,      ("TIMESTAMPADD") },
        { SQL_FN_TD_TIMESTAMPDIFF,     ("TIMESTAMPDIFF") },
        { SQL_FN_TD_WEEK,              ("WEEK") },
        { SQL_FN_TD_YEAR,              ("YEAR") },
        { 0, NULL }
    };
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER r;
    int i = 0;
    int n = 0;
    int top = lua_gettop(L);

    GET_UINT32_INFO(r, SQL_TIMEDATE_FUNCTIONS);
    lua_newtable(L);

    for(; fmap[i].funcId > 0; i++){
        if(r & (fmap[i].funcId)){
            lua_pushstring(L, fmap[i].funcName);
            lua_rawseti(L,-2,++n);
        }
    }
    assert(1 == (lua_gettop(L) - top));
    return 1;
}

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInDataManipulation,
    LUASQL_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE), 
    LUASQL_ODBC3_C(SQL_CU_DML_STATEMENTS, SQL_QU_DML_STATEMENTS)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInProcedureCalls,
    LUASQL_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE), 
    LUASQL_ODBC3_C(SQL_CU_PROCEDURE_INVOCATION,SQL_QU_PROCEDURE_INVOCATION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInTableDefinitions,
    LUASQL_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE), 
    LUASQL_ODBC3_C(SQL_CU_TABLE_DEFINITION,SQL_QU_TABLE_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInIndexDefinitions,
    LUASQL_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE), 
    LUASQL_ODBC3_C(SQL_CU_INDEX_DEFINITION,SQL_QU_INDEX_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCatalogsInPrivilegeDefinitions,
    LUASQL_ODBC3_C(SQL_CATALOG_USAGE,SQL_QUALIFIER_USAGE), 
    LUASQL_ODBC3_C(SQL_CU_PRIVILEGE_DEFINITION,SQL_QU_PRIVILEGE_DEFINITION)
)


DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInDataManipulation,
    LUASQL_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE), 
    LUASQL_ODBC3_C(SQL_SU_DML_STATEMENTS,SQL_OU_DML_STATEMENTS)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInProcedureCalls,
    LUASQL_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE), 
    LUASQL_ODBC3_C(SQL_SU_PROCEDURE_INVOCATION,SQL_OU_PROCEDURE_INVOCATION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInTableDefinitions,
    LUASQL_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE), 
    LUASQL_ODBC3_C(SQL_SU_TABLE_DEFINITION,SQL_OU_TABLE_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInIndexDefinitions,
    LUASQL_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE), 
    LUASQL_ODBC3_C(SQL_SU_INDEX_DEFINITION,SQL_OU_INDEX_DEFINITION)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSchemasInPrivilegeDefinitions,
    LUASQL_ODBC3_C(SQL_SCHEMA_USAGE,SQL_OWNER_USAGE), 
    LUASQL_ODBC3_C(SQL_SU_PRIVILEGE_DEFINITION, SQL_OU_PRIVILEGE_DEFINITION)
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsGroupBy,
  SQL_GROUP_BY, SQL_GB_NOT_SUPPORTED
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsGroupByUnrelated,
  SQL_GROUP_BY, SQL_GB_NO_RELATION
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsGroupByBeyondSelect,
  SQL_GROUP_BY, SQL_GB_GROUP_BY_CONTAINS_SELECT
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsUnion,
    SQL_UNION, SQL_U_UNION
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsUnionAll,
    SQL_UNION, (SQL_U_UNION | SQL_U_UNION_ALL)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsOuterJoins,
    SQL_OJ_CAPABILITIES,
    (SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsFullOuterJoins,
    SQL_OJ_CAPABILITIES,
    (SQL_OJ_FULL | SQL_OJ_NESTED)
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsLimitedOuterJoins,
    SQL_OJ_CAPABILITIES,
    (SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED) | (SQL_OJ_FULL | SQL_OJ_NESTED)
)


DEFINE_GET_UINT16_AS_BOOL_INFO(usesLocalFilePerTable,
    SQL_FILE_USAGE, SQL_FILE_TABLE
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(usesLocalFiles,
    SQL_FILE_USAGE, SQL_FILE_NOT_SUPPORTED
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedHigh,
    SQL_NULL_COLLATION, SQL_NC_HIGH
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedLow,
    SQL_NULL_COLLATION, SQL_NC_LOW
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedAtStart,
    SQL_NULL_COLLATION, SQL_NC_START
)

DEFINE_GET_UINT16_AS_BOOL_INFO(nullsAreSortedAtEnd,
    SQL_NULL_COLLATION, SQL_NC_END
)

DEFINE_GET_CHAR_AS_BOOL_INFO(allProceduresAreCallable,
    SQL_ACCESSIBLE_PROCEDURES, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(allTablesAreSelectable,
    SQL_ACCESSIBLE_TABLES, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(isReadOnly,
    SQL_DATA_SOURCE_READ_ONLY, 'Y'
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsTableCorrelationNames,
    SQL_CORRELATION_NAME, SQL_CN_NONE
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsCorrelatedSubqueries,
    SQL_SUBQUERIES, SQL_SQ_CORRELATED_SUBQUERIES
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInComparisons,
    SQL_SUBQUERIES, SQL_SQ_COMPARISON
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInExists,
    SQL_SUBQUERIES, SQL_SQ_EXISTS
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInIns,
    SQL_SUBQUERIES, SQL_SQ_IN
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsSubqueriesInQuantifieds,
    SQL_SUBQUERIES, SQL_SQ_QUANTIFIED
)



DEFINE_GET_CHAR_AS_BOOL_INFO(supportsExpressionsInOrderBy,
    SQL_EXPRESSIONS_IN_ORDERBY, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsLikeEscapeClause,
    SQL_LIKE_ESCAPE_CLAUSE, 'Y'
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsMultipleResultSets,
    SQL_MULT_RESULT_SETS, 'Y'
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsNonNullableColumns,
    SQL_NON_NULLABLE_COLUMNS, SQL_NNC_NON_NULL
)

static int conn_supportsMinimumSQLGrammar(lua_State *L){
    lua_pushboolean(L, 1);
    return 1;
}

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsCoreSQLGrammar,
    SQL_ODBC_SQL_CONFORMANCE, SQL_OSC_MINIMUM
)

DEFINE_GET_UINT16_AS_BOOL_INFO(supportsExtendedSQLGrammar,
    SQL_ODBC_SQL_CONFORMANCE, SQL_OSC_EXTENDED
)

#if LUASQL_ODBCVER >= 3

DEFINE_GET_UINT32_AS_MASK_INFO(supportsANSI92EntryLevelSQL,
    SQL_SQL_CONFORMANCE, SQL_SC_SQL92_ENTRY
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsANSI92FullSQL,
    SQL_SQL_CONFORMANCE, SQL_SC_SQL92_FULL
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsANSI92IntermediateSQL,
    SQL_SQL_CONFORMANCE, SQL_SC_SQL92_INTERMEDIATE
)

#else

static int conn_supportsANSI92EntryLevelSQL(lua_State *L){
    lua_pushboolean(L, 0);
    return 1;
}

static int conn_supportsANSI92FullSQL(lua_State *L){
    lua_pushboolean(L, 0);
    return 1;
}

static int conn_supportsANSI92IntermediateSQL(lua_State *L){
    lua_pushboolean(L, 0);
    return 1;
}

#endif



DEFINE_GET_UINT32_INFO(getMaxBinaryLiteralLength,SQL_MAX_BINARY_LITERAL_LEN)
DEFINE_GET_UINT32_INFO(getMaxCharLiteralLength,SQL_MAX_CHAR_LITERAL_LEN)
DEFINE_GET_UINT16_INFO(getMaxColumnNameLength,SQL_MAX_COLUMN_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxColumnsInGroupBy,SQL_MAX_COLUMNS_IN_GROUP_BY)
DEFINE_GET_UINT16_INFO(getMaxColumnsInIndex,SQL_MAX_COLUMNS_IN_INDEX)
DEFINE_GET_UINT16_INFO(getMaxColumnsInOrderBy,SQL_MAX_COLUMNS_IN_ORDER_BY)
DEFINE_GET_UINT16_INFO(getMaxColumnsInSelect,SQL_MAX_COLUMNS_IN_SELECT)
DEFINE_GET_UINT16_INFO(getMaxColumnsInTable,SQL_MAX_COLUMNS_IN_TABLE)
DEFINE_GET_UINT16_INFO(getMaxCursorNameLength,SQL_MAX_CURSOR_NAME_LEN)
DEFINE_GET_UINT32_INFO(getMaxIndexLength,SQL_MAX_INDEX_SIZE)
DEFINE_GET_UINT16_INFO(getMaxSchemaNameLength,LUASQL_ODBC3_C(SQL_MAX_SCHEMA_NAME_LEN,SQL_MAX_OWNER_NAME_LEN))
DEFINE_GET_UINT16_INFO(getMaxProcedureNameLength,SQL_MAX_PROCEDURE_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxCatalogNameLength,LUASQL_ODBC3_C(SQL_MAX_CATALOG_NAME_LEN,SQL_MAX_QUALIFIER_NAME_LEN))
DEFINE_GET_UINT32_INFO(getMaxRowSize,SQL_MAX_ROW_SIZE)
DEFINE_GET_UINT32_INFO(getMaxStatementLength,SQL_MAX_STATEMENT_LEN)
DEFINE_GET_UINT16_INFO(getMaxTableNameLength,SQL_MAX_TABLE_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxTablesInSelect,SQL_MAX_TABLES_IN_SELECT)
DEFINE_GET_UINT16_INFO(getMaxUserNameLength,SQL_MAX_USER_NAME_LEN)
DEFINE_GET_UINT16_INFO(getMaxConnections,LUASQL_ODBC3_C(SQL_MAX_DRIVER_CONNECTIONS,SQL_ACTIVE_CONNECTIONS))
DEFINE_GET_UINT16_INFO(getMaxStatements,LUASQL_ODBC3_C(SQL_MAX_CONCURRENT_ACTIVITIES,SQL_ACTIVE_STATEMENTS))
DEFINE_GET_CHAR_AS_BOOL_INFO(doesMaxRowSizeIncludeBlobs,
    SQL_MAX_ROW_SIZE_INCLUDES_LONG, 'Y'
)



DEFINE_GET_CHAR_AS_BOOL_INFO(supportsMultipleTransactions,
    SQL_MULTIPLE_ACTIVE_TXN, 'Y'
)

DEFINE_GET_CHAR_AS_NOTBOOL_INFO(supportsOrderByUnrelated,
    SQL_ORDER_BY_COLUMNS_IN_SELECT, 'Y'
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsDifferentTableCorrelationNames,
    SQL_CORRELATION_NAME, SQL_CN_DIFFERENT
)

DEFINE_GET_UINT32_AS_MASK_INFO(supportsConvertFn,
    SQL_CONVERT_FUNCTIONS, SQL_FN_CVT_CONVERT
)

static int conn_supportsConvert(lua_State *L){
    conn_data *conn = (conn_data *) getconnection (L);
    SQLUINTEGER fromType = luaL_checkint(L, 2);
    SQLUINTEGER toType   = luaL_checkint(L, 3);
    int i = 0, j = 0;

    static struct {
        int id;
        int value;
      } convertMap[TYPES_COUNT] = {
        { TYPE_BIGINT,        SQL_CONVERT_BIGINT },
        { TYPE_BINARY,        SQL_CONVERT_BINARY },
        { TYPE_BIT,           SQL_CONVERT_BIT },
        { TYPE_CHAR,          SQL_CONVERT_CHAR },
        { TYPE_DATE,          SQL_CONVERT_DATE },
        { TYPE_DECIMAL,       SQL_CONVERT_DECIMAL },
        { TYPE_DOUBLE,        SQL_CONVERT_DOUBLE },
        { TYPE_FLOAT,         SQL_CONVERT_FLOAT },
        { TYPE_INTEGER,       SQL_CONVERT_INTEGER },
        { TYPE_LONGVARBINARY, SQL_CONVERT_LONGVARBINARY },
        { TYPE_LONGVARCHAR,   SQL_CONVERT_LONGVARCHAR },
        { TYPE_NUMERIC,       SQL_CONVERT_NUMERIC },
        { TYPE_REAL,          SQL_CONVERT_REAL },
        { TYPE_SMALLINT,      SQL_CONVERT_SMALLINT },
        { TYPE_TIME,          SQL_CONVERT_TIME },
        { TYPE_TIMESTAMP,     SQL_CONVERT_TIMESTAMP },
        { TYPE_TINYINT,       SQL_CONVERT_TINYINT },
        { TYPE_VARBINARY,     SQL_CONVERT_VARBINARY },
        { TYPE_VARCHAR,       SQL_CONVERT_VARCHAR }
#if (LUASQL_ODBCVER >= 0x0300)
        ,{ TYPE_WCHAR,         SQL_CONVERT_WCHAR }
        ,{ TYPE_WLONGVARCHAR,  SQL_CONVERT_WLONGVARCHAR }
        ,{ TYPE_WVARCHAR,      SQL_CONVERT_WVARCHAR }
        ,{ TYPE_GUID,          SQL_CONVERT_GUID }
#endif
    };
    static struct {
        int id;
        int value;
    } cvtMap[TYPES_COUNT] = {
        { TYPE_BIGINT,        SQL_CVT_BIGINT },
        { TYPE_BINARY,        SQL_CVT_BINARY },
        { TYPE_BIT,           SQL_CVT_BIT },
        { TYPE_CHAR,          SQL_CVT_CHAR },
        { TYPE_DATE,          SQL_CVT_DATE },
        { TYPE_DECIMAL,       SQL_CVT_DECIMAL },
        { TYPE_DOUBLE,        SQL_CVT_DOUBLE },
        { TYPE_FLOAT,         SQL_CVT_FLOAT },
        { TYPE_INTEGER,       SQL_CVT_INTEGER },
        { TYPE_LONGVARBINARY, SQL_CVT_LONGVARBINARY },
        { TYPE_LONGVARCHAR,   SQL_CVT_LONGVARCHAR },
        { TYPE_NUMERIC,       SQL_CVT_NUMERIC },
        { TYPE_REAL,          SQL_CVT_REAL },
        { TYPE_SMALLINT,      SQL_CVT_SMALLINT },
        { TYPE_TIME,          SQL_CVT_TIME },
        { TYPE_TIMESTAMP,     SQL_CVT_TIMESTAMP },
        { TYPE_TINYINT,       SQL_CVT_TINYINT },
        { TYPE_VARBINARY,     SQL_CVT_VARBINARY },
        { TYPE_VARCHAR,       SQL_CVT_VARCHAR }
#if (LUASQL_ODBCVER >= 0x0300)
        ,{ TYPE_WCHAR,         SQL_CVT_WCHAR }
        ,{ TYPE_WLONGVARCHAR,  SQL_CVT_WLONGVARCHAR }
        ,{ TYPE_WVARCHAR,      SQL_CVT_WVARCHAR }
        ,{ TYPE_GUID,          SQL_CVT_GUID }
#endif                
    };

    for(;i<TYPES_COUNT; i++){
        if(convertMap[i].id == fromType){
            for(;j<TYPES_COUNT; j++){
                if(cvtMap[j].id == toType){
                    SQLUINTEGER ret;
                    GET_UINT32_INFO(ret, ((convertMap[i].value) & (cvtMap[i].value)));
                    lua_pushboolean(L, (ret!=0)?1:0);
                    return 1;
                }
            }
            return luasql_faildirect(L, "Unknown toType");
        }
    }
    return luasql_faildirect(L, "Unknown fromType");
}

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesLowerCaseIdentifiers,
    SQL_IDENTIFIER_CASE, SQL_IC_LOWER
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesLowerCaseQuotedIdentifiers,
    SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_LOWER
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesMixedCaseIdentifiers,
    SQL_IDENTIFIER_CASE, SQL_IC_MIXED
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesMixedCaseQuotedIdentifiers,
    SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_MIXED
)


DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesUpperCaseIdentifiers,
    SQL_IDENTIFIER_CASE, SQL_IC_UPPER
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(storesUpperCaseQuotedIdentifiers,
    SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_UPPER
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsMixedCaseIdentifiers,
    SQL_IDENTIFIER_CASE, SQL_IC_SENSITIVE
)

DEFINE_GET_UINT16_AS_NOTBOOL_INFO(supportsMixedCaseQuotedIdentifiers,
    SQL_QUOTED_IDENTIFIER_CASE, SQL_IC_SENSITIVE
)

DEFINE_GET_CHAR_AS_BOOL_INFO(supportsStoredProcedures,
    SQL_PROCEDURES, 'Y'
)

#ifdef LUASQL_USE_DRIVERINFO

static int conn_ownXXXAreVisible_(lua_State *L, conn_data *conn, int type, int what){
    int odbcType;
    int ret = conn_init_di_(L, conn);
    const drvinfo_data *di = conn->di;
    if(is_fail(L, ret)) return ret;

    if(!IS_VALID_RS_TYPE(type)){
        return luasql_faildirect(L, "Invalid ResultSet type");
    }
    odbcType = getODBCCursorTypeFor(type, conn->di);

    #if LUASQL_ODBCVER >= 0x0300
    if(conn->di->majorVersion_ > 2){
        SQLUINTEGER r;
        GET_UINT32_INFO(r,(getCursorAttributes2For(odbcType)));
        switch(what) {
            case UPDATES:
                lua_pushboolean(L, (r&SQL_CA2_SENSITIVITY_UPDATES)?1:0);
                return 1;
            case INSERTS:
                lua_pushboolean(L, (r&SQL_CA2_SENSITIVITY_ADDITIONS)?1:0);
                return 1;
            case DELETES:
                lua_pushboolean(L, (r&SQL_CA2_SENSITIVITY_DELETIONS)?1:0);
                return 1;
        }
        //notreached
        assert(0);
    }
    #endif

    // for odbc 2, assume false for forward only, true for dynamic
    // and check for static and keyset driven
    switch(odbcType) {
        case SQL_CURSOR_FORWARD_ONLY:
            lua_pushboolean(L,0);
            return 1;
        case SQL_CURSOR_DYNAMIC:
            lua_pushboolean(L,1);
            return 1;
        case SQL_CURSOR_KEYSET_DRIVEN:
        case SQL_CURSOR_STATIC:{
            SQLUINTEGER r;
            GET_UINT32_INFO(r,SQL_STATIC_SENSITIVITY);
            switch(what) {
                case UPDATES:
                    lua_pushboolean(L, (r&SQL_SS_UPDATES)?1:0);
                    return 1;
                case INSERTS:
                    lua_pushboolean(L, (r&SQL_SS_ADDITIONS)?1:0);
                    return 1;
                case DELETES:
                    lua_pushboolean(L, (r&SQL_SS_DELETIONS)?1:0);
                    return 1;
            }
        }
    }
    // notreached
    assert(0);
    return 0;
}

static int conn_ownUpdatesAreVisible(lua_State *L){
  conn_data *conn = (conn_data *) getconnection (L);
  int type = luaL_checkint(L, 2);
  return conn_ownXXXAreVisible_(L, conn, type, UPDATES);
}

static int conn_ownDeletesAreVisible(lua_State *L){
  conn_data *conn = (conn_data *) getconnection (L);
  int type = luaL_checkint(L, 2);
  return conn_ownXXXAreVisible_(L,conn,type,DELETES);
}

static int conn_ownInsertsAreVisible(lua_State *L){
  conn_data *conn = (conn_data *) getconnection (L);
  int type = luaL_checkint(L, 2);
  return conn_ownXXXAreVisible_(L,conn,type,INSERTS);
}

// If I get this correct, the next 3 methods are only
// true when we're using a dynamic cursor

static int conn_othersUpdatesAreVisible(lua_State *L){
  conn_data *conn = (conn_data *) getconnection (L);
  int type = luaL_checkint(L, 2);
  int ct;
  int ret = conn_init_di_(L, conn);
  const drvinfo_data *di = conn->di;
  if(is_fail(L, ret)) return ret;

  if(!IS_VALID_RS_TYPE(type)){
    return luasql_faildirect(L, "Invalid ResultSet type");
  }
  ct = getODBCCursorTypeFor(type, conn->di);
  lua_pushboolean(L, (ct == SQL_CURSOR_DYNAMIC)?1:0);
  return 1;
}

#define conn_othersInsertsAreVisible conn_othersUpdatesAreVisible
#define conn_othersDeletesAreVisible conn_othersUpdatesAreVisible

static int conn_deletesAreDetected(lua_State *L){
  int type = luaL_checkint(L, 2);
  if(type == RS_TYPE_FORWARD_ONLY){
    lua_pushboolean(L, 0);
    return 1;
  }
  return conn_ownDeletesAreVisible(L);
}

static int conn_insertsAreDetected(lua_State *L){
  int type = luaL_checkint(L, 2);
  if(type == RS_TYPE_FORWARD_ONLY){
    lua_pushboolean(L, 0);
    return 1;
  }
  return conn_ownInsertsAreVisible(L);
}

#endif

static int conn_updatesAreDetected(lua_State *L){
    int type = luaL_checkint(L, 2);
    lua_pushboolean(L, (type!=RS_TYPE_FORWARD_ONLY)?1:0);
    return 1;
}


#undef DEFINE_GET_STRING_INFO
#undef DEFINE_GET_UINT16_AS_BOOL_INFO
#undef DEFINE_GET_UINT32_AS_MASK_INFO
#undef DEFINE_GET_UINT16_AS_NOTBOOL_INFO
#undef DEFINE_GET_CHAR_AS_BOOL_INFO
#undef DEFINE_GET_CHAR_AS_NOTBOOL_INFO
#undef GET_UINT32_INFO
#undef GET_UINT16_INFO
#undef DEFINE_GET_UINT32_INFO
#undef DEFINE_GET_UINT16_INFO

//}

//}----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Environment
//{----------------------------------------------------------------------------

//{ get / set attr

static int env_get_uint_attr_(lua_State*L, env_data *env, SQLINTEGER optnum){
    return get_uint_attr_(L, hENV, env->henv, optnum);
}

static int env_get_str_attr_(lua_State*L, env_data *env, SQLINTEGER optnum){
    return get_str_attr_(L, hENV, env->henv, optnum);
}

static int env_set_uint_attr_(lua_State*L, env_data *env, SQLINTEGER optnum, SQLUINTEGER value){
    return set_uint_attr_(L, hENV, env->henv, optnum, value);
}

static int env_set_str_attr_(lua_State*L, env_data *env, SQLINTEGER optnum, 
    const char* value, size_t len)
{
    return set_str_attr_(L, hENV, env->henv, optnum, value, len);
}

static int env_get_uint_attr(lua_State*L){
    env_data *env = (env_data *) getenvironment (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    return env_get_uint_attr_(L, env, optnum);
}

static int env_get_str_attr(lua_State*L){
    env_data *env = (env_data *) getenvironment (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    return env_get_str_attr_(L, env, optnum);
}

static int env_set_uint_attr(lua_State*L){
    env_data *env = (env_data *) getenvironment (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    return env_set_uint_attr_(L, env, optnum,luaL_checkinteger(L,3));
}

static int env_set_str_attr(lua_State*L){
    env_data *env = (env_data *) getenvironment (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    size_t len;
    const char *str = luaL_checklstring(L,3,&len);
    return env_set_str_attr_(L, env, optnum, str, len);
}

static int env_setlogintimeout(lua_State *L) {
  env_data *env = (env_data *) getenvironment (L);
  env->login_timeout = luaL_checkint(L, 2);
  return pass(L);
}

static int env_getlogintimeout(lua_State *L){
  env_data *env = (env_data *) getenvironment (L);
  lua_pushnumber(L, env->login_timeout);
  return 1;
}

static int env_setv2(lua_State *L){
  env_data *env = (env_data *) getenvironment (L);
  return env_set_uint_attr_(L, env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC2);
}

static int env_setv3(lua_State *L){
  env_data *env = (env_data *) getenvironment (L);
  return env_set_uint_attr_(L, env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3);
}

//}

//{ ctor/dtor/connect

static int env_connection (lua_State *L) {
    env_data *env = (env_data *) getenvironment (L);
    SQLHDBC hdbc;
    SQLRETURN ret = SQLAllocHandle (hDBC, env->henv, &hdbc);
    if (error(ret)) return fail(L, hENV, env->henv);

    return conn_create(L, 1, env, hdbc);
}

/*
** Creates and returns a connection object
** Lua Input: source [, user [, pass]]
**   source: data source
**   user, pass: data source authentication information
** Lua Returns:
**   connection object if successfull
**   nil and error message otherwise.
*/
static int env_connect (lua_State *L) {
    env_data *env = (env_data *) getenvironment (L);
    const char *sourcename = luaL_checkstring (L, 2);
    const char *username = luaL_optstring (L, 3, NULL);
    const char *password = luaL_optstring (L, 4, NULL);
    SQLHDBC hdbc;
    SQLRETURN ret;

    /* tries to allocate connection handle */
    ret = SQLAllocHandle (hDBC, env->henv, &hdbc);
    if (error(ret))
        return fail(L, hENV, env->henv);

    if(env->login_timeout > -1){
        ret = SQLSetConnectAttr(hdbc,SQL_ATTR_LOGIN_TIMEOUT,(SQLPOINTER)env->login_timeout,SQL_IS_UINTEGER);
        if(error(ret)){
            ret = fail(L, hDBC, hdbc);
            SQLFreeHandle(hDBC, hdbc);
            return ret;
        }
    }

    /* tries to connect handle */
    ret = SQLConnect (hdbc, (char *) sourcename, SQL_NTS, 
        (char *) username, SQL_NTS, (char *) password, SQL_NTS);
    if (error(ret)) {
        ret = fail(L, hDBC, hdbc);
        SQLFreeHandle(hDBC, hdbc);
        return ret;
    }

    /* success, return connection object */
    ret = conn_create (L, 1, env, hdbc);
    if(!is_fail(L,ret)){
      assert(ret > 0);
      if(ret > 1)lua_pushvalue(L,-ret);
      lua_insert(L,1);
      /* if conn_after_connect raise error then we lost conn */
      return conn_after_connect(L);
    }
    return ret;
}

static int env_driverconnect(lua_State *L){
    const MIN_CONNECTSTRING_SIZE = 1024; // msdn say

    env_data *env = (env_data *) getenvironment (L);
    size_t connectStringSize;
    const char *connectString = lua_istable(L,2)?table_to_cnnstr(L,2),luaL_checklstring(L, 2, &connectStringSize):
                                                                      luaL_checklstring(L, 2, &connectStringSize);
    SQLUSMALLINT drvcompl = SQL_DRIVER_NOPROMPT;
    char *buf;
    SQLSMALLINT bufSize;
    SQLHDBC hdbc;
    SQLRETURN ret;
    if(lua_isnumber(L,3))
      drvcompl = luaL_checkinteger(L,3);

    buf = malloc(MIN_CONNECTSTRING_SIZE);
    if(!buf)
        return LUASQL_ALLOCATE_ERROR(L);

    /* tries to allocate connection handle */
    ret = SQLAllocHandle (hDBC, env->henv, &hdbc);
    if (error(ret)){
        free(buf);
        return luasql_faildirect (L, "connection allocation error.");
    }

    if(env->login_timeout > -1){
        ret = SQLSetConnectAttr(hdbc,SQL_ATTR_LOGIN_TIMEOUT,(SQLPOINTER)env->login_timeout,SQL_IS_UINTEGER);
        if(error(ret)){
            free(buf);
            ret = fail(L, hDBC, hdbc);
            SQLFreeHandle(hDBC, hdbc);
            return ret;
        }
    }

    ret = SQLDriverConnect(hdbc,0,(SQLPOINTER)connectString,connectStringSize,
                               buf,MIN_CONNECTSTRING_SIZE,&bufSize,drvcompl);
    if (error(ret)) {
        free(buf);
        ret = fail(L, hDBC, hdbc);
        SQLFreeHandle(hDBC, hdbc);
        return ret;
    }

    ret = conn_create (L, 1, env, hdbc);
    if(!is_fail(L,ret)){
      assert(ret > 0);
      if(ret > 1)lua_pushvalue(L,-ret);
      lua_insert(L,1);
      ret = conn_after_connect(L);
    }
    lua_pushstring(L,buf);
    free(buf);
    return ret+1;
} 

/*
** Closes an environment object
*/
static int env_close (lua_State *L) {
    SQLRETURN ret;
    env_data *env = (env_data *)luaL_checkudata(L, 1, LUASQL_ENVIRONMENT_ODBC);
    luaL_argcheck (L, env != NULL, 1, LUASQL_PREFIX"environment expected");
    if (env->closed) {
        lua_pushboolean (L, 0);
        return 1;
    }
    if (env->conn_counter > 0)
        return luaL_error (L, LUASQL_PREFIX"there are open connections");

    env->closed = 1;
    ret = SQLFreeHandle (hENV, env->henv);
    if (error(ret)) {
        int ret = fail(L, hENV, env->henv);
        env->henv = NULL;
        return ret;
    }
    return pass(L);
}

/*
** Creates an Environment and returns it.
*/
static int create_environment (lua_State *L) {
    env_data *env;
    SQLHENV henv;
    SQLRETURN ret = SQLAllocHandle(hENV, SQL_NULL_HANDLE, &henv);
    if (error(ret))
        return luasql_faildirect(L, "error creating environment.");

    env = (env_data *)lua_newuserdata (L, sizeof (env_data));
    luasql_setmeta (L, LUASQL_ENVIRONMENT_ODBC);
    /* fill in structure */
    env->closed = 0;
    env->conn_counter  = 0;
    env->login_timeout = -1;
    env->henv = henv;
    ret = env_set_uint_attr_(L, env, SQL_ATTR_ODBC_VERSION, 
#if LUASQL_ODBCVER >= 0x0300
        SQL_OV_ODBC3
#else
        SQL_OV_ODBC2
#endif
    );
    if(ret == 1){
      lua_pop(L,1);
      return 1;
    }
    return ret;
}

//}

//{ DSN and drivers info

static int env_getdatasources (lua_State *L) {
  env_data *env = (env_data *) getenvironment (L);
  SQLRETURN ret;
  SQLSMALLINT dsnlen,desclen;
  char dsn[SQL_MAX_DSN_LENGTH+1];
  char desc[256];
  int i = 1;
  int is_cb = lua_isfunction(L,2);
  int top = lua_gettop(L);

  ret = SQLDataSources(
      env->henv,SQL_FETCH_FIRST,
      (SQLPOINTER)dsn, SQL_MAX_DSN_LENGTH+1,&dsnlen,
      (SQLPOINTER)desc,256,&desclen
  );
  if(!is_cb) top++, lua_newtable(L);
  if(LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret) return is_cb ? 0 : 1;
  while(!error(ret)){
    assert(top == lua_gettop(L));
    if(is_cb) lua_pushvalue(L, 2);
    lua_newtable(L);
    lua_pushstring(L,dsn);  lua_rawseti(L,-2,1);
    lua_pushstring(L,desc); lua_rawseti(L,-2,2);
    if(!is_cb) lua_rawseti(L,-2,i++);
    else{
      int ret;
      lua_call(L,1,LUA_MULTRET);
      ret = lua_gettop(L) - top;
      assert(ret >= 0);
      if(ret) return ret;
    }

    ret = SQLDataSources(
        env->henv,SQL_FETCH_NEXT,
        (SQLPOINTER)dsn, SQL_MAX_DSN_LENGTH+1,&dsnlen,
        (SQLPOINTER)desc,256,&desclen
        );
    if(LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret) return is_cb ? 0 : 1;
  }
  return fail(L, hENV, env->henv);
}

#define MAX_DESC_LEN 64
#define MAX_ATTR_LEN 1024
static int env_getdrivers (lua_State *L) {
  env_data *env = (env_data *) getenvironment (L);
  SQLRETURN ret;
  SQLSMALLINT attrlen,desclen;
  int i = 1;
  char desc[MAX_DESC_LEN];
  char *attr = malloc(MAX_ATTR_LEN+1);
  int is_cb = lua_isfunction(L,2);
  int top = lua_gettop(L);

  if(!attr)
    return LUASQL_ALLOCATE_ERROR(L);

  ret = SQLDrivers(env->henv,SQL_FETCH_FIRST,
      (SQLPOINTER)desc,MAX_DESC_LEN,&desclen,
      (SQLPOINTER)attr,MAX_ATTR_LEN,&attrlen);
  if(!is_cb) top++,lua_newtable(L);
  if(LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret){
    free(attr);
    return is_cb ? 0 : 1;
  }
  while(!error(ret)){
    assert(top == lua_gettop(L));
    if(is_cb) lua_pushvalue(L, 2);
    lua_newtable(L);
    lua_pushstring(L,desc); lua_rawseti(L,-2,1);
    //find our attributes
    if(attr[0]!=0) {
      size_t i=0, last=0, n=1;
      lua_newtable(L);
      do {
        char *p,*a;
        while(attr[++i] != 0);
        a = &(attr[last]);
        p = strchr(a,'=');
        if(p){
          lua_pushlstring(L,a,p-a);
          lua_pushlstring(L,p + 1, (i-last)-(p-a)-1);
          lua_settable(L,-3);
        }
        else{
          lua_pushlstring(L,a,(i-last)); lua_rawseti(L,-2,n++);
        }
        last=i+1;
      } while(attr[last]!=0);
      lua_rawseti(L,-2,2);
    }
    if(!is_cb) lua_rawseti(L,-2,i++);
    else{
      int ret;
      lua_call(L,1,LUA_MULTRET);
      ret = lua_gettop(L) - top;
      assert(ret >= 0);
      if(ret){
        free(attr);
        return ret;
      }
    }

    ret = SQLDrivers(env->henv,SQL_FETCH_NEXT,
      (SQLPOINTER)desc,MAX_DESC_LEN,&desclen,
      (SQLPOINTER)attr,MAX_ATTR_LEN,&attrlen);

    if(LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND) == ret){
      free(attr);
      return is_cb ? 0 : 1;
    }
  }
  free(attr);
  return fail(L, hENV, env->henv);
}
#undef MAX_DESC_LEN
#undef MAX_ATTR_LEN

//}

//}----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Param list
//{----------------------------------------------------------------------------

static int par_data_setparinfo(par_data* par, lua_State *L, SQLHSTMT hstmt, SQLSMALLINT i){
  SQLRETURN ret = SQLDescribeParam(hstmt, i, &par->sqltype, &par->parsize, &par->digest, NULL);
  // Sybase ODBC driver 9.0.2.3951
  // if use in select/update and so on query - always return SQL_CHAR(32567)
  // so we do not allocate this buffer yet
  // if use CALL SOME_PROC(...)
  // for unsigned int i get -18(SQL_C_ULONG?) from SQLDescribeParam!!!
  // So it's not useful function

  if(error(ret)){
    par->sqltype = SQL_UNKNOWN_TYPE;
    return fail(L,hSTMT,hstmt);
  }
  else{
    if(!issqltype(par->sqltype)){ // SQL_C_ULONG for example
      par->sqltype = SQL_UNKNOWN_TYPE;
      par->parsize = par->digest = 0;
    }
    else{
      // and what now?
    }
  }

  return 0;
}

/* Create params
** only memory allocation error
*/
static int par_data_create_unknown(par_data** ptr, lua_State *L){
    par_data* par = (par_data*)malloc(sizeof(par_data));
    assert(*ptr == NULL);
    if(!par) return 1;
    *ptr = par;

    memset(par,0,sizeof(par_data));
    par->get_cb  = LUA_NOREF;
    par->sqltype = SQL_UNKNOWN_TYPE;
    return 0;
}

/*
** Ensure that in list at least n params
** do not throw error
*/
static int par_data_ensure_nth (par_data **par, lua_State *L, int n, par_data **res){
    assert(n > 0);
    *res = 0;
    if(!(*par)){
        int ret = par_data_create_unknown(par, L);
        if(ret){
            return ret;
        }
    }

    while(--n>0){
        par = &((*par)->next);
        if(!(*par)){
            int ret = par_data_create_unknown(par, L);
            if(ret){
                *res = *par;
                return ret;
            }
        }
    }
    *res = *par;
    return 0;
}

static void par_data_free(par_data* next, lua_State *L){
    while(next){
        const char *luatype = sqltypetolua(next->sqltype);
        par_data* p = next->next;
        luaL_unref (L, LUA_REGISTRYINDEX, next->get_cb);
        if(((luatype == LT_STRING)||(luatype == LT_BINARY))&&(next->value.strval.buf))
            free(next->value.strval.buf);
        free(next);
        next = p;
    }
}

/*
** assign new type to par_data.
** if there error while allocate memory then strval.buf will be NULL
*/
static void par_data_settype(par_data* par, SQLSMALLINT sqltype, SQLULEN parsize, SQLSMALLINT digest, SQLULEN bufsize){
    const char* oldtype = sqltypetolua(par->sqltype);
    const char* newtype = sqltypetolua(sqltype);
    const int old_isstring = (oldtype == LT_BINARY)||(oldtype == LT_STRING);
    const int new_isstring = (newtype == LT_BINARY)||(newtype == LT_STRING);
    par->parsize = parsize;
    par->digest  = digest;
    if(old_isstring){
        if(new_isstring){
            par->sqltype = sqltype;
            if(par->value.strval.bufsize < bufsize){
                par->value.strval.bufsize = bufsize > LUASQL_MIN_PAR_BUFSIZE?bufsize:LUASQL_MIN_PAR_BUFSIZE;
                if(par->value.strval.buf) par->value.strval.buf = realloc(par->value.strval.buf,par->value.strval.bufsize);
                else par->value.strval.buf = malloc(par->value.strval.bufsize);
                if(!par->value.strval.buf)
                    par->value.strval.bufsize = 0;
            }
            return;
        }
        if(par->value.strval.buf){
          free(par->value.strval.buf);
          par->value.strval.buf = NULL;
          par->value.strval.bufsize = 0;
        }
    }
    else{
        if(new_isstring){
            par->sqltype = sqltype;
            if(bufsize){
                par->value.strval.bufsize = bufsize > LUASQL_MIN_PAR_BUFSIZE?bufsize:LUASQL_MIN_PAR_BUFSIZE;
                par->value.strval.buf = malloc(par->value.strval.bufsize);
                if(!par->value.strval.buf)
                    par->value.strval.bufsize = 0;
            }
            else{
                par->value.strval.bufsize = 0;
                par->value.strval.buf = NULL;
            }
            return;
         }
    }
    par->sqltype = sqltype;
}

static par_data* par_data_nth(par_data* p, int n){
    while(p && --n>0){
        p = p->next;
    }
    return p;
}

static int par_init_cb(par_data *par, lua_State *L, SQLUSMALLINT sqltype){
    int data_len=0;
    assert(lua_isfunction(L,3));
    luaL_unref(L, LUA_REGISTRYINDEX, par->get_cb);
    lua_pushvalue(L,3);
    par->get_cb = luaL_ref(L, LUA_REGISTRYINDEX);
    if(LUA_REFNIL == par->get_cb)
        return luasql_faildirect(L, "can not register CallBack (expire resource).");
    // may be should check sqltype in (SQL_BINARY/SQL_CHAR)?
    if(lua_isnumber(L,4))
        data_len = luaL_checkint(L,4);
    par->parsize = data_len;
    par->ind = SQL_LEN_DATA_AT_EXEC(data_len);
    par_data_settype(par, sqltype, data_len, 0, 0);
    return 0;
}

static int par_call_cb(par_data *par, lua_State *L, int nparam){
  assert(LUA_NOREF != par->get_cb);
  lua_rawgeti(L, LUA_REGISTRYINDEX, par->get_cb);
  assert(lua_isfunction(L,-1));
  lua_insert(L,-(nparam+1));
  if(lua_pcall(L,nparam,1,0)){
    lua_pushnil(L);
    lua_insert(L,-2);
    return 2;
  }
  return 0;
}

//}----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Statement
//{----------------------------------------------------------------------------

//{ impl

static int create_parinfo(lua_State *L, stmt_data *stmt){
    par_data **par = &stmt->par;
    int top = lua_gettop(L);
    int i;
    for(i = 1; i <= stmt->numpars; i++){
        if(!(*par)){
            int ret = par_data_create_unknown(par, L);
            if(ret){
                par_data_free(stmt->par, L);
                stmt->par = NULL;
                if(!(*par))
                    return LUASQL_ALLOCATE_ERROR(L);
                return ret;
            }
        }
        par = &((*par)->next);
    }

    assert(top == lua_gettop(L));
    return 0;
}

/*
** Clear addition info about stmt.
*/
static void stmt_clear_info_ (lua_State *L, stmt_data *stmt){
    cur_data *cur = &stmt->cur;
    int top = lua_gettop(L);
    par_data *par = stmt->par;

    assert(cur->closed);

    luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);

    // dont need check error
    SQLFreeStmt(cur->hstmt, SQL_RESET_PARAMS);

    for(;par;par=par->next){
        luaL_unref(L, LUA_REGISTRYINDEX, par->get_cb);
        par->get_cb = LUA_NOREF;
    }

    if(cur->numcols) // to the futer
        SQLFreeStmt(cur->hstmt, SQL_UNBIND);

#ifdef LUASQL_FREE_PAR_AT_CLEAR
    par_data_free(stmt->par, L);
    stmt->par      = NULL;
#endif

    stmt->numpars  = -1;
    stmt->prepared = 0;
    stmt->resultsetno = 0;
    cur->colnames  = LUA_NOREF;
    cur->coltypes  = LUA_NOREF;
    cur->numcols   = 0;

    assert(top == lua_gettop(L));
}

/*
** Prepare a SQL statement.
** init params and cols
** return 0 if success
*/
static int stmt_prepare_ (lua_State *L, stmt_data *stmt, const char *statement){
    SQLHSTMT hstmt  = stmt->cur.hstmt;
    SQLSMALLINT numcols;
    SQLRETURN ret;

    if(!stmt->cur.closed)
        return luasql_faildirect(L, "can not prepare opened statement.");

    stmt_clear_info_(L, stmt);

    ret = SQLPrepare(hstmt, (char *) statement, SQL_NTS);
    if (error(ret))
        return fail(L, hSTMT, hstmt);
    stmt->prepared = 1;

    /* determine the number of results */
    ret = SQLNumResultCols (hstmt, &numcols);
    if (error(ret))
        return fail(L, hSTMT, hstmt);

    stmt->cur.numcols  = numcols;
    if(stmt->cur.numcols){
        create_colinfo(L, &stmt->cur);
    }

    stmt->numpars = -1;

    {conn_data *conn;
    lua_rawgeti(L, LUA_REGISTRYINDEX, stmt->cur.conn);
    conn = getconnection_at(L,-1);
    lua_pop(L,1);
    if(conn->supports[LUASQL_CONN_SUPPORT_NUMPARAMS]){
        SQLSMALLINT numpars;
        ret = SQLNumParams(hstmt, &numpars);
        if (error(ret)) return 0;

        stmt->numpars = numpars;
        if(stmt->numpars){
            ret = create_parinfo(L, stmt);
            if(ret){
                stmt_clear_info_(L, stmt);
                return ret;
            }
        }
    }}
    return 0;

#if 0
// use driver info

#ifdef LUASQL_USE_DRIVERINFO_SUPPORTED_FUNCTIONS
    {conn_data *conn;
    lua_rawgeti(L, LUA_REGISTRYINDEX, stmt->cur.conn);
    conn = getconnection_at(L,-1);
    {int ret = conn_init_di_(L, conn);
    if(ret)lua_pop(L, ret);}
    if(!conn->di){
        lua_pop(L,1); //conn
        return 0;
    }
    if(!di_supports_function(conn->di, SQL_API_SQLNUMPARAMS)){
        lua_pop(L,1); //conn
        return 0;
    }}

    {SQLSMALLINT numpars;
    ret = SQLNumParams(hstmt, &numpars);
    lua_pop(L,1); //conn
    if (error(ret))return 0;

    stmt->numpars = numpars;
    if(stmt->numpars){
        ret = create_parinfo(L, stmt);
        if(ret){
            stmt_clear_info_(L, stmt);
            return ret;
        }
    }}
#endif

    return 0;
#endif
}

/*
** Create stmt on top L and return it in pstmt
*/
static int stmt_create_ (lua_State *L, int o, conn_data *conn, stmt_data **pstmt){
    stmt_data *stmt;
    SQLHSTMT hstmt;
    int top = lua_gettop(L);
    SQLRETURN ret = SQLAllocHandle(hSTMT, conn->hdbc, &hstmt);

    if (error(ret))
        return fail(L, hDBC, conn->hdbc);

    stmt    = (stmt_data *) lua_newuserdata(L, sizeof(stmt_data));
    if(!stmt){
        SQLFreeHandle(hSTMT, hstmt); 
        return LUASQL_ALLOCATE_ERROR(L);
    }
    if(pstmt) 
      *pstmt = stmt;
    stmt->par          = NULL;
    stmt->prepared     = 0;
    stmt->numpars      = -1;
    stmt->destroyed    = 0;
    stmt->resultsetno  = 0;
    stmt->cur.closed   = 1;
    stmt->cur.hstmt    = hstmt;
    stmt->cur.numcols  = 0;
    stmt->cur.autoclose = 0;
    stmt->cur.conn     = LUA_NOREF;
    stmt->cur.colnames = LUA_NOREF;
    stmt->cur.coltypes = LUA_NOREF;

    lua_pushvalue (L, o);
    stmt->cur.conn = luaL_ref (L, LUA_REGISTRYINDEX);

    /* now we can call methods */
    luasql_setmeta (L, LUASQL_STMT_ODBC); 
    conn->cur_counter++;

    assert((top+1) == lua_gettop(L));
    return 1;
}

/*
** Do not throw error
*/
static void stmt_destroy_ (lua_State *L, stmt_data *stmt){
    cur_data *cur   = &stmt->cur;
    conn_data *conn;
    if(stmt->destroyed) return;

    if((cur->numcols > 0)&&(cur->closed == 0)){
        cur->closed = 1;
        SQLCloseCursor(cur->hstmt);
    }

    SQLFreeHandle(hSTMT, cur->hstmt);

    stmt->destroyed = 1;
    /* Decrement cursor counter on connection object */
    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->conn);
    conn = lua_touserdata (L, -1);
    conn->cur_counter--;

    luaL_unref (L, LUA_REGISTRYINDEX, cur->conn);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);

    par_data_free(stmt->par, L);
    stmt->par = NULL;
}

//}

//{ bind CallBack

static int stmt_bind_number_cb_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret = par_init_cb(par, L, LUASQL_NUMBER);
    if(ret)
        return ret;
    par_data_settype(par,LUASQL_NUMBER,LUASQL_NUMBER_SIZE, LUASQL_NUMBER_DIGEST, 0);
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, LUASQL_C_NUMBER, par->sqltype, par->parsize, par->digest, (VOID *)par, 0, &par->ind);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    return pass(L);
}

static int stmt_bind_bool_cb_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret = par_init_cb(par, L, SQL_BIT);
    if(ret)
        return ret;
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, SQL_C_BIT, par->sqltype, 0, 0, (VOID *)par, 0, &par->ind);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    return pass(L);
}

static int stmt_bind_string_cb_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret = par_init_cb(par, L, SQL_CHAR);
    if(ret)
        return ret;
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, SQL_C_CHAR, par->sqltype, 0, 0, (VOID *)par, 0, &par->ind);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    return pass(L);
}

static int stmt_bind_binary_cb_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret = par_init_cb(par, L, SQL_BINARY);
    if(ret)
        return ret;
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, SQL_C_BINARY, par->sqltype, 0, 0, (VOID *)par, 0, &par->ind);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    return pass(L);
}

//}

//{ bind Data

static int stmt_bind_number_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret;
    if(lua_isfunction(L,3))
      return stmt_bind_number_cb_(L,stmt,i,par);

    par_data_settype(par,LUASQL_NUMBER,LUASQL_NUMBER_SIZE, LUASQL_NUMBER_DIGEST, 0);
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, LUASQL_C_NUMBER, par->sqltype, par->parsize, par->digest, &par->value.numval, 0, NULL);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    par->value.numval = luaL_checknumber(L,3);
    return pass(L);
}

static int stmt_bind_string_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret;
    unsigned int buffer_len;
    if(lua_isfunction(L,3))
      return stmt_bind_string_cb_(L,stmt,i,par);

    buffer_len = lua_strlen(L,3)+1;

    par_data_settype(par,SQL_CHAR, 0, 0, buffer_len);
    if(!par->value.strval.buf)
        return LUASQL_ALLOCATE_ERROR(L);
    par->ind = SQL_NTS;
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, SQL_C_CHAR, par->sqltype, 0, 0, par->value.strval.buf, par->value.strval.bufsize, &par->ind);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);

    memcpy(par->value.strval.buf, lua_tostring(L, 3), buffer_len-1);
    ((char*)par->value.strval.buf)[buffer_len-1] = '\0';
    return pass(L);
}

static int stmt_bind_binary_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret;
    unsigned int buffer_len;
    if(lua_isfunction(L,3))
      return stmt_bind_binary_cb_(L,stmt,i,par);
    buffer_len = lua_strlen(L,3);
    par_data_settype(par,SQL_BINARY, 0, 0, buffer_len?buffer_len:1);
    if(!par->value.strval.buf)
        return LUASQL_ALLOCATE_ERROR(L);
    par->ind = buffer_len;
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, SQL_C_BINARY, par->sqltype, 0, 0, par->value.strval.buf, par->value.strval.bufsize, &par->ind);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);

    memcpy(par->value.strval.buf, lua_tostring(L, 3), buffer_len);
    return pass(L);
}

static int stmt_bind_bool_(lua_State *L, stmt_data *stmt, SQLUSMALLINT i, par_data *par){
    SQLRETURN ret;
    if(lua_isfunction(L,3))
      return stmt_bind_bool_cb_(L,stmt,i,par);
    par_data_settype(par,SQL_BIT, 0, 0, 0);
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, SQL_C_BIT, par->sqltype, 0, 0, &par->value.boolval, 0, NULL);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    par->value.boolval = lua_isboolean(L,3) ? lua_toboolean(L,3) : luaL_checkint(L,3);
    return pass(L);
}

//}

//{ bind lua interface

#define CHECK_BIND_PARAM() \
    stmt_data   *stmt = getstmt(L);\
    par_data     *par = NULL;\
    SQLUSMALLINT    i = luaL_checkint(L,2);\
    if(i <= 0)\
        return luasql_faildirect(L, "invalid param index");\
    if(stmt->numpars >= 0){\
        assert(stmt->prepared);\
        if(i > stmt->numpars)\
            return luasql_faildirect(L, "invalid param index");\
        par = par_data_nth(stmt->par, i);\
    }\
    else{\
        int ret = par_data_ensure_nth(&stmt->par, L, i, &par);\
        if(ret){\
            if(!par)\
                return LUASQL_ALLOCATE_ERROR(L);\
            return ret;\
        }\
    }\
    assert(par);\
    luaL_unref (L, LUA_REGISTRYINDEX, par->get_cb);\
    par->get_cb = LUA_NOREF;


static int stmt_bind_ind(lua_State *L, SQLINTEGER ind){
    SQLRETURN ret;
    CHECK_BIND_PARAM();

    par->ind = ind;
    ret = SQLBindParameter(stmt->cur.hstmt, i, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, par->value.strval.buf, 0, &par->ind);
    if (error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    return pass(L);
}

static int stmt_bind(lua_State *L){
    const char* type;
    CHECK_BIND_PARAM();
    type = sqltypetolua(par->sqltype);

    /* deal with data according to type */
    switch (type[1]) {
        /* nUmber */
        case 'u': return stmt_bind_number_(L,stmt,i,par);
        /* bOol */
        case 'o': return stmt_bind_bool_(L,stmt,i,par);
        /* sTring */ 
        case 't': return stmt_bind_string_(L,stmt,i,par);
        /* bInary */
        case 'i': return stmt_bind_binary_(L,stmt,i,par);
    }
    return luasql_faildirect(L,"unknown param type.");
}

static int stmt_bind_number(lua_State *L){
    CHECK_BIND_PARAM();
    return stmt_bind_number_(L,stmt,i,par);
}

static int stmt_bind_bool(lua_State *L){
    CHECK_BIND_PARAM();
    return stmt_bind_bool_(L,stmt,i,par);
}

static int stmt_bind_string(lua_State *L){
    CHECK_BIND_PARAM();
    return stmt_bind_string_(L,stmt,i,par);
}

static int stmt_bind_binary(lua_State *L){
    CHECK_BIND_PARAM();
    return stmt_bind_binary_(L,stmt,i,par);
}

static int stmt_bind_null(lua_State *L){
    return stmt_bind_ind(L,SQL_NULL_DATA);
}

static int stmt_bind_default(lua_State *L){
    return stmt_bind_ind(L,SQL_DEFAULT_PARAM);
}

#undef CHECK_BIND_PARAM
//}

//{ putparam for CallBack

static int stmt_putparam_binary_(lua_State *L, stmt_data *stmt, par_data *par){
    SQLRETURN ret;

    size_t lbytes = par->parsize;
    while((par->parsize==0)||(lbytes > 0)){
        int top = lua_gettop(L);
        const char *data = NULL;
        size_t data_len = 0;

        if(par->parsize){
            lua_pushnumber(L,lbytes);
            ret = par_call_cb(par,L,1); 
        }
        else{
            ret = par_call_cb(par,L,0);
        }

        if(ret)
            return ret;

        data = luaL_checklstring(L,-1,&data_len);
        if(!data_len){
            lua_settop(L,top);
            break;
        }
        if(par->parsize){
            if(lbytes < data_len)
                data_len = lbytes;
            lbytes -= data_len;
            assert(lbytes < par->parsize);// assert overflow
        }

        ret = SQLPutData(stmt->cur.hstmt, (SQLPOINTER*)data, data_len);// is it safe const cast?
        if(error(ret))
            return fail(L, hSTMT, stmt->cur.hstmt);
        lua_settop(L,top);
    }
    return 0;
}

static int stmt_putparam_number_(lua_State *L, stmt_data *stmt, par_data *par){
    SQLRETURN ret;
    int top = lua_gettop(L);
    if(ret = par_call_cb(par,L,0))
        return ret;
    par->value.numval = luaL_checknumber(L,-1);
    lua_settop(L,top);
    ret = SQLPutData(stmt->cur.hstmt, &par->value.numval, sizeof(par->value.numval));
    if(error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    return 0;
}

static int stmt_putparam_bool_(lua_State *L, stmt_data *stmt, par_data *par){
    SQLRETURN ret;
    int top = lua_gettop(L);
    if(ret = par_call_cb(par,L,0))
        return ret;
    par->value.boolval = lua_isboolean(L,-1) ? lua_toboolean(L,-1) : luaL_checkint(L,-1);
    lua_settop(L,top);
    ret = SQLPutData(stmt->cur.hstmt, &par->value.boolval, sizeof(par->value.boolval));
    if(error(ret))
        return fail(L, hSTMT, stmt->cur.hstmt);
    return 0;
}

static int stmt_putparam(lua_State *L, stmt_data *stmt, par_data *par){
    const char *type = sqltypetolua(par->sqltype);
    /* deal with data according to type */
    switch (type[1]) {
        /* nUmber */
        case 'u': return stmt_putparam_number_(L,stmt,par);
        /* bOol */
        case 'o': return stmt_putparam_bool_(L,stmt,par);
        /* sTring */ 
        case 't': 
        /* bInary */
        case 'i': return stmt_putparam_binary_(L,stmt,par);
    }
    return 0;
}

//}

//{ statement interface

static int stmt_execute(lua_State *L){
    stmt_data *stmt = getstmt (L);
    SQLHSTMT hstmt  = stmt->cur.hstmt;
    int top = lua_gettop(L);
    SQLRETURN ret;
    SQLRETURN exec_ret;

    if((stmt->cur.numcols > 0) && (stmt->cur.closed == 0))
        return luaL_error (L, LUASQL_PREFIX"there are open cursor");

    if(stmt->prepared){
        ret = SQLExecute (hstmt);
        if(stmt->resultsetno != 0){// cols are not valid
          luaL_unref (L, LUA_REGISTRYINDEX, stmt->cur.colnames);
          luaL_unref (L, LUA_REGISTRYINDEX, stmt->cur.coltypes);
          stmt->cur.colnames  = LUA_NOREF;
          stmt->cur.coltypes  = LUA_NOREF;
          stmt->cur.numcols   = 0;
          stmt->resultsetno   = 0;
        }
    }
    else{
        const char *statement = luaL_checkstring(L, 2);
        luaL_unref (L, LUA_REGISTRYINDEX, stmt->cur.colnames);
        luaL_unref (L, LUA_REGISTRYINDEX, stmt->cur.coltypes);
        stmt->cur.numcols = 0;

        ret = SQLExecDirect (hstmt, (char *) statement, SQL_NTS); 
    }

    if(
        (error(ret))&&
        (ret != SQL_NEED_DATA)&&
        (ret != LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND))
    ){
        return fail(L, hSTMT, hstmt);
    }

    if(ret == SQL_NEED_DATA){
        while(1){
            par_data *par;
            ret = SQLParamData(hstmt, &par); // if done then this call execute statement
            if(ret == SQL_NEED_DATA){
                if(ret = stmt_putparam(L, stmt, par))
                    return ret;
            }
            else{
               break;
            }
        }
        if(
            (error(ret))&&
            (ret != LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND))
        ){
            return fail(L, hSTMT, hstmt);
        }
    }
    assert(lua_gettop(L) == top);
    exec_ret = ret;

    if(!stmt->cur.numcols){
        SQLSMALLINT numcols;
        if(!stmt->prepared){
        assert(lua_isstring(L,-1));
        lua_pop(L, 1); // SQL text for SQLExecDirect
        }

        ret = SQLNumResultCols (hstmt, &numcols);
        if (error(ret))
            return fail(L, hSTMT, hstmt);
        stmt->cur.numcols = numcols;
        if(numcols)
            create_colinfo(L, &stmt->cur);
    }

    if (stmt->cur.numcols > 0){
        stmt->cur.closed = 0;
    }
    else{
        // For ODBC3, if the last call to SQLExecute or SQLExecDirect
        // returned SQL_NO_DATA, a call to SQLRowCount can cause a
        // function sequence error. Therefore, if the last result is
        // SQL_NO_DATA, we simply return 0

        SQLINTEGER numrows = 0;
        if(exec_ret != LUASQL_ODBC3_C(SQL_NO_DATA, SQL_NO_DATA_FOUND)){
            /* if action has no results (e.g., UPDATE) */
            ret = SQLRowCount(hstmt, &numrows);
            if(error(ret)){
                return fail(L, hSTMT, hstmt);
            }
        }
        lua_pushnumber(L, numrows);
    }

    return 1; // rowsaffected or self
}

static int stmt_reset_colinfo (lua_State *L) {
  cur_data *cur = &getstmt(L)->cur;
  luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
  luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);
  cur->colnames  = LUA_NOREF;
  cur->coltypes  = LUA_NOREF;
  cur->numcols   = 0;
  return pass(L);
}

static int stmt_moreresults(lua_State *L){
    stmt_data *stmt = getstmt (L);
    cur_data *cur   = &stmt->cur;
    SQLHSTMT hstmt  = cur->hstmt;
    SQLRETURN ret;

    if((cur->numcols == 0) || (cur->closed != 0))
        return luaL_error (L, LUASQL_PREFIX"there are no open cursor");

    ret = SQLMoreResults(hstmt);
    if(ret == LUASQL_ODBC3_C(SQL_NO_DATA,SQL_NO_DATA_FOUND)) return 0;
    if(error(ret)) return fail(L, hSTMT, hstmt);


    {
    SQLSMALLINT numcols;
    luaL_unref (L, LUA_REGISTRYINDEX, cur->colnames);
    luaL_unref (L, LUA_REGISTRYINDEX, cur->coltypes);
    cur->colnames  = LUA_NOREF;
    cur->coltypes  = LUA_NOREF;
    cur->numcols   = 0;
    ret = SQLNumResultCols (hstmt, &numcols);
    if (error(ret)) return fail(L, hSTMT, hstmt);
    stmt->cur.numcols = numcols;
    stmt->resultsetno++;
    if(numcols) create_colinfo(L, cur);
    }
    return 1;
}

/*
** Unbind all params and columns from statement
** after call statement steel alive
*/
static int stmt_reset (lua_State *L) {
    stmt_data *stmt = getstmt (L);
    stmt_clear_info_(L, stmt);
    return pass(L);
}

/*
** Prepare statement
*/
static int stmt_prepare (lua_State *L) {
    stmt_data *stmt = (stmt_data *)luaL_checkudata(L, 1, LUASQL_STMT_ODBC);
    const char *statement = luaL_checkstring(L, 2);
    int ret = stmt_prepare_(L, stmt, statement);
    if(ret) return ret;
    return pass(L);
}

/*
** is statement prepare 
*/
static int stmt_prepared (lua_State *L) {
    stmt_data *stmt = getstmt (L);
    lua_pushboolean(L, stmt->prepared);
    return 1;
}

/*
** is cursor opened
*/
static int stmt_opened (lua_State *L) {
    stmt_data *stmt = getstmt (L);
    lua_pushboolean(L, stmt->cur.closed?0:1);
    return 1;
}

/*
** 
*/
static int stmt_parcount (lua_State *L) {
    stmt_data *stmt = getstmt (L);
    lua_pushnumber(L, stmt->numpars);
    return 1;
}

/*
** Closes a statement.
*/
static int stmt_destroy (lua_State *L) {
    stmt_data *stmt = (stmt_data *)luaL_checkudata (L, 1, LUASQL_STMT_ODBC);
    luaL_argcheck (L, stmt != NULL, 1, LUASQL_PREFIX"statement expected");
    stmt_destroy_(L, stmt);
    return pass(L);
}

//}

//{ cursor interface

/*
** Closes a cursor.
*/
static int stmt_cur_close(lua_State *L) {
    stmt_data *stmt = getstmt (L);
    SQLRETURN ret;
    int ret_count;
    if(stmt->cur.closed)
      return pass(L);

    if(stmt->cur.numcols == 0){
       return luasql_faildirect(L, "try close not cursor.");
    }

    // always pass
    ret_count = pass(L);
    ret = SQLCloseCursor(stmt->cur.hstmt);
    if (error(ret)) ret_count += push_diagnostics(L, hSTMT, stmt->cur.hstmt);
    stmt->cur.closed = 1;

    return ret_count;
}

static int stmt_fetch(lua_State *L){
    stmt_data *stmt = getstmt (L);
    return cur_fetch_raw(L, &stmt->cur);
}

/*
** Returns the table with column names.
*/
static int stmt_colnames (lua_State *L) {
    cur_data *cur = &(getstmt(L))->cur;
    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->colnames);
    return 1;
}

/*
** Returns the table with column types.
*/
static int stmt_coltypes (lua_State *L) {
    cur_data *cur = &(getstmt(L))->cur;
    lua_rawgeti (L, LUA_REGISTRYINDEX, cur->coltypes);
    return 1;
}

//}

//{ foreach

static int stmt_foreach(lua_State *L){
    cur_data *cur = &(getstmt(L))->cur;
    return cur_foreach_raw(L, cur, stmt_cur_close);
}

//}

//{ get/set attr

static int stmt_get_uint_attr(lua_State*L){
    stmt_data *stmt = getstmt (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    return cur_get_uint_attr_(L, &stmt->cur, optnum);
}

static int stmt_get_str_attr(lua_State*L){
    stmt_data *stmt = getstmt (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    return cur_get_str_attr_(L, &stmt->cur, optnum);
}

static int stmt_set_uint_attr(lua_State*L){
    stmt_data *stmt = getstmt (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    return cur_set_uint_attr_(L, &stmt->cur, optnum,luaL_checkinteger(L,3));
}

static int stmt_set_str_attr(lua_State*L){
    stmt_data *stmt = getstmt (L);
    SQLINTEGER optnum = luaL_checkinteger(L,2);
    size_t len;
    const char *str = luaL_checklstring(L,3,&len);
    return cur_set_str_attr_(L, &stmt->cur, optnum, str, len);
}

//}

//{ Params
// libodbc++
#define DEFINE_GET_UINT_ATTR(NAME, WHAT) \
static int stmt_get_##NAME(lua_State*L){\
    stmt_data *stmt = getstmt (L);\
    return cur_get_uint_attr_(L, &stmt->cur, (WHAT));\
}

#define DEFINE_SET_UINT_ATTR(NAME,WHAT) \
static int stmt_set_##NAME(lua_State*L){\
    stmt_data *stmt = getstmt (L);\
    return cur_set_uint_attr_(L, &stmt->cur, (WHAT), luaL_checkinteger(L,2));\
}

DEFINE_GET_UINT_ATTR(querytimeout, LUASQL_ODBC3_C(SQL_ATTR_QUERY_TIMEOUT,SQL_QUERY_TIMEOUT));
DEFINE_SET_UINT_ATTR(querytimeout, LUASQL_ODBC3_C(SQL_ATTR_QUERY_TIMEOUT,SQL_QUERY_TIMEOUT));
DEFINE_GET_UINT_ATTR(maxrows,      LUASQL_ODBC3_C(SQL_ATTR_MAX_ROWS,SQL_MAX_ROWS));
DEFINE_SET_UINT_ATTR(maxrows,      LUASQL_ODBC3_C(SQL_ATTR_MAX_ROWS,SQL_MAX_ROWS));
DEFINE_GET_UINT_ATTR(maxfieldsize, LUASQL_ODBC3_C(SQL_ATTR_MAX_LENGTH,SQL_MAX_LENGTH));
DEFINE_SET_UINT_ATTR(maxfieldsize, LUASQL_ODBC3_C(SQL_ATTR_MAX_LENGTH,SQL_MAX_LENGTH));


static int stmt_set_escapeprocessing(lua_State *L){
  stmt_data *stmt = getstmt (L);
  int on = lua_toboolean(L,2);
  return cur_set_uint_attr_(L, &stmt->cur,
    (LUASQL_ODBC3_C(SQL_ATTR_NOSCAN,SQL_NOSCAN)),
    on?SQL_NOSCAN_OFF:SQL_NOSCAN_ON
  );
}

static int stmt_get_escapeprocessing(lua_State *L){
  stmt_data *stmt = getstmt (L);
  int ret = cur_get_uint_attr_(L, &stmt->cur, (LUASQL_ODBC3_C(SQL_ATTR_NOSCAN,SQL_NOSCAN)));
  if(ret != 1) return ret;
  lua_pushboolean(L, lua_tonumber(L,-1) == SQL_NOSCAN_OFF);
  return 1;
}

#undef DEFINE_GET_UINT_ATTR
#undef DEFINE_SET_UINT_ATTR

static int stmt_get_autoclose(lua_State *L) {
    stmt_data *stmt = getstmt (L);
    lua_pushboolean(L, stmt->cur.autoclose?1:0);
    return 1;
}

static int stmt_set_autoclose(lua_State *L) {
    stmt_data *stmt = getstmt (L);
    stmt->cur.autoclose = lua_toboolean(L,2)?1:0;
    return 1;
}

//}

//}----------------------------------------------------------------------------

static int conn_environment(lua_State *L){
    lua_rawgeti (L, LUA_REGISTRYINDEX, getconnection(L)->env);
    return 1;
}

static int stmt_connection(lua_State *L){
    lua_rawgeti (L, LUA_REGISTRYINDEX, getstmt(L)->cur.conn);
    return 1;
}

static int cur_connection(lua_State *L){
    lua_rawgeti (L, LUA_REGISTRYINDEX, getcursor(L)->conn);
    return 1;
}


/*
** Create metatables for each class of object.
*/
static void create_metatables (lua_State *L) {
    struct luaL_Reg environment_methods[] = {
        {"__gc", env_close},
        {"close", env_close},
        {"connect", env_connect},
        {"driverconnect", env_driverconnect},

        {"connection", env_connection},
        {"getdrivers", env_getdrivers},
        {"getdatasources", env_getdatasources},

        {"setlogintimeout", env_setlogintimeout},
        {"getlogintimeout", env_getlogintimeout},
        // {"setv2", env_setv2},
        // {"setv3", env_setv3},

        {"getuintattr", env_get_uint_attr},
        {"getstrattr", env_get_str_attr},
        {"setuintattr", env_set_uint_attr},
        {"setstrattr", env_set_str_attr},
        {NULL, NULL},
    };
    struct luaL_Reg connection_methods[] = {
        {"environment",   conn_environment},

        {"__gc",          conn_close},
        {"close",         conn_close},
        {"execute",       conn_execute},

        {"statement",     conn_create_stmt},
        {"prepare",       conn_prepare_stmt},

        {"commit",        conn_commit},
        {"rollback",      conn_rollback},

        {"setautocommit", conn_setautocommit},
        {"getautocommit", conn_getautocommit},
        {"setcatalog",    conn_setcatalog},
        {"getcatalog",    conn_getcatalog},
        {"setreadonly",   conn_setreadonly},
        {"getreadonly",   conn_getreadonly},
        {"settracefile",  conn_settracefile},
        {"gettracefile",  conn_gettracefile},
        {"settrace",      conn_settrace},
        {"gettrace",      conn_gettrace},
        {"gettransactionisolation",conn_gettransactionisolation},
        {"settransactionisolation",conn_settransactionisolation},
        {"setlogintimeout", conn_setlogintimeout},
        {"getlogintimeout", conn_getlogintimeout},

        {"connected",     conn_connected},

        {"getuintattr",   conn_get_uint_attr},
        {"getstrattr",    conn_get_str_attr},
        {"setuintattr",   conn_set_uint_attr},
        {"setstrattr",    conn_set_str_attr},

        {"getuint32info", conn_get_uint32_info},
        {"getuint16info", conn_get_uint16_info},
        {"getstrinfo",    conn_get_str_info},

        {"supportsPrepare",   conn_has_prepare},
        {"supportsBindParam", conn_has_bindparam},

        {"disconnect",   conn_disconnect},
        {"connect",   conn_connect},
        {"driverconnect", conn_driverconnect},

        // libodbc++ databasemetadata
        {"getdbmsname"       ,     conn_getdbmsname},
        {"getdbmsver"        ,     conn_getdbmsver},
        {"getdrvname"        ,     conn_getdrvname},
        {"getdrvver"         ,     conn_getdrvver},
        {"getodbcver"        ,     conn_getodbcver},
        {"getodbcvermm"      ,     conn_getodbcvermm},

        {"gettypeinfo"          ,  conn_gettypeinfo         },
        {"gettables"            ,  conn_gettables           },
        {"getstatistics"        ,  conn_getstatistics       },
        {"getcolumns"           ,  conn_getcolumns          },
        {"gettableprivileges"   ,  conn_gettableprivileges  },
        {"getcolumnprivileges"  ,  conn_getcolumnprivileges },
        {"getprimarykeys"       ,  conn_getprimarykeys      },
        {"getindexinfo"         ,  conn_getindexinfo        },
        {"crossreference"       ,  conn_crossreference      },
        {"getprocedures"        ,  conn_getprocedures       },
        {"getprocedurecolumns"  ,  conn_getprocedurecolumns },
        {"getspecialcolumns"    ,  conn_getspecialcolumns   },


        {"getIdentifierQuoteString",                  conn_getIdentifierQuoteString},
        {"getCatalogTerm",                            conn_getCatalogTerm},
        {"getSchemaTerm",                             conn_getSchemaTerm},
        {"getTableTerm",                              conn_getTableTerm},
        {"getProcedureTerm",                          conn_getProcedureTerm},
        {"getUserName",                               conn_getUserName},
        {"getCatalogSeparator",                       conn_getCatalogSeparator},
        {"isCatalogName",                             conn_isCatalogName},
        {"isCatalogAtStart",                          conn_isCatalogAtStart},
        {"getSQLKeywords",                            conn_getSQLKeywords},
        {"supportsTransactions",                      conn_supportsTransactions},
        {"supportsDataDefinitionAndDataManipulationTransactions", conn_supportsDataDefinitionAndDataManipulationTransactions},
        {"supportsDataManipulationTransactionsOnly",  conn_supportsDataManipulationTransactionsOnly},
        {"dataDefinitionCausesTransactionCommit",     conn_dataDefinitionCausesTransactionCommit},
        {"dataDefinitionIgnoredInTransactions",       conn_dataDefinitionIgnoredInTransactions},
        {"getDefaultTransactionIsolation",            conn_getDefaultTransactionIsolation},
        {"supportsTransactionIsolationLevel",         conn_supportsTransactionIsolationLevel},
        {"supportsOpenCursorsAcrossCommit",           conn_supportsOpenCursorsAcrossCommit},
        {"supportsOpenStatementsAcrossCommit",        conn_supportsOpenStatementsAcrossCommit},
        {"supportsOpenCursorsAcrossRollback",         conn_supportsOpenCursorsAcrossRollback },
        {"supportsOpenStatementsAcrossRollback",      conn_supportsOpenStatementsAcrossRollback },
#ifdef LUASQL_USE_DRIVERINFO
        {"supportsResultSetType",                     conn_supportsResultSetType},
        {"supportsResultSetConcurrency",              conn_supportsResultSetConcurrency},
#endif
        {"nullPlusNonNullIsNull",                     conn_nullPlusNonNullIsNull},
        {"supportsColumnAliasing",                    conn_supportsColumnAliasing},
        {"supportsAlterTableWithAddColumn",           conn_supportsAlterTableWithAddColumn},
        {"supportsAlterTableWithDropColumn",          conn_supportsAlterTableWithDropColumn},
        {"getExtraNameCharacters",                    conn_getExtraNameCharacters},
        {"getSearchStringEscape",                     conn_getSearchStringEscape},
        {"getNumericFunctions",                       conn_getNumericFunctions},
        {"getStringFunctions",                        conn_getStringFunctions},
        {"getSystemFunctions",                        conn_getSystemFunctions},
        {"getTimeDateFunctions",                      conn_getTimeDateFunctions},
        {"supportsCatalogsInDataManipulation",        conn_supportsCatalogsInDataManipulation},             
        {"supportsCatalogsInProcedureCalls",          conn_supportsCatalogsInProcedureCalls},                         
        {"supportsCatalogsInTableDefinitions",        conn_supportsCatalogsInTableDefinitions},                     
        {"supportsCatalogsInPrivilegeDefinitions",    conn_supportsCatalogsInIndexDefinitions},                     
        {"supportsCatalogsInIndexDefinitions",        conn_supportsCatalogsInIndexDefinitions},                     
        {"supportsSchemasInDataManipulation",         conn_supportsSchemasInDataManipulation},                       
        {"supportsSchemasInProcedureCalls",           conn_supportsSchemasInProcedureCalls},                           
        {"supportsSchemasInTableDefinitions",         conn_supportsSchemasInTableDefinitions},                       
        {"supportsSchemasInIndexDefinitions",         conn_supportsSchemasInIndexDefinitions},                       
        {"supportsSchemasInPrivilegeDefinitions",     conn_supportsSchemasInPrivilegeDefinitions},               
        {"supportsGroupBy",                           conn_supportsGroupBy},                                                           
        {"supportsGroupByUnrelated",                  conn_supportsGroupByUnrelated},                                         
        {"supportsGroupByBeyondSelect",               conn_supportsGroupByBeyondSelect},                                   
        {"supportsUnion",                             conn_supportsUnion},                                                       
        {"supportsUnionAll",                          conn_supportsUnionAll},                                                 
        {"supportsOuterJoins",                        conn_supportsOuterJoins},                                             
        {"supportsFullOuterJoins",                    conn_supportsFullOuterJoins},                                     
        {"supportsLimitedOuterJoins",                 conn_supportsLimitedOuterJoins},                               
        {"usesLocalFilePerTable",                     conn_usesLocalFilePerTable},      
        {"usesLocalFiles",                            conn_usesLocalFiles},                    
        {"nullsAreSortedHigh",                        conn_nullsAreSortedHigh},            
        {"nullsAreSortedLow",                         conn_nullsAreSortedLow},              
        {"nullsAreSortedAtStart",                     conn_nullsAreSortedAtStart},      
        {"nullsAreSortedAtEnd",                       conn_nullsAreSortedAtEnd},          
        {"allProceduresAreCallable",                  conn_allProceduresAreCallable},
        {"allTablesAreSelectable",                    conn_allTablesAreSelectable},    
        {"isReadOnly",                                conn_isReadOnly},                            
        {"supportsTableCorrelationNames",             conn_supportsTableCorrelationNames},  
        {"supportsCorrelatedSubqueries",              conn_supportsCorrelatedSubqueries},   
        {"supportsSubqueriesInComparisons",           conn_supportsSubqueriesInComparisons},
        {"supportsSubqueriesInExists",                conn_supportsSubqueriesInExists},     
        {"supportsSubqueriesInIns",                   conn_supportsSubqueriesInIns},                
        {"supportsSubqueriesInQuantifieds",           conn_supportsSubqueriesInQuantifieds},                
        {"supportsExpressionsInOrderBy",              conn_supportsExpressionsInOrderBy},      
        {"supportsLikeEscapeClause",                  conn_supportsLikeEscapeClause},                      
        {"supportsMultipleResultSets",                conn_supportsMultipleResultSets},                  
        {"supportsNonNullableColumns",                conn_supportsNonNullableColumns},                  
        {"supportsMinimumSQLGrammar",                 conn_supportsMinimumSQLGrammar},                    
        {"supportsCoreSQLGrammar",                    conn_supportsCoreSQLGrammar},                  
        {"supportsExtendedSQLGrammar",                conn_supportsExtendedSQLGrammar},                  
        {"supportsANSI92EntryLevelSQL",               conn_supportsANSI92EntryLevelSQL},                
        {"supportsANSI92FullSQL",                     conn_supportsANSI92FullSQL},                            
        {"supportsANSI92IntermediateSQL",             conn_supportsANSI92IntermediateSQL},            
        {"getMaxBinaryLiteralLength",                 conn_getMaxBinaryLiteralLength}, 
        {"getMaxCharLiteralLength",                   conn_getMaxCharLiteralLength},           
        {"getMaxColumnNameLength",                    conn_getMaxColumnNameLength},            
        {"getMaxColumnsInGroupBy",                    conn_getMaxColumnsInGroupBy},            
        {"getMaxColumnsInIndex",                      conn_getMaxColumnsInIndex},              
        {"getMaxColumnsInOrderBy",                    conn_getMaxColumnsInOrderBy},            
        {"getMaxColumnsInSelect",                     conn_getMaxColumnsInSelect},             
        {"getMaxColumnsInTable",                      conn_getMaxColumnsInTable},              
        {"getMaxCursorNameLength",                    conn_getMaxCursorNameLength},            
        {"getMaxIndexLength",                         conn_getMaxIndexLength},                 
        {"getMaxSchemaNameLength",                    conn_getMaxSchemaNameLength},            
        {"getMaxProcedureNameLength",                 conn_getMaxProcedureNameLength},         
        {"getMaxCatalogNameLength",                   conn_getMaxCatalogNameLength},           
        {"getMaxRowSize",                             conn_getMaxRowSize},                     
        {"getMaxStatementLength",                     conn_getMaxStatementLength},             
        {"getMaxTableNameLength",                     conn_getMaxTableNameLength},             
        {"getMaxTablesInSelect",                      conn_getMaxTablesInSelect},              
        {"getMaxUserNameLength",                      conn_getMaxUserNameLength},              
        {"getMaxConnections",                         conn_getMaxConnections},                 
        {"getMaxStatements",                          conn_getMaxStatements},                  
        {"doesMaxRowSizeIncludeBlobs",                conn_doesMaxRowSizeIncludeBlobs},        
        {"supportsMultipleTransactions",              conn_supportsMultipleTransactions},                    
        {"supportsOrderByUnrelated",                  conn_supportsOrderByUnrelated},                            
        {"supportsDifferentTableCorrelationNames",    conn_supportsDifferentTableCorrelationNames},
        {"supportsConvertFn",                         conn_supportsConvertFn},                                          
        {"supportsConvert",                           conn_supportsConvert},                                              
        {"storesLowerCaseIdentifiers",                conn_storesLowerCaseIdentifiers},                        
        {"storesLowerCaseQuotedIdentifiers",          conn_storesLowerCaseQuotedIdentifiers},            
        {"storesMixedCaseIdentifiers",                conn_storesMixedCaseIdentifiers},                        
        {"storesMixedCaseQuotedIdentifiers",          conn_storesMixedCaseQuotedIdentifiers},            
        {"storesUpperCaseIdentifiers",                conn_storesUpperCaseIdentifiers},                        
        {"storesUpperCaseQuotedIdentifiers",          conn_storesUpperCaseQuotedIdentifiers},            
        {"supportsMixedCaseIdentifiers",              conn_supportsMixedCaseIdentifiers},                    
        {"supportsMixedCaseQuotedIdentifiers",        conn_supportsMixedCaseQuotedIdentifiers},        
        {"supportsStoredProcedures",                  conn_supportsStoredProcedures},                            
#ifdef LUASQL_USE_DRIVERINFO
        {"ownUpdatesAreVisible",                      conn_ownUpdatesAreVisible},
        {"ownDeletesAreVisible",                      conn_ownDeletesAreVisible},
        {"ownInsertsAreVisible",                      conn_ownInsertsAreVisible},
        {"othersUpdatesAreVisible",                   conn_othersUpdatesAreVisible},
        {"othersInsertsAreVisible",                   conn_othersInsertsAreVisible},
        {"othersDeletesAreVisible",                   conn_othersDeletesAreVisible},
        {"deletesAreDetected",                        conn_deletesAreDetected},
        {"insertsAreDetected",                        conn_insertsAreDetected},
#endif
        {"updatesAreDetected",                        conn_updatesAreDetected},

        {NULL, NULL},
    };
    struct luaL_Reg cursor_methods[] = {
        {"connection",   cur_connection},

        {"__gc", cur_close},
        {"close", cur_close},
        {"fetch", cur_fetch},
        {"getcoltypes", cur_coltypes},
        {"getcolnames", cur_colnames},

        {"getmoreresults", cur_moreresults},
        {"getautoclose", cur_get_autoclose},
        {"setautoclose", cur_set_autoclose},
        {"opened", cur_opened},

        {"foreach", cur_foreach},

        {"getuintattr", cur_get_uint_attr},
        {"getstrattr", cur_get_str_attr},
        {"setuintattr", cur_set_uint_attr},
        {"setstrattr", cur_set_str_attr},
        {NULL, NULL},
    };
    struct luaL_Reg stmt_methods[] = {
        {"connection",  stmt_connection},

        {"__gc",        stmt_destroy},
        {"destroy",     stmt_destroy},

        {"reset",       stmt_reset},

        {"bind",        stmt_bind},
        {"bindnum",     stmt_bind_number},
        {"bindstr",     stmt_bind_string},
        {"bindbin",     stmt_bind_binary},
        {"bindbool",    stmt_bind_bool},
        {"bindnull",    stmt_bind_null},
        {"binddefault", stmt_bind_default},

        {"execute",     stmt_execute},

        {"prepare",     stmt_prepare},
        {"prepared",    stmt_prepared},
        
        {"getparcount", stmt_parcount},
        {"getmoreresults", stmt_moreresults},
        {"foreach", stmt_foreach},

        // cursor function
        {"close",       stmt_cur_close},
        {"fetch",       stmt_fetch},
        {"getcoltypes", stmt_coltypes},
        {"getcolnames", stmt_colnames},

        {"opened",      stmt_opened},

        {"resetcolinfo", stmt_reset_colinfo},

        {"getuintattr", stmt_get_uint_attr},
        {"getstrattr",  stmt_get_str_attr},
        {"setuintattr", stmt_set_uint_attr},
        {"setstrattr",  stmt_set_str_attr},
        
        
        {"getquerytimeout", stmt_get_querytimeout},
        {"setquerytimeout", stmt_set_querytimeout},
        {"getmaxrows"     , stmt_get_maxrows},
        {"setmaxrows"     , stmt_set_maxrows},
        {"getmaxfieldsize", stmt_get_maxfieldsize},
        {"setmaxfieldsize", stmt_set_maxfieldsize},
        {"getescapeprocessing", stmt_get_escapeprocessing},
        {"setescapeprocessing", stmt_set_escapeprocessing},
        {"getautoclose", stmt_get_autoclose},
        {"setautoclose", stmt_set_autoclose},
        

        {NULL, NULL},
    };

    luasql_createmeta (L, LUASQL_ENVIRONMENT_ODBC, environment_methods);
    luasql_createmeta (L, LUASQL_CONNECTION_ODBC, connection_methods);
    luasql_createmeta (L, LUASQL_CURSOR_ODBC, cursor_methods);
    luasql_createmeta (L, LUASQL_STMT_ODBC, stmt_methods);
    lua_pop (L, 3);
}


/*
** Creates the metatables for the objects and registers the
** driver open method.
*/
LUASQL_API int luaopen_luasql_odbc (lua_State *L) {
    struct luaL_Reg driver[] = {
        {"odbc", create_environment},
        {NULL, NULL},
    };
    create_metatables (L);
    lua_newtable (L);
    luaL_setfuncs (L, driver, 0);
    luasql_set_info (L);
    return 1;
} 




