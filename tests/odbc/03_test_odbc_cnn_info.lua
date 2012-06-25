require "config"
env = assert(luasql.odbc())

cnn = assert(env:connect(unpack(CNN_DSN)))

SQL_KEYWORDS=89
print('keywords:  ',cnn:getstrinfo(SQL_KEYWORDS))

print('catalog:   ',cnn:getcatalog())
print('trace:     ',cnn:gettrace())
print('tracefile: ',cnn:gettracefile())

function gettabletypes(cnn)
  return cnn:gettables(nil,nil,nil,'%')
end

function getschemas(cnn)
  return cnn:gettables(nil,'%',nil,nil)
end

function getcatalogs(cnn)
  return cnn:gettables('%',nil,nil,nil)
end

function dump_cursor(cur)
	local t = {}
	while cur:fetch(t,"a")do
		print("----------------------------------")
		table.foreach(t,print)
	end
	cur:close()
end

print("\n----------------------------------")
print("Table types")
print("----------------------------------")
dump_cursor(gettabletypes(cnn))

print("\n----------------------------------")
print("schemas")
print("----------------------------------")
dump_cursor(getschemas(cnn))

print("\n----------------------------------")
print("catalogs")
print("----------------------------------")
dump_cursor(getcatalogs(cnn))

--
-- ATTR
print("getdbmsname",  cnn:getdbmsname())
print("getdbmsver",   cnn:getdbmsver())
print("getdrvname",   cnn:getdrvname())
print("getdrvver",    cnn:getdrvver())
print("getodbcver",   cnn:getodbcver())
print("getodbcvermm", cnn:getodbcvermm())

print'-------------------------------------------------------------------------------------------------'
print("getIdentifierQuoteString ", cnn:getIdentifierQuoteString())
print("getCatalogTerm           ", cnn:getCatalogTerm())
print("getSchemaTerm            ", cnn:getSchemaTerm())
print("getTableTerm             ", cnn:getTableTerm())
print("getProcedureTerm         ", cnn:getProcedureTerm())
print("getUserName              ", cnn:getUserName())
print("getCatalogSeparator      ", cnn:getCatalogSeparator())
print("isCatalogName            ", cnn:isCatalogName())
print("isCatalogAtStart         ", cnn:isCatalogAtStart())
print("getSQLKeywords           ", cnn:getSQLKeywords())

print'-------------------------------------------------------------------------------------------------'
print("supportsTransactions                                  ",cnn:supportsTransactions())
print("supportsDataDefinitionAndDataManipulationTransactions ",cnn:supportsDataDefinitionAndDataManipulationTransactions())
print("supportsDataManipulationTransactionsOnly              ",cnn:supportsDataManipulationTransactionsOnly())
print("dataDefinitionCausesTransactionCommit                 ",cnn:dataDefinitionCausesTransactionCommit())
print("dataDefinitionIgnoredInTransactions                   ",cnn:dataDefinitionIgnoredInTransactions())

print'-------------------------------------------------------------------------------------------------'
TT = {
  "TRANSACTION_NONE","TRANSACTION_READ_UNCOMMITTED","TRANSACTION_READ_COMMITTED",
  "TRANSACTION_REPEATABLE_READ","TRANSACTION_SERIALIZABLE"
}
print("getDefaultTransactionIsolation       ",       TT[cnn:getDefaultTransactionIsolation()]);



for i, n in ipairs(TT) do
  print("supportsTransactionIsolationLevel", n,    cnn:supportsTransactionIsolationLevel(i));
end

print("supportsOpenCursorsAcrossCommit      ",      cnn:supportsOpenCursorsAcrossCommit());
print("supportsOpenStatementsAcrossCommit   ",   cnn:supportsOpenStatementsAcrossCommit());
print("supportsOpenCursorsAcrossRollback    ",    cnn:supportsOpenCursorsAcrossRollback());
print("supportsOpenStatementsAcrossRollback ", cnn:supportsOpenStatementsAcrossRollback());

print'-------------------------------------------------------------------------------------------------'
RST = {
  "RS_TYPE_FORWARD_ONLY",
  "RS_TYPE_SCROLL_INSENSITIVE", 
  "RS_TYPE_SCROLL_SENSITIVE"
}

for i, n in ipairs(RST) do
  print("supportsResultSetType", n, cnn:supportsResultSetType(i))
end
print("supportsResultSetType", "<ERR>", cnn:supportsResultSetType(#RST+1))


RSC = {
  "RS_CONCUR_READ_ONLY",
  "RS_CONCUR_UPDATABLE"
}
print ""
for i, n in ipairs(RST) do
  for j, m in ipairs(RSC) do
    print("supportsResultSetConcurrency",n, m, cnn:supportsResultSetConcurrency(i,j))
  end
end
print'-------------------------------------------------------------------------------------------------'


print ("nullPlusNonNullIsNull",               cnn:nullPlusNonNullIsNull())
print ("supportsColumnAliasing",              cnn:supportsColumnAliasing())
print ("supportsAlterTableWithAddColumn",     cnn:supportsAlterTableWithAddColumn())
print ("supportsAlterTableWithDropColumn",    cnn:supportsAlterTableWithDropColumn())
print ("getExtraNameCharacters",              cnn:getExtraNameCharacters())
print ("getSearchStringEscape",               cnn:getSearchStringEscape())
print'-------------------------------------------------------------------------------------------------'

print("getSQLKeywords           ", cnn:getSQLKeywords())

print"getNumericFunctions"
table.foreach(cnn:getNumericFunctions(), print)
print'-------------------------------------------------------------------------------------------------'
print"getStringFunctions"
table.foreach(cnn:getStringFunctions(), print)
print'-------------------------------------------------------------------------------------------------'
print"getSystemFunctions"
table.foreach(cnn:getSystemFunctions(), print)
print'-------------------------------------------------------------------------------------------------'
print"getTimeDateFunctions"
table.foreach(cnn:getTimeDateFunctions(), print)
print'-------------------------------------------------------------------------------------------------'

print("supportsCatalogsInDataManipulation     ",cnn:supportsCatalogsInDataManipulation())
print("supportsCatalogsInProcedureCalls       ",cnn:supportsCatalogsInProcedureCalls())
print("supportsCatalogsInTableDefinitions     ",cnn:supportsCatalogsInTableDefinitions())
print("supportsCatalogsInIndexDefinitions     ",cnn:supportsCatalogsInIndexDefinitions())
print("supportsCatalogsInPrivilegeDefinitions ",cnn:supportsCatalogsInPrivilegeDefinitions())
print("supportsSchemasInDataManipulation      ",cnn:supportsSchemasInDataManipulation())
print("supportsSchemasInProcedureCalls        ",cnn:supportsSchemasInProcedureCalls())
print("supportsSchemasInTableDefinitions      ",cnn:supportsSchemasInTableDefinitions())
print("supportsSchemasInIndexDefinitions      ",cnn:supportsSchemasInTableDefinitions())
print("supportsSchemasInPrivilegeDefinitions  ",cnn:supportsSchemasInPrivilegeDefinitions())
print("supportsGroupBy                        ",cnn:supportsGroupBy())
print("supportsGroupByUnrelated               ",cnn:supportsGroupByUnrelated())
print("supportsGroupByBeyondSelect            ",cnn:supportsGroupByBeyondSelect())
print("supportsUnion                          ",cnn:supportsUnion())
print("supportsUnionAll                       ",cnn:supportsUnionAll())
print("supportsOuterJoins                     ",cnn:supportsOuterJoins())
print("supportsFullOuterJoins                 ",cnn:supportsFullOuterJoins())
print("supportsLimitedOuterJoins              ",cnn:supportsLimitedOuterJoins())

print'-------------------------------------------------------------------------------------------------'
print("usesLocalFilePerTable    ",cnn:usesLocalFilePerTable())
print("usesLocalFiles           ",cnn:usesLocalFiles())
print("nullsAreSortedHigh       ",cnn:nullsAreSortedHigh())
print("nullsAreSortedLow        ",cnn:nullsAreSortedLow())
print("nullsAreSortedAtStart    ",cnn:nullsAreSortedAtStart())
print("nullsAreSortedAtEnd      ",cnn:nullsAreSortedAtEnd())
print("allProceduresAreCallable ",cnn:allProceduresAreCallable())
print("allTablesAreSelectable   ",cnn:allTablesAreSelectable())
print("isReadOnly               ",cnn:isReadOnly())

print'-------------------------------------------------------------------------------------------------'
print("supportsTableCorrelationNames   ",cnn:supportsTableCorrelationNames())
print("supportsCorrelatedSubqueries    ",cnn:supportsCorrelatedSubqueries())
print("supportsSubqueriesInComparisons ",cnn:supportsSubqueriesInComparisons())
print("supportsSubqueriesInExists      ",cnn:supportsSubqueriesInExists())
print("supportsSubqueriesInIns         ",cnn:supportsSubqueriesInIns())
print("supportsSubqueriesInIns         ",cnn:supportsSubqueriesInQuantifieds())

print'-------------------------------------------------------------------------------------------------'
print("supportsExpressionsInOrderBy  ", cnn:supportsExpressionsInOrderBy())
print("supportsLikeEscapeClause      ", cnn:supportsLikeEscapeClause())
print("supportsMultipleResultSets    ", cnn:supportsMultipleResultSets())
print("supportsNonNullableColumns    ", cnn:supportsNonNullableColumns())
print("supportsMinimumSQLGrammar     ", cnn:supportsMinimumSQLGrammar())
print("supportsCoreSQLGrammar        ", cnn:supportsCoreSQLGrammar())
print("supportsExtendedSQLGrammar    ", cnn:supportsExtendedSQLGrammar())
print("supportsANSI92EntryLevelSQL   ", cnn:supportsANSI92EntryLevelSQL())
print("supportsANSI92FullSQL         ", cnn:supportsANSI92FullSQL())
print("supportsANSI92IntermediateSQL ", cnn:supportsANSI92IntermediateSQL())

print'-------------------------------------------------------------------------------------------------'
print("getMaxBinaryLiteralLength  ",cnn:getMaxBinaryLiteralLength())
print("getMaxCharLiteralLength    ",cnn:getMaxCharLiteralLength())
print("getMaxColumnNameLength     ",cnn:getMaxColumnNameLength())
print("getMaxColumnsInGroupBy     ",cnn:getMaxColumnsInGroupBy())
print("getMaxColumnsInIndex       ",cnn:getMaxColumnsInIndex())
print("getMaxColumnsInOrderBy     ",cnn:getMaxColumnsInOrderBy())
print("getMaxColumnsInSelect      ",cnn:getMaxColumnsInSelect())
print("getMaxColumnsInTable       ",cnn:getMaxColumnsInTable())
print("getMaxCursorNameLength     ",cnn:getMaxCursorNameLength())
print("getMaxIndexLength          ",cnn:getMaxIndexLength())
print("getMaxSchemaNameLength     ",cnn:getMaxSchemaNameLength())
print("getMaxProcedureNameLength  ",cnn:getMaxProcedureNameLength())
print("getMaxCatalogNameLength    ",cnn:getMaxCatalogNameLength())
print("getMaxRowSize              ",cnn:getMaxRowSize())
print("getMaxStatementLength      ",cnn:getMaxStatementLength())
print("getMaxTableNameLength      ",cnn:getMaxTableNameLength())
print("getMaxTablesInSelect       ",cnn:getMaxTablesInSelect())
print("getMaxUserNameLength       ",cnn:getMaxUserNameLength())
print("getMaxConnections          ",cnn:getMaxConnections())
print("getMaxStatements           ",cnn:getMaxStatements())
print("doesMaxRowSizeIncludeBlobs ",cnn:doesMaxRowSizeIncludeBlobs())
print'-------------------------------------------------------------------------------------------------'
print("supportsMultipleTransactions            ",cnn:supportsMultipleTransactions())
print("supportsOrderByUnrelated                ",cnn:supportsOrderByUnrelated())
print("supportsDifferentTableCorrelationNames  ",cnn:supportsDifferentTableCorrelationNames())
print("supportsConvertFn                       ",cnn:supportsConvertFn())

print'-------------------------------------------------------------------------------------------------'
TYPES = {
"TYPE_BIGINT","TYPE_BINARY","TYPE_BIT","TYPE_CHAR"
,"TYPE_DATE","TYPE_DECIMAL","TYPE_DOUBLE","TYPE_FLOAT"
,"TYPE_INTEGER","TYPE_LONGVARBINARY","TYPE_LONGVARCHAR"
,"TYPE_NUMERIC","TYPE_REAL","TYPE_SMALLINT","TYPE_TIME"
,"TYPE_TIMESTAMP","TYPE_TINYINT","TYPE_VARBINARY"
,"TYPE_VARCHAR"
,"TYPE_WCHAR","TYPE_WLONGVARCHAR","TYPE_WVARCHAR","TYPE_GUID"
}
for i, n in ipairs(TYPES) do
  for j, m in ipairs(TYPES) do
    print("supportsConvert                        ",n, m, cnn:supportsConvert(i,j))
  end
end
print'-------------------------------------------------------------------------------------------------'

print("storesLowerCaseIdentifiers              ",cnn:storesLowerCaseIdentifiers())
print("storesLowerCaseQuotedIdentifiers        ",cnn:storesLowerCaseQuotedIdentifiers())
print("storesMixedCaseIdentifiers              ",cnn:storesMixedCaseIdentifiers())
print("storesMixedCaseQuotedIdentifiers        ",cnn:storesMixedCaseQuotedIdentifiers())
print("storesUpperCaseIdentifiers              ",cnn:storesUpperCaseIdentifiers())
print("storesUpperCaseQuotedIdentifiers        ",cnn:storesUpperCaseQuotedIdentifiers())
print("supportsMixedCaseIdentifiers            ",cnn:supportsMixedCaseIdentifiers())
print("supportsMixedCaseQuotedIdentifiers      ",cnn:supportsMixedCaseQuotedIdentifiers())
print("supportsStoredProcedures                ",cnn:supportsStoredProcedures())

print'-------------------------------------------------------------------------------------------------'



for i, n in ipairs(RST) do
  print("====================== ", n, " ======================")
  print("ownUpdatesAreVisible"                    ,cnn:ownUpdatesAreVisible(i))
  print("ownDeletesAreVisible"                    ,cnn:ownDeletesAreVisible(i))
  print("ownInsertsAreVisible"                    ,cnn:ownInsertsAreVisible(i))
  print("othersUpdatesAreVisible"                 ,cnn:othersUpdatesAreVisible(i))
  print("othersInsertsAreVisible"                 ,cnn:othersInsertsAreVisible(i))
  print("othersDeletesAreVisible"                 ,cnn:othersDeletesAreVisible(i))
  print("deletesAreDetected"                      ,cnn:deletesAreDetected(i))
  print("insertsAreDetected"                      ,cnn:insertsAreDetected(i))
  print("updatesAreDetected"                      ,cnn:updatesAreDetected(i))
end

cnn:close()
env:close()
