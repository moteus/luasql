/*Sybase ASA 9.0.2*/

CREATE PROCEDURE DBA.sp_TestParam(
  in inIntVal     integer,
  in inUIntVal    unsigned integer,
  in inDoubleVal  double,
  in inStringVal  char(50),
  in inBinaryVal  binary(50),
  in inDateVal    date,
  in inNullVal    integer,
  in inDefaultVal integer default 1234,
  in inBitVal     bit
)
BEGIN
  select 
    inIntVal,
    inUIntVal,
    inDoubleVal,
    inStringVal,
    inBinaryVal,
    inDateVal,
    inNullVal,
    inDefaultVal,
    inBitVal,
    inGuidVal
END

GRANT CONNECT TO TestUser IDENTIFIED BY 'sql';
GRANT EXECUTE ON DBA.sp_TestParam TO TestUser