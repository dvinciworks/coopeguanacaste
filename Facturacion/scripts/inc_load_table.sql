-- passthru all queries to incremental update ADRs coopeguanacaste

PROCEDURE incremental_load_table
 (
  my_query IN VARCHAR2
 )
IS

BEGIN
  EXECUTE IMMEDIATE my_query ;
END;