CREATE OR REPLACE
PROCEDURE incremental_load_adr_ventas
IS
  m_adr 		infografico."my_queries"."adr_name"%TYPE;
  m_table 		infografico."my_queries"."table_name"%TYPE;
  m_insert 		infografico."my_queries"."insert_statement"%TYPE;
  m_overflow    infografico."my_queries"."overflow_insert"%	TYPE;
  m_overflow2    infografico."my_queries"."overflow_insert2"%TYPE;
  m_query 		infografico."my_queries"."count_statement"%TYPE;
  m_secuencia 	infografico."my_queries"."secuencia"%TYPE;

  CURSOR m_cursor IS
    SELECT 
       "adr_name",
       "table_name",
       "insert_statement",
       "overflow_insert",
	   "overflow_insert2",
       "count_statement",
       "secuencia"
    FROM infografico."my_queries"
    WHERE "adr_name" = 'ventas'
    ORDER BY "adr_name", "secuencia";
BEGIN
  open m_cursor;
      loop 
          fetch m_cursor into m_adr, m_table, m_insert, m_overflow, m_overflow2, m_query, m_secuencia ;
          exit when m_cursor%NOTFOUND ;
          dbms_output.put_line(m_secuencia);
          if m_table = 'infografico.ventas_hechos_recargas_ta' then
			DECLARE
			   ret_int INTEGER;
			   ret_int_execute INTEGER;
			   plsql_block  VARCHAR2(1000);
			BEGIN
			   plsql_block :='BEGIN CARGAR_RECARGAS; END;';
			   ret_int := DBMS_SQL.OPEN_CURSOR;
			   DBMS_SQL.PARSE(ret_int,plsql_block,DBMS_SQL.NATIVE);
			   ret_int_execute := DBMS_SQL.EXECUTE(ret_int);
			   DBMS_SQL.CLOSE_CURSOR(ret_int);
               incremental_load_table(plsql_block, 'select count(*) from infografico.ventas_hechos_recargas_ta', 'infografico.ventas_hechos_recargas_ta', 'ventas',TRUE);
			EXCEPTION
			WHEN OTHERS THEN
			   DBMS_SQL.CLOSE_CURSOR(ret_int);
			END;
          else
            incremental_load_table(m_insert||m_overflow||m_overflow2, m_query, m_table, m_adr,FALSE);
          end if;
      end loop;
  close m_cursor;
END;