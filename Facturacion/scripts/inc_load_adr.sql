CREATE OR REPLACE
PROCEDURE incremental_load_adr_ventas
IS
  m_adr 		infografico."my_queries"."adr_name"%TYPE;
  m_table 		infografico."my_queries"."table_name"%TYPE;
  m_insert 		infografico."my_queries"."insert_statement"%TYPE;
  m_overflow    infografico."my_queries"."overflow_insert"%	TYPE;
  m_query 		infografico."my_queries"."count_statement"%TYPE;
  m_secuencia 	infografico."my_queries"."secuencia"%TYPE;

  CURSOR m_cursor IS
    SELECT 
       "adr_name",
       "table_name",
       "insert_statement",
       "overflow_insert",
       "count_statement",
       "secuencia"
    FROM infografico."my_queries"
    WHERE "adr_name" = 'ventas'
    ORDER BY "adr_name", "secuencia";
BEGIN
  open m_cursor;
      loop 
          fetch m_cursor into m_adr, m_table, m_insert, m_overflow, m_query, m_secuencia ;
          exit when m_cursor%NOTFOUND ;
          incremental_load_table(m_insert||m_overflow, m_query, m_table, m_adr);
      end loop;
  close m_cursor;
END;