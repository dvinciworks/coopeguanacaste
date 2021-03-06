CREATE OR REPLACE
PROCEDURE incremental_load_table
 (
  my_insert IN VARCHAR2,
  my_query IN VARCHAR2,
  my_table IN VARCHAR2,
  my_adr IN VARCHAR2,
  my_flag IN BOOLEAN 
 )
IS
  conteo INTEGER;
  log_me VARCHAR2(2000);
BEGIN
  -- realizar conteo de los nuevos registros para efectos del log
  EXECUTE IMMEDIATE my_query INTO conteo;
  dbms_output.put_line(conteo);
  dbms_output.put_line('---');
  dbms_output.put_line(my_query);

  -- agregar registros nuevos a la tabla my_table
  if not my_flag then 
     EXECUTE IMMEDIATE my_insert;
  end if;

  -- registrar la ejecución en la bitácora my_log
  log_me := '
	INSERT INTO infografico."my_log" 
				( 
							"fecha_log", 
							"reference_log", 
							"count_log",
                            "adr_log" 
				) 
	SELECT sysdate  AS "fecha_log", 
		   :1 AS "reference_log", 
		   :2 AS "count_log",
           :3 AS "adr_log"
	FROM   dual
            ';
  EXECUTE IMMEDIATE log_me USING my_table, conteo, my_adr;
END;