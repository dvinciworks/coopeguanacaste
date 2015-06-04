DECLARE
  m_adr varchar2(200);
  m_table varchar2(100);
  m_insert varchar2(10000);
  m_overflow varchar2(10000);
  m_overflow2 varchar2(8000);
  m_query varchar2(4000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_hechos_recargas_ta';
  m_insert := '
DROP SEQUENCE seq_recargas_ta; 

CREATE SEQUENCE seq_recargas_ta 
  START WITH 1; 

DELETE FROM infografico.ventas_hechos_recargas_ta; 

INSERT INTO infografico.ventas_hechos_recargas_ta 
            (idhechorecarga, 
             "idcliente", 
             numero_telefono_recarga, 
             idusuario, 
             id_pos, 
             id_operador, 
             id_dia_recarga, 
             estado_recarga, 
             mensaje_recarga, 
             canal_recarga, 
             numero_factura_recarga, 
             error_ice, 
             reenviar_ice, 
             fecha_recarga, 
             fecha_envio, 
             monto_recarga) 
SELECT seq_recargas_ta.NEXTVAL AS idhechorecarga, 
       dc."idcliente"          AS idcliente, 
       ta.numero_telefono      AS numero_telefono_recarga, 
       0                       AS idusuario, 
       ta.id_pos               AS id_pos, 
       ta.id_operador          AS id_operador, 
       ta.id_dia_recarga       AS id_dia_recarga, 
       ta.estado               AS estado_recarga, 
       ta.mensaje              AS mensaje_recarga, 
       ta.canal                AS canal_recarga, 
       ta.no_factura           AS numero_factura_recarga, 
       ta.error_ice            AS error_ice, 
       ta.reenviar_ice         AS reenviar_ice, 
       ta.fecha                AS fecha_recarga, 
       ta.fec_hor_envio        AS fecha_envio, 
       ta.monto_recarga        AS monto_recarga 
FROM   coope_ta.ta_recargas ta 
       join (SELECT "id_cliente_ta", 
                    Max("idcliente") AS "idcliente" 
             FROM   infografico.ventas_dim_clientes 
             GROUP  BY "id_cliente_ta") dc 
         ON ta.id_cliente = dc."id_cliente_ta"; 
   ' ;

  if length(m_insert) > 4000 then
     if length(m_insert) <= 8000 then
		 m_overflow := substr(m_insert,4000-length(m_insert),length(m_insert));
		 m_insert := substr(m_insert,1,4000);
     else
	     m_overflow2 := substr(m_insert,8000-length(m_insert),length(m_insert));
         m_overflow := substr(m_insert,4001,4000);
		 m_insert := substr(m_insert,1,4000);
     end if;
  end if;

  m_query := '
    SELECT COUNT(*) 
	FROM   coope_ta.ta_recargas ta 
		   join (SELECT "id_cliente_ta", 
						Max("idcliente") AS "idcliente" 
				 FROM   infografico.ventas_dim_clientes 
				 GROUP  BY "id_cliente_ta") dc 
			 ON ta.id_cliente = dc."id_cliente_ta"; 
';
  
  insert into infografico."my_queries" 
    ("secuencia", "insert_statement","overflow_insert","overflow_insert2","count_statement","table_name","adr_name")
  select 
    seq_my_queries.nextval as "secuencia",
    m_insert as "insert_statement",
    m_overflow as "overflow_insert",
    m_overflow2 as "overflow_insert2",
    m_query as "count_statement",
    m_table as "table_name",
    m_adr as "adr_name"
  from dual;
 
END;