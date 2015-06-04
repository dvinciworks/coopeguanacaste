CREATE OR REPLACE
PROCEDURE Cargar_recargas 
AS 
BEGIN 
    --EXECUTE IMMEDIATE 'DROP SEQUENCE seq_recargas_ta'; 

    --EXECUTE IMMEDIATE 'CREATE SEQUENCE seq_recargas_ta START WITH 1'; 

    EXECUTE IMMEDIATE 'DELETE FROM infografico.ventas_hechos_recargas_ta'; 

    EXECUTE IMMEDIATE 'INSERT INTO infografico.ventas_hechos_recargas_ta              (idhechorecarga,               "idcliente",               numero_telefono_recarga,               idusuario,               id_pos,               id_operador,               id_dia_recarga,               estado_recarga,               mensaje_recarga,               canal_recarga,               numero_factura_recarga,               error_ice,               reenviar_ice,               fecha_recarga,               fecha_envio,               monto_recarga)  SELECT seq_recargas_ta.NEXTVAL AS idhechorecarga,         dc."idcliente"          AS idcliente,         ta.numero_telefono      AS numero_telefono_recarga,         0                       AS idusuario,         ta.id_pos               AS id_pos,         ta.id_operador          AS id_operador,         ta.id_dia_recarga       AS id_dia_recarga,         ta.estado               AS estado_recarga,         ta.mensaje              AS mensaje_recarga,         ta.canal                AS canal_recarga,         ta.no_factura           AS numero_factura_recarga,         ta.error_ice            AS error_ice,         ta.reenviar_ice         AS reenviar_ice,         ta.fecha                AS fecha_recarga,         ta.fec_hor_envio        AS fecha_envio,         ta.monto_recarga        AS monto_recarga  FROM   coope_ta.ta_recargas ta         join (SELECT "id_cliente_ta",                      Max("idcliente") AS "idcliente"               FROM   infografico.ventas_dim_clientes               GROUP  BY "id_cliente_ta") dc           ON ta.id_cliente = dc."id_cliente_ta"  '; 
END; 