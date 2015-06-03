DECLARE
  m_table varchar2(100);
  m_insert varchar2(2000);
  m_query varchar2(1000);
BEGIN
  m_table := 'infografico.ventas_dim_tipos_articulo';
  m_insert := 
	'INSERT INTO infografico.ventas_dim_tipos_articulo 
	SELECT seq_tipo_articulo.nextval AS idtipoarticulo, 
		   tipo                      AS tipo_articulo 
	FROM   coopegua.articulo 
	WHERE  tipo NOT IN (SELECT DISTINCT "tipo_articulo" 
						FROM   infografico.ventas_dim_tipos_articulo) ';

  m_query := 
	'SELECT COUNT(*) 
	FROM   coopegua.articulo 
	WHERE  tipo NOT IN (SELECT DISTINCT "tipo_articulo" 
						FROM   infografico.ventas_dim_tipos_articulo) ';
  
  insert into infografico."my_queries" 
    ("secuencia", "insert_statement","count_statement","table_name")
  select 
    seq_my_queries.nextval as "secuencia",
    m_insert as "insert_statement",
    m_query as "count_statement",
    m_table as "table_name"
  from dual;
 
END;

DECLARE
  m_adr varchar2(200);
  m_table varchar2(100);
  m_insert varchar2(2000);
  m_query varchar2(1000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_dim_prov_articulos';
  m_insert := '
	INSERT INTO infografico.ventas_dim_prov_articulos 
	SELECT seq_dim_prov_articulos.nextval AS idproveedor, 
		   proveedor                      AS id_proveedor 
	FROM   coopegua.articulo 
	WHERE  proveedor NOT IN (SELECT "id_proveedor" 
							 FROM   infografico.ventas_dim_prov_articulos) ';

  m_query := 
	'SELECT COUNT(*) 
	FROM   coopegua.articulo 
	WHERE  proveedor NOT IN (SELECT "id_proveedor" 
							 FROM   infografico.ventas_dim_prov_articulos)  ';
  
  insert into infografico."my_queries" 
    ("secuencia", "insert_statement","count_statement","table_name","adr_name")
  select 
    seq_my_queries.nextval as "secuencia",
    m_insert as "insert_statement",
    m_query as "count_statement",
    m_table as "table_name",
    m_adr as "adr_name"
  from dual;
 
END;

DECLARE
  m_adr 		infografico."my_queries"."adr_name"%TYPE;
  m_table 		infografico."my_queries"."table_name"%TYPE;
  m_insert 		infografico."my_queries"."insert_statement"%TYPE;
  m_query 		infografico."my_queries"."count_statement"%TYPE;
  m_secuencia 	infografico."my_queries"."secuencia"%TYPE;

  CURSOR m_cursor IS
    SELECT 
       "adr_name",
       "table_name",
       "insert_statement",
       "count_statement",
       "secuencia"
    FROM infografico."my_queries"
    ORDER BY "adr_name", "secuencia";
BEGIN
  open m_cursor;
      loop 
          fetch m_cursor into m_adr, m_table, m_insert, m_query, m_secuencia ;
          exit when m_cursor%NOTFOUND ;
          incremental_load_table(m_insert, m_query, m_table, m_adr);
      end loop;
  close m_cursor;
END;

DECLARE
  m_adr varchar2(200);
  m_table varchar2(100);
  m_insert varchar2(6000);
  m_overflow varchar2(4000);
  m_query varchar2(2000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_hechos_articulos ';
  m_insert := '
	INSERT INTO ventas_hechos_articulos 
				("idhechosarticulos", 
				 "idusuariocreacion", 
				 "idusuarioultimomodificacion", 
				 "idcuentaarticulo", 
				 "idtipoimpuesto", 
				 "idunidadventa", 
				 "idunidadempaque", 
				 "idunidadalmacen", 
				 "idproveedor", 
				 "idclasificacionarticulo6", 
				 "idclasificacionarticulo5", 
				 "idclasificacionarticulo4", 
				 "idclasificacionarticulo3", 
				 "idclasificacionarticulo2", 
				 "idclasificacionarticulo1", 
				 "idtipoarticulo", 
				 "idarticulo", 
				 "fecha_creacion", 
				 "fecha_ultima_modificacion", 
				 "fecha_ultima_salida", 
				 "fecha_ultimo_ingreso", 
				 "fecha_ultimo_inventario", 
				 "fecha_ultimo_movimiento", 
				 "costo_comparativo", 
				 "costo_fiscal", 
				 "costo_prom_colones", 
				 "costo_prom_dolares", 
				 "costo_std_colones", 
				 "costo_std_dolares", 
				 "costo_ult_colones", 
				 "costo_ult_dolares", 
				 "existencia_maxima", 
				 "existencia_minima", 
				 "frecuencia_conteo", 
				 "orden_minima", 
				 "peso_bruto", 
				 "peso_neto", 
				 "plazo_reabastecimiento", 
				 "precio_base_colones", 
				 "precio_base_dolares", 
				 "punto_reorden") 
	SELECT seq_hechos_articulos.nextval AS idhechosarticulos, 
		   dum."idusuario"              AS idusuariocreacion, 
		   duc."idusuario"              AS idusuarioultimomodificacion, 
		   dca."idcuentaarticulo", 
		   dti."idtipoimpuesto", 
		   duv."idunidad"               AS idunidadventa, 
		   due."idunidad"               AS idunidadempaque, 
		   dua."idunidad"               AS idunidadalmacen, 
		   dpa."idproveedor", 
		   c6."idclasificacionarticulo" AS idclasificacion6, 
		   c5."idclasificacionarticulo" AS idclasificacion5, 
		   c4."idclasificacionarticulo" AS idclasificacion4, 
		   c3."idclasificacionarticulo" AS idclasificacion3, 
		   c2."idclasificacionarticulo" AS idclasificacion2, 
		   c1."idclasificacionarticulo" AS idclasificacion1, 
		   dta."idtipoarticulo", 
		   da."idarticulo", 
		   a.fch_hora_creacion          AS "fecha_creacion", 
		   a.fch_hora_ult_modif         AS "fecha_ultima_modificacion", 
		   a.ultima_salida              AS "fecha_ultima_salida", 
		   a.ultimo_ingreso             AS "fecha_ultimo_ingreso", 
		   a.ultimo_inventario          AS "fecha_ultimo_inventario", 
		   a.ultimo_movimiento          AS "fecha_ultimo_movimiento", 
		   a.costo_comparativo, 
		   a.costo_fiscal, 
		   a.costo_prom_loc             AS "costo_prom_colones", 
		   a.costo_prom_dol             AS "costo_prom_dolares", 
		   a.costo_std_loc              AS "costo_std_colones", 
		   a.costo_std_dol              AS "costo_std_dolares", 
		   a.costo_ult_loc              AS "costo_ult_colones", 
		   a.costo_ult_dol              AS "costo_ult_dolares", 
		   a.existencia_maxima, 
		   a.existencia_minima, 
		   a.frecuencia_conteo, 
		   a.orden_minima, 
		   a.peso_bruto, 
		   a.peso_neto, 
		   a.plazo_reabast              AS "plazo_reabastecimiento", 
		   a.precio_base_local          AS "precio_base_colones", 
		   a.precio_base_dolar          AS "precio_base_dolares", 
		   a.punto_de_reorden           AS punto_reorden 
	FROM   coopegua.articulo a 
		   LEFT OUTER JOIN ventas_dim_articulos da 
						ON a.articulo = da."id_articulo" 
		   LEFT OUTER JOIN ventas_dim_tipos_articulo dta 
						ON a.tipo = dta."tipo_articulo" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c1 
						ON a.clasificacion_1 = c1."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c2 
						ON a.clasificacion_1 = c2."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c3 
						ON a.clasificacion_1 = c3."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c4 
						ON a.clasificacion_1 = c4."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c5 
						ON a.clasificacion_1 = c5."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c6 
						ON a.clasificacion_1 = c6."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_prov_articulos dpa 
						ON a.proveedor = dpa."id_proveedor" 
		   LEFT OUTER JOIN ventas_dim_unidades dua 
						ON a.unidad_almacen = dua."unidad" 
		   LEFT OUTER JOIN ventas_dim_unidades due 
						ON a.unidad_empaque = due."unidad" 
		   LEFT OUTER JOIN ventas_dim_unidades duv 
						ON a.unidad_venta = duv."unidad" 
		   LEFT OUTER JOIN ventas_dim_tipos_impuesto dti 
						ON a.impuesto = dti."tipo_impuesto" 
		   LEFT OUTER JOIN ventas_dim_cuentas_articulo dca 
						ON a.articulo_cuenta = dca."cuenta_articulo" 
		   LEFT OUTER JOIN ventas_dim_usuarios duc 
						ON a.usuario_creacion = duc."nombre_usuario" 
		   LEFT OUTER JOIN ventas_dim_usuarios dum 
						ON a.usuario_ult_modif = dum."nombre_usuario" 
		   JOIN (SELECT d."id_articulo" 
				 FROM   infografico.ventas_hechos_articulos h 
						RIGHT OUTER JOIN infografico.ventas_dim_articulos d 
									  ON h."idarticulo" = d."idarticulo" 
				 WHERE  h."idarticulo" IS NULL) nca 
			 ON a.articulo = nca."id_articulo" 
   ';

  if length(m_insert) > 4000 then
     m_overflow := substr(m_insert,4000-length(m_insert),length(m_insert));
     m_insert := substr(m_insert,1,4000);
  end if;

  m_query := 
	'
     SELECT COUNT(*) 
	FROM   coopegua.articulo a 
		   LEFT OUTER JOIN ventas_dim_articulos da 
						ON a.articulo = da."id_articulo" 
		   LEFT OUTER JOIN ventas_dim_tipos_articulo dta 
						ON a.tipo = dta."tipo_articulo" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c1 
						ON a.clasificacion_1 = c1."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c2 
						ON a.clasificacion_1 = c2."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c3 
						ON a.clasificacion_1 = c3."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c4 
						ON a.clasificacion_1 = c4."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c5 
						ON a.clasificacion_1 = c5."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_clasif_articulo c6 
						ON a.clasificacion_1 = c6."clasificacion" 
		   LEFT OUTER JOIN ventas_dim_prov_articulos dpa 
						ON a.proveedor = dpa."id_proveedor" 
		   LEFT OUTER JOIN ventas_dim_unidades dua 
						ON a.unidad_almacen = dua."unidad" 
		   LEFT OUTER JOIN ventas_dim_unidades due 
						ON a.unidad_empaque = due."unidad" 
		   LEFT OUTER JOIN ventas_dim_unidades duv 
						ON a.unidad_venta = duv."unidad" 
		   LEFT OUTER JOIN ventas_dim_tipos_impuesto dti 
						ON a.impuesto = dti."tipo_impuesto" 
		   LEFT OUTER JOIN ventas_dim_cuentas_articulo dca 
						ON a.articulo_cuenta = dca."cuenta_articulo" 
		   LEFT OUTER JOIN ventas_dim_usuarios duc 
						ON a.usuario_creacion = duc."nombre_usuario" 
		   LEFT OUTER JOIN ventas_dim_usuarios dum 
						ON a.usuario_ult_modif = dum."nombre_usuario" 
		   JOIN (SELECT d."id_articulo" 
				 FROM   infografico.ventas_hechos_articulos h 
						RIGHT OUTER JOIN infografico.ventas_dim_articulos d 
									  ON h."idarticulo" = d."idarticulo" 
				 WHERE  h."idarticulo" IS NULL) nca 
			 ON a.articulo = nca."id_articulo" 
   ';
  
  insert into infografico."my_queries" 
    ("secuencia", "insert_statement","overflow_insert","count_statement","table_name","adr_name")
  select 
    seq_my_queries.nextval as "secuencia",
    m_insert as "insert_statement",
    m_overflow as "overflow_insert",
    m_query as "count_statement",
    m_table as "table_name",
    m_adr as "adr_name"
  from dual;
 
END;

DECLARE
  m_adr varchar2(200);
  m_table varchar2(100);
  m_insert varchar2(6000);
  m_overflow varchar2(4000);
  m_query varchar2(2000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_dim_consecutivos';
  m_insert := '
	INSERT INTO ventas_dim_consecutivos 
				("idconsecutivo", 
				 "id_consecutivo") 
	SELECT seg_dim_consecutivos.nextval AS idconsecutivo, 
		   a.consecutivo                AS id_consecutivo 
	FROM   (SELECT DISTINCT consecutivo 
			FROM   coopegua.factura) a 
   ' ;

  if length(m_insert) > 4000 then
     m_overflow := substr(m_insert,4000-length(m_insert),length(m_insert));
     m_insert := substr(m_insert,1,4000);
  end if;

  m_query := 
	'
     SELECT COUNT(*) 
	FROM   (SELECT DISTINCT consecutivo 
			FROM   coopegua.factura) a 
   ';
  
  insert into infografico."my_queries" 
    ("secuencia", "insert_statement","overflow_insert","count_statement","table_name","adr_name")
  select 
    seq_my_queries.nextval as "secuencia",
    m_insert as "insert_statement",
    m_overflow as "overflow_insert",
    m_query as "count_statement",
    m_table as "table_name",
    m_adr as "adr_name"
  from dual;
 
END;




