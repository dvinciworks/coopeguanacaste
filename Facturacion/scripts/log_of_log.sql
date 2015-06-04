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

DECLARE
  m_adr varchar2(200);
  m_table varchar2(100);
  m_insert varchar2(6000);
  m_overflow varchar2(4000);
  m_query varchar2(2000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_dim_vendedor';
  m_insert := '
	INSERT INTO infografico.ventas_dim_vendedor 
				("idvendedor", 
				 "vendedor", 
				 "nombre") 
	SELECT seq_pedido.nextval AS idvendedor, 
		   cp.vendedor        AS vendedor, 
		   cp.nombre          AS nombre 
	FROM   coopegua.vendedor cp 
		   LEFT JOIN infografico.ventas_dim_vendedor dcp 
				  ON dcp."vendedor" = cp.vendedor 
	WHERE  dcp."vendedor" IS NULL 
   ' ;

  if length(m_insert) > 4000 then
     m_overflow := substr(m_insert,4000-length(m_insert),length(m_insert));
     m_insert := substr(m_insert,1,4000);
  end if;

  m_query := 
	'
    SELECT COUNT(*) 
	FROM   coopegua.vendedor cp 
		   LEFT JOIN infografico.ventas_dim_vendedor dcp 
				  ON dcp."vendedor" = cp.vendedor 
	WHERE  dcp."vendedor" IS NULL 
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
  m_table := 'infografico.ventas_dim_documento_credito';
  m_insert := '
	INSERT INTO infografico.ventas_dim_documento_credito 
				("iddocumentocredito", 
				 "id_documento_cxc", 
				 "subtipo_doc_cxc", 
				 "tipo_credito_cxc", 
				 "tipo_doc_cxc") 
	SELECT seg_dim_doc_credito .nextval AS iddocumentocredito, 
		   a.doc_credito_cxc            AS id_documento_cxc, 
		   a.subtipo_doc_cxc            AS subtipo_doc_cxc, 
		   a.tipo_credito_cxc           AS tipo_credito_cxc, 
		   a.tipo_doc_cxc               AS tipo_doc_cxc 
	FROM   (SELECT doc_credito_cxc, 
				   subtipo_doc_cxc, 
				   tipo_credito_cxc, 
				   tipo_doc_cxc 
			FROM   coopegua.factura 
			GROUP  BY doc_credito_cxc, 
					  subtipo_doc_cxc, 
					  tipo_credito_cxc, 
					  tipo_doc_cxc) a 
     left join infografico.ventas_dim_documento_credito dc 
      on  a.doc_credito_cxc = dc."id_documento_cxc"
      and a.subtipo_doc_cxc = dc."subtipo_doc_cxc"
      and a.tipo_credito_cxc = dc."tipo_credito_cxc"
      and a.tipo_doc_cxc = dc."tipo_doc_cxc"
      where dc."id_documento_cxc" is null and a.doc_credito_cxc is not null
   ' ;

  if length(m_insert) > 4000 then
     m_overflow := substr(m_insert,4000-length(m_insert),length(m_insert));
     m_insert := substr(m_insert,1,4000);
  end if;

  m_query := 
	'
    SELECT COUNT(*) 
	FROM   (SELECT doc_credito_cxc, 
				   subtipo_doc_cxc, 
				   tipo_credito_cxc, 
				   tipo_doc_cxc 
			FROM   coopegua.factura 
			GROUP  BY doc_credito_cxc, 
					  subtipo_doc_cxc, 
					  tipo_credito_cxc, 
					  tipo_doc_cxc) a 
     left join infografico.ventas_dim_documento_credito dc 
      on  a.doc_credito_cxc = dc."id_documento_cxc"
      and a.subtipo_doc_cxc = dc."subtipo_doc_cxc"
      and a.tipo_credito_cxc = dc."tipo_credito_cxc"
      and a.tipo_doc_cxc = dc."tipo_doc_cxc"
      where dc."id_documento_cxc" is null and a.doc_credito_cxc is not null
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
  m_table := 'infografico.ventas_dim_tipos';
  m_insert := '
	INSERT INTO ventas_dim_tipos 
				("idtipo", 
				 "id_tipo") 
	SELECT seg_dim_tipos.nextval AS idtipo, 
		   a.id_tipo             AS id_tipo 
	FROM   (SELECT DISTINCT tipo_documento AS id_tipo 
			FROM   coopegua.factura_linea 
			UNION 
			SELECT DISTINCT tipo_linea AS id_tipo 
			FROM   coopegua.factura_linea 
			UNION 
			SELECT DISTINCT tipo_documento AS id_tipo 
			FROM   coopegua.factura 
			UNION 
			SELECT DISTINCT tipo_original AS id_tipo 
			FROM   coopegua.factura) a 
     left join infografico.ventas_dim_tipos dd 
      on  a.id_tipo = dd."id_tipo"
      where dd."id_tipo" is null and a.id_tipo is not null
   ' ;

  if length(m_insert) > 4000 then
     m_overflow := substr(m_insert,4000-length(m_insert),length(m_insert));
     m_insert := substr(m_insert,1,4000);
  end if;

  m_query := 
	'
    SELECT COUNT(*) 
	FROM   (SELECT DISTINCT tipo_documento AS id_tipo 
			FROM   coopegua.factura_linea 
			UNION 
			SELECT DISTINCT tipo_linea AS id_tipo 
			FROM   coopegua.factura_linea 
			UNION 
			SELECT DISTINCT tipo_documento AS id_tipo 
			FROM   coopegua.factura 
			UNION 
			SELECT DISTINCT tipo_original AS id_tipo 
			FROM   coopegua.factura) a 
     left join infografico.ventas_dim_tipos dd 
      on  a.id_tipo = dd."id_tipo"
      where dd."id_tipo" is null and a.id_tipo is not null
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
  m_table := 'infografico.ventas_dim_bodega';
  m_insert := '
	INSERT INTO infografico.ventas_dim_bodega 
				("idbodega", 
				 "bodega", 
				 "nombre_bodega") 
	SELECT seq_bodega.nextval AS idbodega, 
		   cp.bodega, 
		   cp.nombre          AS nombre_bodega 
	FROM   coopegua.bodega cp 
		   LEFT JOIN infografico.ventas_dim_bodega dcp 
				  ON dcp."bodega" = cp.bodega 
	WHERE  dcp."bodega" IS NULL 
   ' ;

  if length(m_insert) > 4000 then
     m_overflow := substr(m_insert,4000-length(m_insert),length(m_insert));
     m_insert := substr(m_insert,1,4000);
  end if;

  m_query := 
	'
    SELECT COUNT(*) 
	SELECT seq_cond_pago.nextval AS idcondicionpago, 
		   cp.condicion_pago, 
		   cp.descripcion 
	FROM   coopegua.bodega cp 
		   LEFT JOIN infografico.ventas_dim_bodega dcp 
				  ON dcp."bodega" = cp.bodega 
	WHERE  dcp."bodega" IS NULL 
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
  m_insert varchar2(10000);
  m_overflow varchar2(4000);
  m_overflow2 varchar2(6000);
  m_query varchar2(2000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_hechos_facturas';
  m_insert := '
INSERT INTO ventas_hechos_facturas 
            ("idhechosfactura", 
             "idbodega", 
             "idarticulo", 
             "idfactura", 
             "idpedidos", 
             "iddocumentoasiento", 
             "iddocumentocredito", 
             "idfacturaoriginal", 
             "idestado", 
             "idestadolinea", 
             "idestadocobro", 
             "idestadoremision", 
             "idcontrato", 
             "idvendedor", 
             "idusuario", 
             "idusuarioanula", 
             "idcondicionpago", 
             "idconsecutivo", 
             "idrutas", 
             "idzona", 
             "ididlcliente", 
             "idtipodocumentolinea", 
             "idtipodocumentofactura", 
             "idtipooriginal", 
             "idtipolinea", 
             "factura_recorddate", 
             "fecha_factura", 
             "fecha_despacho", 
             "fecha_recibido", 
             "fecha_encabezado_factura", 
             "fecha_pedido", 
             "fecha_hora_anulacion", 
             "fecha_orden", 
             "fecha_rige", 
             "otra_fecha_factura", 
             "precio_unitario", 
             "cantidad_total_unidades", 
             "cantidad", 
             "cantidad_no_entregada", 
             "cantidad_devuelta", 
             "cantidad_aceptada", 
             "total_peso", 
             "total_peso_neto", 
             "importe_anticipo", 
             "importe_total_facturado", 
             "importe_total_mercaderia", 
             "importe_descuento1", 
             "importe_descuento2", 
             "importe_documentacion", 
             "importe_flete_factura", 
             "importe_impuesto__factura", 
             "importe_descuento_vol_fact", 
             "importe_base_impuesto", 
             "importe_descuento_volumen", 
             "importe_costo_dolares", 
             "importe_costo_colones", 
             "importe_costo_moneda", 
             "importe_impuesto", 
             "importe_precio_calculado", 
             "importe_descuento_linea", 
             "importe_descuento_general") 
SELECT seq_hechos_factura.nextval AS IDHECHOSFACTURA, 
       bod."idbodega", 
       dart."idarticulo", 
       df."idfactura", 
       dp."idpedidos", 
       0                          AS IDDOCUMENTOASIENTO, 
       dcxc."iddocumentocredito", 
       dfo."id_factura"           AS IDFACTURAORIGINAL, 
       0                          AS IDESTADO, 
       0                          AS IDESTADOLINEA, 
       0                          AS IDESTADOCOBRO, 
       0                          AS IDESTADOREMISION, 
       0                          AS IDCONTRATO, 
       dv."idvendedor", 
       du."idusuario", 
       duanula."idusuario"        AS IDUSUARIOANULA, 
       dcp."idcondicionpago", 
       conse."idconsecutivo", 
       dr."idruta"                AS IDRUTAS, 
       dz."idzona", 
       dc."idcliente"             AS ididlcliente, 
       dtdl."idtipo"              AS IDTIPODOCUMENTOLINEA, 
       tdf."idtipo"               AS IDTIPODOCUMENTOFACTURA, 
       tdo."idtipo"               AS IDTIPOORIGINAL, 
       dtl."idtipo"               AS IDTIPOLINEA, 
       f.recorddate               AS FACTURA_RECORDDATE, 
       f.fecha                    AS FECHA_FACTURA, 
       f.fecha_despacho           AS FECHA_DESPACHO, 
       f.fecha_recibido           AS FECHA_RECIBIDO, 
       f.fecha                    AS FECHA_ENCABEZADO_FACTURA, 
       f.fecha_pedido, 
       f.fecha_hora_anula         AS FECHA_HORA_ANULACION, 
       f.fecha_orden, 
       f.fecha_rige, 
       f.fecha_hora               AS OTRA_FECHA_FACTURA, 
       fl.precio_unitario, 
       f.total_unidades           AS CANTIDAD_TOTAL_UNIDADES, 
       fl.cantidad, 
       fl.cant_no_entregada       AS CANTIDAD_NO_ENTREGADA, 
       fl.cantidad_devuelt        AS CANTIDAD_DEVUELTA, 
       fl.cantidad_aceptada       AS CANTIDAD_ACEPTADA, 
       f.total_peso, 
       f.total_peso_neto, 
       f.monto_anticipo           AS IMPORTE_ANTICIPO, 
       f.total_factura            AS IMPORTE_TOTAL_FACTURADO, 
       f.total_mercaderia         AS IMPORTE_TOTAL_MERCADERIA, 
       f.monto_descuento1         AS IMPORTE_DESCUENTO1, 
       f.monto_descuento2         AS IMPORTE_DESCUENTO2, 
       f.monto_documentacio       AS IMPORTE_DOCUMENTACION, 
       f.monto_flete              AS IMPORTE_FLETE_FACTURA, 
       f.total_impuesto1          AS IMPORTE_IMPUESTO__FACTURA, 
       f.descuento_volumen        AS IMPORTE_DESCUENTO_VOL_FACT, 
       f.base_impuesto1           AS IMPORTE_BASE_IMPUESTO, 
       fl.descuento_volumen       AS IMPORTE_DESCUENTO_VOLUMEN, 
       fl.costo_total_dolar       AS IMPORTE_COSTO_DOLARES, 
       fl.costo_total_local       AS IMPORTE_COSTO_COLONES, 
       fl.costo_total             AS IMPORTE_COSTO_MONEDA, 
       fl.total_impuesto1         AS IMPORTE_IMPUESTO, 
       fl.precio_total            AS IMPORTE_PRECIO_CALCULADO, 
       fl.desc_tot_linea          AS IMPORTE_DESCUENTO_LINEA, 
       fl.desc_tot_general        AS IMPORTE_DESCUENTO_GENERAL 
FROM   coopegua.factura_linea fl 
       JOIN (SELECT fl.factura, 
                    fl.linea 
             FROM   coopegua.factura_linea fl 
                    JOIN (SELECT a.factura 
                          FROM   (SELECT factura, 
                                         Count(*) 
                                  FROM   coopegua.factura_linea 
                                  GROUP  BY factura) a 
                                 LEFT JOIN (SELECT df."id_factura", 
                                                   Count(*) 
                                            FROM 
                                 infografico.ventas_hechos_facturas 
                                 dhf 
                                 JOIN infografico.ventas_dim_facturas 
                                      df 
                                   ON dhf."idfactura" = 
                                      df."idfactura" 
                                            GROUP  BY df."id_factura") b 
                                        ON a.factura = b."id_factura" 
                          WHERE  b."id_factura" IS NULL) c 
                      ON fl.factura = c.factura 
             WHERE  fl.recorddate > (SELECT Max("factura_recorddate") 
                                     FROM   infografico.ventas_hechos_facturas)) 
            nuevos 
         ON fl.factura = nuevos.factura 
            AND fl.linea = nuevos.linea 
       JOIN coopegua.factura f 
            LEFT OUTER JOIN ventas_dim_consecutivos conse 
                         ON f.consecutivo = conse."id_consecutivo" 
            LEFT OUTER JOIN ventas_dim_pedidos dp 
                         ON f.pedido = dp."id_pedido" 
            LEFT OUTER JOIN ventas_dim_vendedor dv 
                         ON f.vendedor = dv."vendedor" 
            LEFT OUTER JOIN ventas_dim_documento_credito dcxc 
                         ON f.doc_credito_cxc = dcxc."id_documento_cxc" 
            LEFT OUTER JOIN ventas_dim_facturas df 
                         ON f.factura = df."id_factura" 
            LEFT OUTER JOIN ventas_dim_facturas dfo 
                         ON f.factura = dfo."id_factura_original" 
            LEFT OUTER JOIN ventas_dim_zonas dz 
                         ON f.zona = dz."zona" 
            LEFT OUTER JOIN ventas_dim_condicion_pago dcp 
                         ON f.condicion_pago = dcp."condicion_pago" 
            LEFT OUTER JOIN (SELECT To_char("codigo_cliente_facturacion") AS 
                                    codigo_cliente_facturacion, 
                                    Max("idcliente")                      AS 
                                    "idcliente" 
                             FROM   ventas_dim_clientes 
                             GROUP  BY To_char("codigo_cliente_facturacion")) dc 
                         ON f.cliente = dc.codigo_cliente_facturacion 
            LEFT OUTER JOIN ventas_dim_rutas dr 
                         ON f.ruta = dr."id_ruta" 
            LEFT OUTER JOIN ventas_dim_tipos tdf 
                         ON f.tipo_documento = tdf."id_tipo" 
            LEFT OUTER JOIN ventas_dim_tipos tdo 
                         ON f.tipo_original = tdo."id_tipo" 
            LEFT OUTER JOIN ventas_dim_usuarios du 
                         ON f.usuario = du."nombre_usuario" 
            LEFT OUTER JOIN ventas_dim_usuarios duanula 
                         ON f.usuario_anula = duanula."nombre_usuario" 
         ON fl.factura = f.factura 
       LEFT OUTER JOIN ventas_dim_articulos dart 
                    ON fl.articulo = dart."id_articulo" 
       LEFT OUTER JOIN ventas_dim_documentos dd 
                    ON fl.documento_origen = dd."id_documento" 
       LEFT OUTER JOIN ventas_dim_tipos dtdl 
                    ON fl.tipo_documento = dtdl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_tipos dtl 
                    ON fl.tipo_linea = dtl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_bodega bod 
                    ON fl.bodega = bod."bodega" 
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

  m_query := 
	'
    SELECT COUNT(*) 
FROM   coopegua.factura_linea fl 
       JOIN (SELECT fl.factura, 
                    fl.linea 
             FROM   coopegua.factura_linea fl 
                    JOIN (SELECT a.factura 
                          FROM   (SELECT factura, 
                                         Count(*) 
                                  FROM   coopegua.factura_linea 
                                  GROUP  BY factura) a 
                                 LEFT JOIN (SELECT df."id_factura", 
                                                   Count(*) 
                                            FROM 
                                 infografico.ventas_hechos_facturas 
                                 dhf 
                                 JOIN infografico.ventas_dim_facturas 
                                      df 
                                   ON dhf."idfactura" = 
                                      df."idfactura" 
                                            GROUP  BY df."id_factura") b 
                                        ON a.factura = b."id_factura" 
                          WHERE  b."id_factura" IS NULL) c 
                      ON fl.factura = c.factura 
             WHERE  fl.recorddate > (SELECT Max("factura_recorddate") 
                                     FROM   infografico.ventas_hechos_facturas)) 
            nuevos 
         ON fl.factura = nuevos.factura 
            AND fl.linea = nuevos.linea 
       JOIN coopegua.factura f 
            LEFT OUTER JOIN ventas_dim_consecutivos conse 
                         ON f.consecutivo = conse."id_consecutivo" 
            LEFT OUTER JOIN ventas_dim_pedidos dp 
                         ON f.pedido = dp."id_pedido" 
            LEFT OUTER JOIN ventas_dim_vendedor dv 
                         ON f.vendedor = dv."vendedor" 
            LEFT OUTER JOIN ventas_dim_documento_credito dcxc 
                         ON f.doc_credito_cxc = dcxc."id_documento_cxc" 
            LEFT OUTER JOIN ventas_dim_facturas df 
                         ON f.factura = df."id_factura" 
            LEFT OUTER JOIN ventas_dim_facturas dfo 
                         ON f.factura = dfo."id_factura_original" 
            LEFT OUTER JOIN ventas_dim_zonas dz 
                         ON f.zona = dz."zona" 
            LEFT OUTER JOIN ventas_dim_condicion_pago dcp 
                         ON f.condicion_pago = dcp."condicion_pago" 
            LEFT OUTER JOIN (SELECT To_char("codigo_cliente_facturacion") AS 
                                    codigo_cliente_facturacion, 
                                    Max("idcliente")                      AS 
                                    "idcliente" 
                             FROM   ventas_dim_clientes 
                             GROUP  BY To_char("codigo_cliente_facturacion")) dc 
                         ON f.cliente = dc.codigo_cliente_facturacion 
            LEFT OUTER JOIN ventas_dim_rutas dr 
                         ON f.ruta = dr."id_ruta" 
            LEFT OUTER JOIN ventas_dim_tipos tdf 
                         ON f.tipo_documento = tdf."id_tipo" 
            LEFT OUTER JOIN ventas_dim_tipos tdo 
                         ON f.tipo_original = tdo."id_tipo" 
            LEFT OUTER JOIN ventas_dim_usuarios du 
                         ON f.usuario = du."nombre_usuario" 
            LEFT OUTER JOIN ventas_dim_usuarios duanula 
                         ON f.usuario_anula = duanula."nombre_usuario" 
         ON fl.factura = f.factura 
       LEFT OUTER JOIN ventas_dim_articulos dart 
                    ON fl.articulo = dart."id_articulo" 
       LEFT OUTER JOIN ventas_dim_documentos dd 
                    ON fl.documento_origen = dd."id_documento" 
       LEFT OUTER JOIN ventas_dim_tipos dtdl 
                    ON fl.tipo_documento = dtdl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_tipos dtl 
                    ON fl.tipo_linea = dtl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_bodega bod 
                    ON fl.bodega = bod."bodega" 
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

DECLARE
  m_adr varchar2(200);
  m_table varchar2(100);
  m_insert varchar2(10000);
  m_overflow varchar2(10000);
  m_overflow2 varchar2(8000);
  m_query varchar2(4000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_hechos_facturas';
  m_insert := '
INSERT INTO ventas_hechos_facturas 
            ("idhechosfactura", 
             "idbodega", 
             "idarticulo", 
             "idfactura", 
             "idpedidos", 
             "iddocumentoasiento", 
             "iddocumentocredito", 
             "idfacturaoriginal", 
             "idestado", 
             "idestadolinea", 
             "idestadocobro", 
             "idestadoremision", 
             "idcontrato", 
             "idvendedor", 
             "idusuario", 
             "idusuarioanula", 
             "idcondicionpago", 
             "idconsecutivo", 
             "idrutas", 
             "idzona", 
             "ididlcliente", 
             "idtipodocumentolinea", 
             "idtipodocumentofactura", 
             "idtipooriginal", 
             "idtipolinea", 
             "factura_recorddate", 
             "fecha_factura", 
             "fecha_despacho", 
             "fecha_recibido", 
             "fecha_encabezado_factura", 
             "fecha_pedido", 
             "fecha_hora_anulacion", 
             "fecha_orden", 
             "fecha_rige", 
             "otra_fecha_factura", 
             "precio_unitario", 
             "cantidad_total_unidades", 
             "cantidad", 
             "cantidad_no_entregada", 
             "cantidad_devuelta", 
             "cantidad_aceptada", 
             "total_peso", 
             "total_peso_neto", 
             "importe_anticipo", 
             "importe_total_facturado", 
             "importe_total_mercaderia", 
             "importe_descuento1", 
             "importe_descuento2", 
             "importe_documentacion", 
             "importe_flete_factura", 
             "importe_impuesto__factura", 
             "importe_descuento_vol_fact", 
             "importe_base_impuesto", 
             "importe_descuento_volumen", 
             "importe_costo_dolares", 
             "importe_costo_colones", 
             "importe_costo_moneda", 
             "importe_impuesto", 
             "importe_precio_calculado", 
             "importe_descuento_linea", 
             "importe_descuento_general") 
SELECT seq_hechos_factura.nextval AS IDHECHOSFACTURA, 
       bod."idbodega", 
       dart."idarticulo", 
       df."idfactura", 
       dp."idpedidos", 
       0                          AS IDDOCUMENTOASIENTO, 
       dcxc."iddocumentocredito", 
       dfo."id_factura"           AS IDFACTURAORIGINAL, 
       0                          AS IDESTADO, 
       0                          AS IDESTADOLINEA, 
       0                          AS IDESTADOCOBRO, 
       0                          AS IDESTADOREMISION, 
       0                          AS IDCONTRATO, 
       dv."idvendedor", 
       du."idusuario", 
       duanula."idusuario"        AS IDUSUARIOANULA, 
       dcp."idcondicionpago", 
       conse."idconsecutivo", 
       dr."idruta"                AS IDRUTAS, 
       dz."idzona", 
       dc."idcliente"             AS ididlcliente, 
       dtdl."idtipo"              AS IDTIPODOCUMENTOLINEA, 
       tdf."idtipo"               AS IDTIPODOCUMENTOFACTURA, 
       tdo."idtipo"               AS IDTIPOORIGINAL, 
       dtl."idtipo"               AS IDTIPOLINEA, 
       f.recorddate               AS FACTURA_RECORDDATE, 
       f.fecha                    AS FECHA_FACTURA, 
       f.fecha_despacho           AS FECHA_DESPACHO, 
       f.fecha_recibido           AS FECHA_RECIBIDO, 
       f.fecha                    AS FECHA_ENCABEZADO_FACTURA, 
       f.fecha_pedido, 
       f.fecha_hora_anula         AS FECHA_HORA_ANULACION, 
       f.fecha_orden, 
       f.fecha_rige, 
       f.fecha_hora               AS OTRA_FECHA_FACTURA, 
       fl.precio_unitario, 
       f.total_unidades           AS CANTIDAD_TOTAL_UNIDADES, 
       fl.cantidad, 
       fl.cant_no_entregada       AS CANTIDAD_NO_ENTREGADA, 
       fl.cantidad_devuelt        AS CANTIDAD_DEVUELTA, 
       fl.cantidad_aceptada       AS CANTIDAD_ACEPTADA, 
       f.total_peso, 
       f.total_peso_neto, 
       f.monto_anticipo           AS IMPORTE_ANTICIPO, 
       f.total_factura            AS IMPORTE_TOTAL_FACTURADO, 
       f.total_mercaderia         AS IMPORTE_TOTAL_MERCADERIA, 
       f.monto_descuento1         AS IMPORTE_DESCUENTO1, 
       f.monto_descuento2         AS IMPORTE_DESCUENTO2, 
       f.monto_documentacio       AS IMPORTE_DOCUMENTACION, 
       f.monto_flete              AS IMPORTE_FLETE_FACTURA, 
       f.total_impuesto1          AS IMPORTE_IMPUESTO__FACTURA, 
       f.descuento_volumen        AS IMPORTE_DESCUENTO_VOL_FACT, 
       f.base_impuesto1           AS IMPORTE_BASE_IMPUESTO, 
       fl.descuento_volumen       AS IMPORTE_DESCUENTO_VOLUMEN, 
       fl.costo_total_dolar       AS IMPORTE_COSTO_DOLARES, 
       fl.costo_total_local       AS IMPORTE_COSTO_COLONES, 
       fl.costo_total             AS IMPORTE_COSTO_MONEDA, 
       fl.total_impuesto1         AS IMPORTE_IMPUESTO, 
       fl.precio_total            AS IMPORTE_PRECIO_CALCULADO, 
       fl.desc_tot_linea          AS IMPORTE_DESCUENTO_LINEA, 
       fl.desc_tot_general        AS IMPORTE_DESCUENTO_GENERAL 
FROM   coopegua.factura_linea fl 
       JOIN (SELECT fl.factura, 
                    fl.linea 
             FROM   coopegua.factura_linea fl 
                    JOIN (SELECT a.factura 
                          FROM   (SELECT factura, 
                                         Count(*) 
                                  FROM   coopegua.factura_linea 
                                  GROUP  BY factura) a 
                                 LEFT JOIN (SELECT df."id_factura", 
                                                   Count(*) 
                                            FROM 
                                 infografico.ventas_hechos_facturas 
                                 dhf 
                                 JOIN infografico.ventas_dim_facturas 
                                      df 
                                   ON dhf."idfactura" = 
                                      df."idfactura" 
                                            GROUP  BY df."id_factura") b 
                                        ON a.factura = b."id_factura" 
                          WHERE  b."id_factura" IS NULL) c 
                      ON fl.factura = c.factura 
             WHERE  fl.recorddate > (SELECT Max("FACTURA_RECORDDATE") 
                                     FROM   infografico.ventas_hechos_facturas)) 
            nuevos 
         ON fl.factura = nuevos.factura 
            AND fl.linea = nuevos.linea 
       JOIN coopegua.factura f 
            LEFT OUTER JOIN ventas_dim_consecutivos conse 
                         ON f.consecutivo = conse."id_consecutivo" 
            LEFT OUTER JOIN ventas_dim_pedidos dp 
                         ON f.pedido = dp."id_pedido" 
            LEFT OUTER JOIN ventas_dim_vendedor dv 
                         ON f.vendedor = dv."vendedor" 
            LEFT OUTER JOIN ventas_dim_documento_credito dcxc 
                         ON f.doc_credito_cxc = dcxc."id_documento_cxc" 
            LEFT OUTER JOIN ventas_dim_facturas df 
                         ON f.factura = df."id_factura" 
            LEFT OUTER JOIN ventas_dim_facturas dfo 
                         ON f.factura = dfo."id_factura_original" 
            LEFT OUTER JOIN ventas_dim_zonas dz 
                         ON f.zona = dz."zona" 
            LEFT OUTER JOIN ventas_dim_condicion_pago dcp 
                         ON f.condicion_pago = dcp."condicion_pago" 
            LEFT OUTER JOIN (SELECT To_char("codigo_cliente_facturacion") AS 
                                    codigo_cliente_facturacion, 
                                    Max("idcliente")                      AS 
                                    "idcliente" 
                             FROM   ventas_dim_clientes 
                             GROUP  BY To_char("codigo_cliente_facturacion")) dc 
                         ON f.cliente = dc.codigo_cliente_facturacion 
            LEFT OUTER JOIN ventas_dim_rutas dr 
                         ON f.ruta = dr."id_ruta" 
            LEFT OUTER JOIN ventas_dim_tipos tdf 
                         ON f.tipo_documento = tdf."id_tipo" 
            LEFT OUTER JOIN ventas_dim_tipos tdo 
                         ON f.tipo_original = tdo."id_tipo" 
            LEFT OUTER JOIN ventas_dim_usuarios du 
                         ON f.usuario = du."nombre_usuario" 
            LEFT OUTER JOIN ventas_dim_usuarios duanula 
                         ON f.usuario_anula = duanula."nombre_usuario" 
         ON fl.factura = f.factura 
       LEFT OUTER JOIN ventas_dim_articulos dart 
                    ON fl.articulo = dart."id_articulo" 
       LEFT OUTER JOIN ventas_dim_documentos dd 
                    ON fl.documento_origen = dd."id_documento" 
       LEFT OUTER JOIN ventas_dim_tipos dtdl 
                    ON fl.tipo_documento = dtdl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_tipos dtl 
                    ON fl.tipo_linea = dtl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_bodega bod 
                    ON fl.bodega = bod."bodega" 
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

  m_query := 
	'SELECT COUNT(*) 
FROM   coopegua.factura_linea fl 
       JOIN (SELECT fl.factura, 
                    fl.linea 
             FROM   coopegua.factura_linea fl 
                    JOIN (SELECT a.factura 
                          FROM   (SELECT factura, 
                                         Count(*) 
                                  FROM   coopegua.factura_linea 
                                  GROUP  BY factura) a 
                                 LEFT JOIN (SELECT df."id_factura", 
                                                   Count(*) 
                                            FROM 
                                 infografico.ventas_hechos_facturas 
                                 dhf 
                                 JOIN infografico.ventas_dim_facturas 
                                      df 
                                   ON dhf."idfactura" = 
                                      df."idfactura" 
                                            GROUP  BY df."id_factura") b 
                                        ON a.factura = b."id_factura" 
                          WHERE  b."id_factura" IS NULL) c 
                      ON fl.factura = c.factura 
             WHERE  fl.recorddate > (SELECT Max("FACTURA_RECORDDATE") 
                                     FROM   infografico.ventas_hechos_facturas)) 
            nuevos 
         ON fl.factura = nuevos.factura 
            AND fl.linea = nuevos.linea 
       JOIN coopegua.factura f 
            LEFT OUTER JOIN ventas_dim_consecutivos conse ON f.consecutivo = conse."id_consecutivo" 
            LEFT OUTER JOIN ventas_dim_pedidos dp 
                         ON f.pedido = dp."id_pedido" 
            LEFT OUTER JOIN ventas_dim_vendedor dv 
                         ON f.vendedor = dv."vendedor" 
            LEFT OUTER JOIN ventas_dim_documento_credito dcxc 
                         ON f.doc_credito_cxc = dcxc."id_documento_cxc" 
            LEFT OUTER JOIN ventas_dim_facturas df 
                         ON f.factura = df."id_factura" 
            LEFT OUTER JOIN ventas_dim_facturas dfo 
                         ON f.factura = dfo."id_factura_original" 
            LEFT OUTER JOIN ventas_dim_zonas dz 
                         ON f.zona = dz."zona" 
            LEFT OUTER JOIN ventas_dim_condicion_pago dcp 
                         ON f.condicion_pago = dcp."condicion_pago" 
            LEFT OUTER JOIN (SELECT To_char("codigo_cliente_facturacion") AS 
                                    codigo_cliente_facturacion, 
                                    Max("idcliente")                      AS 
                                    "idcliente" 
                             FROM   ventas_dim_clientes 
                             GROUP  BY To_char("codigo_cliente_facturacion")) dc 
                         ON f.cliente = dc.codigo_cliente_facturacion 
            LEFT OUTER JOIN ventas_dim_rutas dr 
                         ON f.ruta = dr."id_ruta" 
            LEFT OUTER JOIN ventas_dim_tipos tdf 
                         ON f.tipo_documento = tdf."id_tipo" 
            LEFT OUTER JOIN ventas_dim_tipos tdo 
                         ON f.tipo_original = tdo."id_tipo" 
            LEFT OUTER JOIN ventas_dim_usuarios du 
                         ON f.usuario = du."nombre_usuario" 
            LEFT OUTER JOIN ventas_dim_usuarios duanula 
                         ON f.usuario_anula = duanula."nombre_usuario" 
         ON fl.factura = f.factura 
       LEFT OUTER JOIN ventas_dim_articulos dart 
                    ON fl.articulo = dart."id_articulo" 
       LEFT OUTER JOIN ventas_dim_documentos dd 
                    ON fl.documento_origen = dd."id_documento" 
       LEFT OUTER JOIN ventas_dim_tipos dtdl 
                    ON fl.tipo_documento = dtdl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_tipos dtl 
                    ON fl.tipo_linea = dtl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_bodega bod 
                    ON fl.bodega = bod."bodega"';
  
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

DECLARE
  m_adr varchar2(200);
  m_table varchar2(100);
  m_insert varchar2(10000);
  m_overflow varchar2(10000);
  m_overflow2 varchar2(8000);
  m_query varchar2(4000);
BEGIN
  m_adr := 'ventas';
  m_table := 'infografico.ventas_hechos_facturas';
  m_insert := '
INSERT INTO infografico.ventas_hechos_facturas 
            (idhechosfactura, 
             "idbodega", 
             "idarticulo", 
             "idfactura", 
             "idpedidos", 
             iddocumentoasiento, 
             "iddocumentocredito", 
             idfacturaoriginal, 
             idestado, 
             idestadolinea, 
             idestadocobro, 
             idestadoremision, 
             idcontrato, 
             "idvendedor", 
             "idusuario", 
             idusuarioanula, 
             "idcondicionpago", 
             "idconsecutivo", 
             idrutas, 
             "idzona", 
             ididlcliente, 
             idtipodocumentolinea, 
             idtipodocumentofactura, 
             idtipooriginal, 
             idtipolinea, 
             factura_recorddate, 
             fecha_factura, 
             fecha_despacho, 
             fecha_recibido, 
             fecha_encabezado_factura, 
             fecha_pedido, 
             fecha_hora_anulacion, 
             fecha_orden, 
             fecha_rige, 
             otra_fecha_factura, 
             precio_unitario, 
             cantidad_total_unidades, 
             cantidad, 
             cantidad_no_entregada, 
             cantidad_devuelta, 
             cantidad_aceptada, 
             total_peso, 
             total_peso_neto, 
             importe_anticipo, 
             importe_total_facturado, 
             importe_total_mercaderia, 
             importe_descuento1, 
             importe_descuento2, 
             importe_documentacion, 
             importe_flete_factura, 
             importe_impuesto__factura, 
             importe_descuento_vol_fact, 
             importe_base_impuesto, 
             importe_descuento_volumen, 
             importe_costo_dolares, 
             importe_costo_colones, 
             importe_costo_moneda, 
             importe_impuesto, 
             importe_precio_calculado, 
             importe_descuento_linea, 
             importe_descuento_general) 
SELECT seq_hechos_factura.nextval AS IDHECHOSFACTURA, 
       bod."idbodega", 
       dart."idarticulo", 
       df."idfactura", 
       dp."idpedidos", 
       0                          AS IDDOCUMENTOASIENTO, 
       dcxc."iddocumentocredito", 
       dfo."id_factura"           AS IDFACTURAORIGINAL, 
       0                          AS IDESTADO, 
       0                          AS IDESTADOLINEA, 
       0                          AS IDESTADOCOBRO, 
       0                          AS IDESTADOREMISION, 
       0                          AS IDCONTRATO, 
       dv."idvendedor", 
       du."idusuario", 
       duanula."idusuario"        AS IDUSUARIOANULA, 
       dcp."idcondicionpago", 
       conse."idconsecutivo", 
       dr."idruta"                AS IDRUTAS, 
       dz."idzona", 
       dc."idcliente"             AS ididlcliente, 
       dtdl."idtipo"              AS IDTIPODOCUMENTOLINEA, 
       tdf."idtipo"               AS IDTIPODOCUMENTOFACTURA, 
       tdo."idtipo"               AS IDTIPOORIGINAL, 
       dtl."idtipo"               AS IDTIPOLINEA, 
       f.recorddate               AS FACTURA_RECORDDATE, 
       f.fecha                    AS FECHA_FACTURA, 
       f.fecha_despacho           AS FECHA_DESPACHO, 
       f.fecha_recibido           AS FECHA_RECIBIDO, 
       f.fecha                    AS FECHA_ENCABEZADO_FACTURA, 
       f.fecha_pedido, 
       f.fecha_hora_anula         AS FECHA_HORA_ANULACION, 
       f.fecha_orden, 
       f.fecha_rige, 
       f.fecha_hora               AS OTRA_FECHA_FACTURA, 
       fl.precio_unitario, 
       f.total_unidades           AS CANTIDAD_TOTAL_UNIDADES, 
       fl.cantidad, 
       fl.cant_no_entregada       AS CANTIDAD_NO_ENTREGADA, 
       fl.cantidad_devuelt        AS CANTIDAD_DEVUELTA, 
       fl.cantidad_aceptada       AS CANTIDAD_ACEPTADA, 
       f.total_peso, 
       f.total_peso_neto, 
       f.monto_anticipo           AS IMPORTE_ANTICIPO, 
       f.total_factura            AS IMPORTE_TOTAL_FACTURADO, 
       f.total_mercaderia         AS IMPORTE_TOTAL_MERCADERIA, 
       f.monto_descuento1         AS IMPORTE_DESCUENTO1, 
       f.monto_descuento2         AS IMPORTE_DESCUENTO2, 
       f.monto_documentacio       AS IMPORTE_DOCUMENTACION, 
       f.monto_flete              AS IMPORTE_FLETE_FACTURA, 
       f.total_impuesto1          AS IMPORTE_IMPUESTO__FACTURA, 
       f.descuento_volumen        AS IMPORTE_DESCUENTO_VOL_FACT, 
       f.base_impuesto1           AS IMPORTE_BASE_IMPUESTO, 
       fl.descuento_volumen       AS IMPORTE_DESCUENTO_VOLUMEN, 
       fl.costo_total_dolar       AS IMPORTE_COSTO_DOLARES, 
       fl.costo_total_local       AS IMPORTE_COSTO_COLONES, 
       fl.costo_total             AS IMPORTE_COSTO_MONEDA, 
       fl.total_impuesto1         AS IMPORTE_IMPUESTO, 
       fl.precio_total            AS IMPORTE_PRECIO_CALCULADO, 
       fl.desc_tot_linea          AS IMPORTE_DESCUENTO_LINEA, 
       fl.desc_tot_general        AS IMPORTE_DESCUENTO_GENERAL 
FROM   coopegua.factura_linea fl 
       JOIN (SELECT fl.factura, 
                    fl.linea 
             FROM   coopegua.factura_linea fl 
                    JOIN (SELECT a.factura 
                          FROM   (SELECT factura, 
                                         Count(*) 
                                  FROM   coopegua.factura_linea 
                                  GROUP  BY factura) a 
                                 LEFT JOIN (SELECT df."id_factura", 
                                                   Count(*) 
                                            FROM 
                                 infografico.ventas_hechos_facturas 
                                 dhf 
                                 JOIN infografico.ventas_dim_facturas 
                                      df 
                                   ON dhf."idfactura" = 
                                      df."idfactura" 
                                            GROUP  BY df."id_factura") b 
                                        ON a.factura = b."id_factura" 
                          WHERE  b."id_factura" IS NULL) c 
                      ON fl.factura = c.factura 
             WHERE  fl.recorddate > (SELECT Max("factura_recorddate") 
                                     FROM   infografico.ventas_hechos_facturas)) 
            nuevos 
         ON fl.factura = nuevos.factura 
            AND fl.linea = nuevos.linea 
       JOIN coopegua.factura f 
            LEFT OUTER JOIN ventas_dim_consecutivos conse 
                         ON f.consecutivo = conse."id_consecutivo" 
            LEFT OUTER JOIN ventas_dim_pedidos dp 
                         ON f.pedido = dp."id_pedido" 
            LEFT OUTER JOIN ventas_dim_vendedor dv 
                         ON f.vendedor = dv."vendedor" 
            LEFT OUTER JOIN ventas_dim_documento_credito dcxc 
                         ON f.doc_credito_cxc = dcxc."id_documento_cxc" 
            LEFT OUTER JOIN ventas_dim_facturas df 
                         ON f.factura = df."id_factura" 
            LEFT OUTER JOIN ventas_dim_facturas dfo 
                         ON f.factura = dfo."id_factura_original" 
            LEFT OUTER JOIN ventas_dim_zonas dz 
                         ON f.zona = dz."zona" 
            LEFT OUTER JOIN ventas_dim_condicion_pago dcp 
                         ON f.condicion_pago = dcp."condicion_pago" 
            LEFT OUTER JOIN (SELECT To_char("codigo_cliente_facturacion") AS 
                                    codigo_cliente_facturacion, 
                                    Max("idcliente")                      AS 
                                    "idcliente" 
                             FROM   ventas_dim_clientes 
                             GROUP  BY To_char("codigo_cliente_facturacion")) dc 
                         ON f.cliente = dc.codigo_cliente_facturacion 
            LEFT OUTER JOIN ventas_dim_rutas dr 
                         ON f.ruta = dr."id_ruta" 
            LEFT OUTER JOIN ventas_dim_tipos tdf 
                         ON f.tipo_documento = tdf."id_tipo" 
            LEFT OUTER JOIN ventas_dim_tipos tdo 
                         ON f.tipo_original = tdo."id_tipo" 
            LEFT OUTER JOIN ventas_dim_usuarios du 
                         ON f.usuario = du."nombre_usuario" 
            LEFT OUTER JOIN ventas_dim_usuarios duanula 
                         ON f.usuario_anula = duanula."nombre_usuario" 
         ON fl.factura = f.factura 
       LEFT OUTER JOIN ventas_dim_articulos dart 
                    ON fl.articulo = dart."id_articulo" 
       LEFT OUTER JOIN ventas_dim_documentos dd 
                    ON fl.documento_origen = dd."id_documento" 
       LEFT OUTER JOIN ventas_dim_tipos dtdl 
                    ON fl.tipo_documento = dtdl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_tipos dtl 
                    ON fl.tipo_linea = dtl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_bodega bod 
                    ON fl.bodega = bod."bodega" 
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

  m_query := 
	'SELECT COUNT(*) 
FROM   coopegua.factura_linea fl 
       JOIN (SELECT fl.factura, 
                    fl.linea 
             FROM   coopegua.factura_linea fl 
                    JOIN (SELECT a.factura 
                          FROM   (SELECT factura, 
                                         Count(*) 
                                  FROM   coopegua.factura_linea 
                                  GROUP  BY factura) a 
                                 LEFT JOIN (SELECT df."id_factura", 
                                                   Count(*) 
                                            FROM 
                                 infografico.ventas_hechos_facturas 
                                 dhf 
                                 JOIN infografico.ventas_dim_facturas 
                                      df 
                                   ON dhf."idfactura" = 
                                      df."idfactura" 
                                            GROUP  BY df."id_factura") b 
                                        ON a.factura = b."id_factura" 
                          WHERE  b."id_factura" IS NULL) c 
                      ON fl.factura = c.factura 
             WHERE  fl.recorddate > (SELECT Max("factura_recorddate") 
                                     FROM   infografico.ventas_hechos_facturas)) 
            nuevos 
         ON fl.factura = nuevos.factura 
            AND fl.linea = nuevos.linea 
       JOIN coopegua.factura f 
            LEFT OUTER JOIN ventas_dim_consecutivos conse ON f.consecutivo = conse."id_consecutivo" 
            LEFT OUTER JOIN ventas_dim_pedidos dp ON f.pedido = dp."id_pedido" 
            LEFT OUTER JOIN ventas_dim_vendedor dv 
                         ON f.vendedor = dv."vendedor" 
            LEFT OUTER JOIN ventas_dim_documento_credito dcxc 
                         ON f.doc_credito_cxc = dcxc."id_documento_cxc" 
            LEFT OUTER JOIN ventas_dim_facturas df 
                         ON f.factura = df."id_factura" 
            LEFT OUTER JOIN ventas_dim_facturas dfo 
                         ON f.factura = dfo."id_factura_original" 
            LEFT OUTER JOIN ventas_dim_zonas dz 
                         ON f.zona = dz."zona" 
            LEFT OUTER JOIN ventas_dim_condicion_pago dcp 
                         ON f.condicion_pago = dcp."condicion_pago" 
            LEFT OUTER JOIN (SELECT To_char("codigo_cliente_facturacion") AS 
                                    codigo_cliente_facturacion, 
                                    Max("idcliente")                      AS 
                                    "idcliente" 
                             FROM   ventas_dim_clientes 
                             GROUP  BY To_char("codigo_cliente_facturacion")) dc 
                         ON f.cliente = dc.codigo_cliente_facturacion 
            LEFT OUTER JOIN ventas_dim_rutas dr 
                         ON f.ruta = dr."id_ruta" 
            LEFT OUTER JOIN ventas_dim_tipos tdf 
                         ON f.tipo_documento = tdf."id_tipo" 
            LEFT OUTER JOIN ventas_dim_tipos tdo 
                         ON f.tipo_original = tdo."id_tipo" 
            LEFT OUTER JOIN ventas_dim_usuarios du 
                         ON f.usuario = du."nombre_usuario" 
            LEFT OUTER JOIN ventas_dim_usuarios duanula 
                         ON f.usuario_anula = duanula."nombre_usuario" 
         ON fl.factura = f.factura 
       LEFT OUTER JOIN ventas_dim_articulos dart 
                    ON fl.articulo = dart."id_articulo" 
       LEFT OUTER JOIN ventas_dim_documentos dd 
                    ON fl.documento_origen = dd."id_documento" 
       LEFT OUTER JOIN ventas_dim_tipos dtdl 
                    ON fl.tipo_documento = dtdl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_tipos dtl 
                    ON fl.tipo_linea = dtl."id_tipo" 
       LEFT OUTER JOIN ventas_dim_bodega bod 
                    ON fl.bodega = bod."bodega" ';
  
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



select count(*) 
from (select "idpersona" from infografico."ventas_hechos_ubicaciones" group by "idpersona")
-- 100,942

select count(*) 
from infografico.ventas_dim_personas dp 
-- 125,790
-- 24,848 registros de personas sin ubicacion

SELECT dp.idpersona 
FROM   infografico.ventas_dim_personas dp 
       LEFT JOIN (SELECT "idpersona" 
                  FROM   infografico."ventas_hechos_ubicaciones" 
                  GROUP  BY "idpersona") hu 
              ON dp."idpersona" = hu."idpersona" 
WHERE  hu."idpersona" IS NULL 
-- 24,850 identificados los registros de dim_personas sin hubicaciones


SELECT count(*) 
FROM   infografico.ventas_dim_personas dp 
       LEFT JOIN (SELECT "idpersona" 
                  FROM   infografico."ventas_hechos_ubicaciones" 
                  GROUP  BY "idpersona") hu 
              ON dp."idpersona" = hu."idpersona" 
       JOIN infografico.vista_ubicaciones_en_cl clu 
         ON dp."identificacion" = clu.identificacion
WHERE  hu."idpersona" IS NULL 




