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



