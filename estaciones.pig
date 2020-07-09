


raw = LOAD '/uhadoop2020/crahumadao/part-00000' USING PigStorage('\t') AS (codigo_estacion, anho, mes,dia,precipitacion_media);

-- para filtrar anho 
raw = filter raw by anho >= 1900;
-- concatenar para tener id codigo_estacion + anho
por_anho_raw_d = foreach raw generate CONCAT(codigo_estacion,'##',anho), dia;

-- agrupar por id codigo+anho
por_anho = GROUP por_anho_raw_d BY $0; 

-- genera la cuenta de días
por_anho_agregado_d = foreach por_anho generate $0 as estanho, COUNT($1) as count_anho;

-- cuenta las filas 
num_anho_agregado_d = FOREACH (GROUP por_anho_agregado_d ALL) GENERATE COUNT(por_anho_agregado_d);
-- se filtran los anhos para ver cualaes tienen al menos 95%
por_anho_filtrado_d = filter por_anho_agregado_d by count_anho >= 346;
-- cuenta las filas 
num_anho_filtrado_d = FOREACH (GROUP por_anho_filtrado_d ALL) GENERATE COUNT(por_anho_filtrado_d);

--genera ahora para calcular la acumulada precipitacion_media
por_anho_raw_p = foreach raw generate CONCAT(codigo_estacion,'##',anho), precipitacion_media;
-- agrupa por id estacion+anho
por_anho = GROUP por_anho_raw_p BY $0;

-- genera la suma de la precipitacion media por anho
por_anho_agregado_p = foreach por_anho generate group as estanho, SUM(por_anho_raw_p.precipitacion_media);

-- junta ambas tablas y queda solo las estaciones filtradas junto con la precipitacion ( además los días)
joined_a_p = join por_anho_filtrado_d by estanho, por_anho_agregado_p by estanho;
--se quita el id duplicado
tabla_final = foreach joined_a_p generate $0 as estanho, $1 as dias, $3 as prec_acumulada;
-- para guardar la tabla
STORE tabla_final INTO '/uhadoop2020/crahumadao/acumuladaanho';



