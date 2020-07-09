


raw = LOAD '/uhadoop2020/crahumadao/part-00000' USING PigStorage('\t') AS (codigo_estacion, anho, mes,dia,precipitacion_media);

-- para filtrar anho 
raw = filter raw by anho >= 1900;
-- concatenar para tener id codigo_estacion + anho+mes
por_mes_raw_d = foreach raw generate CONCAT(codigo_estacion,'##',anho,'##',mes), dia;

-- agrupar por id codigo+anho+mes
por_mes = GROUP por_mes_raw_d BY $0; 

-- genera la cuenta de días
por_mes_agregado_d = foreach por_mes generate $0 as estmes, COUNT($1) as count_mes;

-- cuenta las filas 
num_mes_agregado_d = FOREACH (GROUP por_mes_agregado_d ALL) GENERATE COUNT(por_mes_agregado_d);


--genera ahora para calcular la acumulada precipitacion_media
por_mes_raw_p = foreach raw generate CONCAT(codigo_estacion,'##',anho,'##',mes), precipitacion_media;
-- agrupa por id estacion+mes
por_mes = GROUP por_mes_raw_p BY $0;

-- genera la suma de la precipitacion media por mes
por_mes_agregado_p = foreach por_mes generate group as estmes, SUM(por_mes_raw_p.precipitacion_media);

-- junta ambas tablas y queda solo las estaciones filtradas junto con la precipitacion ( además los días)
joined_a_p = join por_mes_agregado_d by estmes, por_mes_agregado_p by estmes;
--se quita el id duplicado
tabla_final = foreach joined_a_p generate $0 as estmes, $1 as dias, $3 as prec_acumulada;
-- para guardar la tabla
STORE tabla_final INTO '/uhadoop2020/crahumadao/acumuladames';



