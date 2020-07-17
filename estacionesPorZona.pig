raw = LOAD '/uhadoop2020/project-group17/cr2_prDaily_2018_stations_ghcn.txt' USING PigStorage(',') AS (codigo_estacion, institucion, fuente, nombre, altura, latitud, longitud, codigo_cuenca, nombre_cuenca, codigo_subcuenca, nombre_subcuenca, inicio_obs, fin_obs, cantidad_obs, inicio_auto);
 
data = foreach raw generate codigo_estacion, latitud;

norteG = filter data by latitud <= -17.8 and latitud > -28;
norteC = filter data by latitud <= -28 and latitud > -32;
zonaC = filter data by latitud <= -32 and latitud >-37;
zonaS = filter data by latitud <= -37 and latitud >-43.5;
zonaA = filter data by latitud <= -43.5 and latitud >= -56;

zonaNorteG = foreach norteG generate codigo_estacion, 'Norte Grande' as zona;
zonaNorteC = foreach norteC generate codigo_estacion, 'Norte Chico' as zona;
zonaCentro = foreach zonaC generate codigo_estacion, 'Centro' as zona;
zonaSur = foreach zonaS generate codigo_estacion, 'Sur' as zona;
zonaAustral = foreach zonaA generate codigo_estacion, 'Austral' as zona;

estacionesPorZona = UNION zonaNorteG, zonaNorteC, zonaCentro, zonaSur, zonaAustral;

STORE estacionesPorZona INTO '/uhadoop2020/project-group17/estacionesPorZonas';  