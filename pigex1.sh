# Clase inicial
mkdir /var/tmp/materialPig
hdfs dfs -mkdir input
hdfs dfs -put
/var/tmp/materialPig/airQualityEs.csv input
hdfs dfs -ls input
# Entrar a pig
pig
# Cargar datos
measure = load 'input/airQualityEs.csv'
using PigStorage(';') AS (date:chararray,
co:float, no:float, no2:float, o3:float,
pm10:float, sh2:float, pm25:float,
pst:float, so2:float, province:chararray,
station:chararray);
# Guardar datos
store measure into
'pigResults/AirQualityProcessed';
hdfs dfs -cat
pigResults/AirQualityProcessed/part-m-
00000 | less
store measure into
'pigResults/AirQualityProcessed2' using
PigStorage (',');
hdfs dfs -cat
pigResults/AirQualityProcessed2/part-m-
00000 | less
dump measure;
Localizacion = foreach measure generate
province, station;
# Group by
filter_measure = filter measure by date != 'DIA';
measure_by_province = group filter_measure by
province;
num_measures_by_province = foreach
measure_by_province generate group,
AVG(filter_measure.co) as measure;
store num_measures_by_province into
'pigResults/AirQualityProcessed3';
hdfs dfs -cat
pigResults/AirQualityProcessed3/part-r-
00000 | less
# Comprobar resultados
hdfs dfs -cat
pigResults/AirQualityProcessed3/part-r-
00000 | less
meas
