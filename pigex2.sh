ID;DIA;CO (mg/m3);NO (ug/m3);NO2 (ug/m3);O3 (ug/m3);PM10 (ug/m3);SH2 (ug/m3);PM25 (ug/m3);PST (ug/m3);SO2 (ug/m3);PROVINCIA;ESTACIÓN

measure = load 'airQualityEsId.csv' using PigStorage(';') AS (id:int, dia:chararray, co:float, no:float, no2:float, o3:float, pm10:float, sh2:float, pm25:float, pst:float, so2:float, provincia:float, estacion:float);

# Guardar datos
store measure into
'pigResults/AirQuality2id';

# Seleccionar solo los gases
filter_gas = foreach measure generate id,co..o3,sh2,so2;
# Gaurdar los datos
store filter_gas into 'pigResults/AirQuality2gas';
# Seleccionar solo las particulas
filter_pm = foreach measure generate id,pm10,pm25;
# Guardar los datos
store filter_pm into 'pigResults/AirQuality2pm';
# Seleccionar lo demas
filter_rest = foreach measure generate id,dia,pst,provincia,estacion;
# Guardar los datos
store filter_rest into 'pigResults/AirQuality2rest';

