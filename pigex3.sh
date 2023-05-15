# Cargar resultados
gases = load 'pigResults/AirQuality2gas' using PigStorage(';') AS (id:int, co:float, no:float, no2:float, o3:float, sh2:float, so2:float);

pm = load 'pigResults/AirQuality2pm' using PigStorage(';') AS (id:int, pm10:float, pm25:float);

rest = load 'pigResults/AirQuality2rest' using PigStorage(';') AS (id:int, dia:chararray, pst:float, provincia:float, estacion:float);

# Reunion natural
result = join gases by (id,co), pm by (id,pm10), rest by (id,provincia);
# Para cada uno
# -------------------------------------------------------------------------------------
filtradoGases = foreach gases generate id, co;
filtradoParticulas = foreach parts generate id, pm10;
filtradoResto = foreach rest_data generate id, province;

-- Reunir los datos de los tres flujos en uno (poner los datos juntos) a través de la característica que los une (ID)

filtradosJuntos = JOIN filtradoGases BY id, filtradoParticulas BY id, filtradoResto BY id;

-- Crear grupos (poner juntas) todas las muestras que tengan la misma provincia (el mismo valor en la característica)

agrupados = GROUP filtradosJuntos BY province;

-- Para cada grupo creado, generar una relación que contenga al representante de grupo (la provincia) y los máximos de los valores de CO y PM10 en las muestras del grupo 

final = foreach agrupados generate group, MAX(filtradosJuntos.filtradoGases::co), MAX(filtradosJuntos.filtradoParticulas::pm10);

-- Almacenar el resultado

store final into 'pigResults/other_data' using PigStorage(';');

