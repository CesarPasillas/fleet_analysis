# Rutas: inicio, fin y punto de retorno

Generado: 2026-10-11 19:53 · script `backend/scripts/detect_route_turnarounds.py`
Detalle por ruta: `rutas_inicio_fin.csv` · viajes: `viajes.csv` · escalas: `escalas.csv` · sitios: `sitios.csv` · mapa: `mapa_rutas.html`

## Alcance de los datos

- Lista de assets: `asset_id` (262 IDs). **Solo 16 tienen registros** en el snapshot (`merged_pos_eventtype`, cliente 64); los otros 246 no aparecen.
- Con rutas: 15. Sin rutas (sin movimiento ≥ 2 km en todo el periodo): 3001.
- Periodo: 2023-12-31 18:00 a 2024-01-31 17:59 (hora America/Mexico_City).

## Método

1. **Limpieza**: se quitan coordenadas nulas/0 y puntos que implican > 180 km/h desde el último punto válido.
2. **Paradas**: el vehículo permanece en un radio de 150 m por ≥ 5 min.
3. **Cortes duros** (separan bloques de movimiento): parada ≥ 2 h o hueco de datos ≥ 3 h.
4. **Retorno** (nueva ruta): dentro de cada bloque la trayectoria se remuestrea cada 250 m. Un punto
   "repasa" un punto previo si está a < 300 m y entre ambos se recorrió bastante más que esa separación.
   Si durante ≥ 3 km se repasa el camino previo **en sentido inverso**, hay retorno. El corte va en el
   ápice (el punto más lejano antes de empezar a regresar). Si hay una parada a ≤ 2 km del ápice, la ida
   termina al llegar a la parada y el regreso empieza al salir (`parada_destino_min`). Se repite en lo que queda
   del bloque, así que un bloque puede tener varias vueltas.
5. **Carreteras**: cada punto se empata con la vértice más cercana de `carreteras_raw` (≤ 300 m). Si sobre la misma
   carretera el kilometraje avanza antes del ápice y retrocede después, el retorno queda confirmado por carretera
   (`retorno_metodo = carretera: …`); si no (calles urbanas fuera del catálogo), queda `geométrico`.
6. **Sitios**: inicios y fines a ≤ 1.5 km se agrupan (DBSCAN) en sitios `S001…`, ordenados por visitas.
7. Rutas de menos de 2 km se descartan (maniobras en patio).

8. **Regreso tras parada larga**: si el vehículo llega al destino, se queda ≥ 2 h y después regresa, el
   corte ya lo hizo la parada. Ese trayecto se marca como regreso cuando ≥ 50% de sus puntos repasan el
   trayecto significativo anterior (o el previo a ese, si en medio hubo un tramo corto en el destino) en sentido
   inverso (correlación de Spearman ≤ −0.5 entre el avance de uno y del otro).

9. **Bases y viajes**: el rol de cada sitio se infiere del comportamiento de cada asset (`bases.csv`):
   **base** = sitio con ≥ 10% de sus inicios/fines (y ≥ 3 visitas); entre sus bases, la que tiene
   ≥ 3 estancias nocturnas y ≥ 60% de sus estancias nocturnas es **pernocta** (pensión) y las demás son
   **carga**; **centro de flota** = sitio donde ≥ 5 assets distintos se quedan ≥ 4 h (cuenta si el asset también
   se quedó ahí). Un viaje sale de una base y termina al llegar a una base. Excepción: carga → pernocta y, en
   ≤ 24 h, salida de la pernocta a un sitio que no es base = cargó, durmió en la pensión y salió a repartir:
   es un solo viaje y la noche en la pensión es una escala ("pernocta en base"). Una ruta base → base de
   ≤ 30 km es un **traslado** (columna `traslado`), no un viaje. `sitio_destino` es el fin de ruta más lejano.
10. **Escalas**: el tiempo entre el fin de una ruta y el inicio de la siguiente. Dentro de la escala se buscan las
   paradas de ≥ 15 min (puede haber varias, con movimientos cortos entre ellas) y, con `--poi`, se consulta
   en OSM qué hay a ≤ 300 m de cada una (`stop_detection`). `motor_apagado_pct` = % del tiempo entre
   IGNITION_OFF y el siguiente IGNITION_ON (el resto es ralentí o movimiento corto). `tipo_escala`, en este orden: escala en la pernocta del asset → pernocta en base; incluye las 02:00 y dura entre
   3 y 24 h → descanso nocturno; POI zona industrial/bodega → carga/descarga; hotel, tienda, farmacia u
   hospital → entrega probable; gasolinera → combustible/descanso; restaurante → alimentos; taller; si no, sin identificar.
   Es una lectura probable: un cliente que no está en OSM queda "sin identificar".

Tipos:
- **ida**: ruta que después se recorre de vuelta (por retorno detectado o tras parada larga).
- **regreso**: empieza donde los puntos empiezan a regresar; `ruta_ida_n` indica su ida.
- **trayecto**: ruta sin regreso por el mismo camino (termina en parada larga, hueco o fin de datos).
- **local**: movimiento que nunca se aleja más de 3 km de su inicio (patio, maniobras).

## Resultados globales

- Rutas detectadas: **640** · ida: 160 · regreso: 203 · trayecto: 247 · local: 30
- Regresos: **203** · retorno detectado en movimiento: 137 (confirmados por carretera: 77, solo geométricos: 60) · regreso tras parada larga: 66
- Regresos que vuelven a ≤ 5 km del origen de su ida: 84 de 203
- Distancia total: 128,336 km · mediana por ruta: 98.2 km · mediana de duración: 2.7 h
- Fines de ruta por motivo: parada larga: 495; retorno detectado: 137; fin de datos: 8

## Viajes (base → base)

- Viajes: **235** · regresan a una base: 229 · con escalas: 146
- Assets sin base clara (ningún sitio con ≥ 10% de sus inicios/fines; sus viajes juntan muchas rutas):
  2966, 2967
- Escalas dentro de viajes: 347 (sin consulta de POIs; correr con `--poi` para clasificarlas)

| tipo_escala | escalas |
|---|---|
| sin identificar | 228 |
| descanso nocturno | 114 |
| pernocta en base | 5 |

### Viajes más largos

| assetId | viaje_n | rutas | inicio | fin | sitio_origen | sitio_destino | sitio_fin | km | horas_total | regresa_a_base | escalas |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2973 | 140 | 15–32 | 2024-01-11 05:18 | 2024-01-19 19:25 | S001 | S042 | S001 | 5034.5 | 206.1 | True | S042 descanso nocturno 11.3 h; S076 sin identificar 1.1 h; S020 sin identificar 3.2 h; S150 descanso nocturno 6.6 h; S041 descanso nocturno 20.0 h; S020 descanso nocturno 7.5 h; S032 sin identificar 2.5 h; S151 sin identificar 8.4 h; S152 descanso nocturno 4.8 h; S054 descanso nocturno 10.6 h; S054 sin identificar 3.1 h; S153 sin identificar 0.3 h; S054 sin identificar 1.7 h; S025 descanso nocturno 8.2 h |
| 2969 | 80 | 46–55 | 2024-01-18 21:25 | 2024-01-25 11:54 | S001 | S024 | S001 | 4300.2 | 158.5 | True | S024 sin identificar 0.1 h; S118 descanso nocturno 6.0 h; S028 sin identificar 38.2 h; S040 sin identificar 0.6 h; S028 sin identificar 0.3 h; S040 sin identificar 0.4 h; S024 sin identificar 2.6 h; S056 descanso nocturno 5.2 h; S119 descanso nocturno 10.1 h |
| 2968 | 59 | 25–34 | 2024-01-13 05:02 | 2024-01-18 00:35 | S031 | S110 | S006 | 3331.4 | 115.5 | True | S108 descanso nocturno 7.1 h; S046 descanso nocturno 11.8 h; S051 sin identificar 0.7 h; S019 descanso nocturno 13.2 h; S051 sin identificar 1.4 h; S019 sin identificar 0.4 h; S051 sin identificar 0.9 h; S109 descanso nocturno 8.8 h; S110 sin identificar 6.4 h |
| 2967 | 51 | 32–52 | 2024-01-20 15:16 | 2024-01-30 10:53 | S001 | S002 | S002 | 2924.2 | 235.6 | True | S036 descanso nocturno 10.1 h; S100 sin identificar 6.5 h; S008 descanso nocturno 11.3 h; S024 sin identificar 26.0 h; S008 descanso nocturno 14.1 h; S063 sin identificar 2.2 h; S064 sin identificar 2.4 h; S034 descanso nocturno 10.6 h; S050 sin identificar 4.0 h; S101 descanso nocturno 11.4 h; S037 sin identificar 8.7 h; S025 descanso nocturno 14.2 h; S014 sin identificar 2.1 h; S014 descanso nocturno 7.9 h; S050 sin identificar 2.4 h; S035 sin identificar 2.5 h; S033 descanso nocturno 7.7 h; S061 sin identificar 4.6 h; S102 sin identificar 3.1 h; S103 descanso nocturno 8.0 h |
| 2971 | 116 | 42–47 | 2024-01-27 05:57 | 2024-01-30 16:37 | S016 | S038 | S002 | 2823.6 | 82.7 | True | S008 sin identificar 2.3 h; S030 descanso nocturno 7.4 h; S144 descanso nocturno 10.6 h; S038 sin identificar 2.8 h; S025 sin identificar 1.4 h |
| 2968 | 54 | 1–9 | 2024-01-01 12:55 | 2024-01-05 19:04 | S001 | S104 | S001 | 2566.6 | 102.2 | True | S048 descanso nocturno 7.0 h; S065 sin identificar 2.1 h; S104 descanso nocturno 15.3 h; S038 sin identificar 2.1 h; S025 sin identificar 4.0 h; S105 sin identificar 2.4 h; S020 descanso nocturno 12.0 h |
| 2970 | 93 | 29–35 | 2024-01-13 13:28 | 2024-01-16 19:46 | S007 | S074 | S007 | 2279.9 | 78.3 | True | S059 descanso nocturno 9.5 h; S047 descanso nocturno 13.1 h; S074 sin identificar 2.2 h; S074 sin identificar 3.6 h; S025 sin identificar 2.4 h; S131 sin identificar 2.0 h |
| 2969 | 78 | 30–43 | 2024-01-13 12:42 | 2024-01-17 16:11 | S011 | S052 | S001 | 2211.2 | 99.5 | True | S027 sin identificar 0.1 h; S115 descanso nocturno 12.5 h; S052 descanso nocturno 11.1 h; S070 sin identificar 2.7 h; S052 sin identificar 0.3 h; S070 sin identificar 2.2 h; S052 sin identificar 0.1 h; S039 sin identificar 0.4 h; S008 descanso nocturno 6.4 h; S024 sin identificar 9.8 h; S116 descanso nocturno 4.5 h |
| 2973 | 143 | 38–42 | 2024-01-25 07:19 | 2024-01-28 00:30 | S016 | S157 | S002 | 2207.8 | 65.2 | True | S156 descanso nocturno 6.4 h; S157 sin identificar 3.9 h; S158 descanso nocturno 10.7 h; S159 sin identificar 2.6 h |
| 2966 | 31 | 23–29 | 2024-01-18 05:56 | 2024-01-21 20:04 | S016 | S035 | S013 | 1924.1 | 86.1 | True | S008 sin identificar 2.9 h; S091 sin identificar 2.4 h; S034 sin identificar 2.5 h; S035 sin identificar 7.4 h; S033 descanso nocturno 18.8 h; S092 descanso nocturno 13.7 h |

## Por asset

| assetId | rutas | ida | regreso | trayecto | local | km_total | horas_mov | km_mediana_ruta |
|---|---|---|---|---|---|---|---|---|
| 2728 | 39 | 9 | 9 | 19 | 2 | 3237.9 | 88.0 | 25.8 |
| 2894 | 21 | 5 | 8 | 7 | 1 | 4999.7 | 100.0 | 140.7 |
| 2966 | 51 | 6 | 12 | 29 | 4 | 9468.2 | 204.9 | 95.3 |
| 2967 | 56 | 7 | 9 | 34 | 6 | 8114.0 | 202.2 | 95.1 |
| 2968 | 48 | 12 | 14 | 20 | 2 | 10430.1 | 225.5 | 104.2 |
| 2969 | 68 | 20 | 28 | 15 | 5 | 15514.0 | 363.4 | 121.6 |
| 2970 | 55 | 10 | 14 | 27 | 4 | 10892.1 | 227.6 | 119.3 |
| 2971 | 50 | 12 | 15 | 19 | 4 | 12941.4 | 279.7 | 113.8 |
| 2972 | 37 | 8 | 9 | 19 | 1 | 9026.9 | 198.5 | 156.0 |
| 2973 | 50 | 14 | 18 | 18 | 0 | 12425.4 | 274.0 | 113.0 |
| 2987 | 26 | 10 | 15 | 1 | 0 | 5131.5 | 88.2 | 210.0 |
| 2991 | 44 | 19 | 21 | 4 | 0 | 3852.0 | 107.7 | 29.6 |
| 2999 | 13 | 4 | 5 | 4 | 0 | 1513.8 | 35.7 | 8.6 |
| 3000 | 40 | 12 | 12 | 15 | 1 | 10083.4 | 211.5 | 208.2 |
| 3002 | 42 | 12 | 14 | 16 | 0 | 10705.4 | 257.5 | 223.0 |

## Sitios más visitados

| sitio | lat | lon | visitas |
|---|---|---|---|
| S001 | 20.51817 | -100.8963 | 141 |
| S002 | 31.29394 | -109.58063 | 59 |
| S003 | 20.51702 | -100.93679 | 48 |
| S004 | 30.98323 | -110.32185 | 46 |
| S005 | 32.53506 | -115.34674 | 38 |
| S006 | 30.79066 | -110.84152 | 36 |
| S007 | 19.08267 | -104.28054 | 34 |
| S008 | 23.19695 | -106.19638 | 32 |
| S009 | 30.48553 | -109.63938 | 26 |
| S011 | 25.6094 | -100.91459 | 24 |

## Pares origen → destino más frecuentes

| sitio_inicio | sitio_fin | rutas | assets | km_mediana | horas_mediana |
|---|---|---|---|---|---|
| S001 | S003 | 23 | 8 | 5.0 | 0.7 |
| S003 | S001 | 22 | 8 | 5.0 | 1.2 |
| S013 | S009 | 11 | 5 | 6.1 | 0.8 |
| S002 | S006 | 9 | 7 | 155.2 | 3.1 |
| S005 | S004 | 9 | 4 | 652.2 | 16.7 |
| S018 | S014 | 8 | 1 | 288.9 | 4.8 |
| S016 | S008 | 8 | 6 | 420.2 | 6.2 |
| S004 | S002 | 8 | 4 | 89.3 | 1.7 |
| S011 | S015 | 7 | 5 | 3.3 | 0.9 |
| S009 | S002 | 7 | 5 | 97.6 | 1.9 |
| S006 | S016 | 7 | 6 | 704.1 | 13.3 |
| S012 | S017 | 6 | 1 | 15.0 | 0.6 |
| S022 | S018 | 6 | 1 | 209.8 | 3.5 |
| S006 | S005 | 5 | 2 | 556.8 | 9.3 |
| S014 | S022 | 5 | 1 | 94.9 | 1.7 |

## Retornos más largos (ejemplos)

| assetId | ruta_n | tipo | inicio | fin | distancia_km | sitio_inicio | sitio_fin | retorno_metodo |
|---|---|---|---|---|---|---|---|---|
| 2973 | 16 | ida | 2024-01-12 09:19 | 2024-01-13 13:09 | 1174.6 | S042 | S076 | geométrico |
| 2969 | 65 | regreso | 2024-01-29 15:30 | 2024-01-30 12:50 | 840.1 | S040 | S007 | geométrico |
| 2969 | 46 | ida | 2024-01-18 21:25 | 2024-01-19 14:39 | 814.7 | S001 | S024 | geométrico |
| 2894 | 11 | ida | 2024-01-15 08:27 | 2024-01-15 21:38 | 652.3 | S005 | S004 | geométrico |
| 2894 | 8 | ida | 2024-01-12 07:33 | 2024-01-12 19:11 | 652.2 | S005 | S004 | geométrico |
| 2970 | 1 | ida | 2024-01-02 20:06 | 2024-01-03 11:42 | 560.0 | S001 | S007 | geométrico |
| 2968 | 27 | ida | 2024-01-15 07:43 | 2024-01-15 18:18 | 523.1 | S046 | S051 | geométrico |
| 2970 | 54 | ida | 2024-01-24 05:58 | 2024-01-24 16:25 | 501.9 | S007 | S135 | geométrico |
| 2970 | 23 | ida | 2024-01-11 09:23 | 2024-01-11 16:53 | 446.3 | S053 | S073 | geométrico |
| 2973 | 48 | ida | 2024-01-31 05:59 | 2024-01-31 12:34 | 422.4 | S016 | S008 | carretera: LIBRAMIENTO DE MAZATLAN (DIRECTO) (2585-L) |

## Limitaciones

- El catálogo de carreteras cubre la red federal; en ciudad el retorno se detecta solo por geometría.
- Un retorno por una vía distinta a la de ida (circuito) no repasa el camino previo, así que no se corta; esa ruta
  queda como `trayecto` hasta la siguiente parada larga.
- Los huecos de GPS (puntos sin coordenadas, sección 4) reducen la densidad de puntos; si el retorno cae en un hueco,
  el ápice se ubica en el último punto con coordenadas.
- Los umbrales están al inicio del script y se pueden ajustar.
