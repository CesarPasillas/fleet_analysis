# Rutas: inicio, fin y punto de retorno

Generado: 2026-10-12 12:53 · script `backend/scripts/detect_route_turnarounds.py`
Detalle por ruta: `rutas_inicio_fin.csv` · viajes: `viajes.csv` · escalas: `escalas.csv` · sitios: `sitios.csv` · mapa: `mapa_rutas.html`

## Alcance de los datos

- Assets 29563, 46917, 48811, 60753, 60755, 60757, 60793, 65191, 65193, 65195, 65197, 65201, 65210, 66184 del cliente 64.
- Con rutas: 14. Sin rutas (sin movimiento ≥ 2 km en todo el periodo): ninguno.
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

- Rutas detectadas: **785** · ida: 219 · regreso: 297 · trayecto: 228 · local: 41
- Regresos: **297** · retorno detectado en movimiento: 232 (confirmados por carretera: 165, solo geométricos: 67) · regreso tras parada larga: 65
- Regresos que vuelven a ≤ 5 km del origen de su ida: 147 de 297
- Distancia total: 127,709 km · mediana por ruta: 67.2 km · mediana de duración: 2.5 h
- Fines de ruta por motivo: parada larga: 544; retorno detectado: 232; fin de datos: 9

## Viajes (base → base)

- Viajes: **211** · regresan a una base: 203 · con escalas: 141
- Assets sin base clara (ningún sitio con ≥ 10% de sus inicios/fines; sus viajes juntan muchas rutas):
  65193
- Escalas dentro de viajes: 362 (sin consulta de POIs; correr con `--poi` para clasificarlas)

| tipo_escala | escalas |
|---|---|
| sin identificar | 224 |
| descanso nocturno | 137 |
| pernocta en base | 1 |

### Viajes más largos

| assetId | viaje_n | rutas | inicio | fin | sitio_origen | sitio_destino | sitio_fin | km | horas_total | regresa_a_base | escalas |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 60755 | 103 | 12–30 | 2024-01-12 08:28 | 2024-01-21 14:28 | S009 | S121 | S001 | 5153.3 | 222.0 | True | S027 sin identificar 0.2 h; S119 sin identificar 2.4 h; S120 descanso nocturno 7.7 h; S076 descanso nocturno 10.2 h; S077 descanso nocturno 15.7 h; S054 sin identificar 5.0 h; S077 sin identificar 0.3 h; S121 descanso nocturno 12.3 h; S122 sin identificar 2.1 h; S076 descanso nocturno 8.5 h; S014 sin identificar 0.5 h; S035 sin identificar 6.0 h; S014 sin identificar 1.1 h; S021 descanso nocturno 8.8 h; S015 descanso nocturno 11.7 h; S025 sin identificar 2.6 h; S026 descanso nocturno 8.2 h; S123 descanso nocturno 19.3 h |
| 65193 | 139 | 13–32 | 2024-01-12 09:26 | 2024-01-21 17:28 | S009 | S054 | S001 | 4945.4 | 224.0 | True | S139 descanso nocturno 5.7 h; S022 descanso nocturno 16.3 h; S140 sin identificar 7.9 h; S054 sin identificar 5.5 h; S060 descanso nocturno 12.9 h; S054 sin identificar 2.7 h; S060 sin identificar 3.3 h; S060 sin identificar 2.8 h; S088 descanso nocturno 8.8 h; S022 sin identificar 2.6 h; S056 descanso nocturno 9.3 h; S014 sin identificar 3.4 h; S035 sin identificar 0.5 h; S014 sin identificar 4.0 h; S021 descanso nocturno 7.5 h; S015 descanso nocturno 10.7 h; S025 sin identificar 3.3 h; S026 descanso nocturno 7.3 h |
| 60755 | 104 | 35–46 | 2024-01-23 05:29 | 2024-01-29 17:32 | S001 | S021 | S001 | 4525.2 | 156.1 | True | S019 sin identificar 2.1 h; S055 descanso nocturno 9.3 h; S049 sin identificar 2.9 h; S022 sin identificar 2.2 h; S078 descanso nocturno 7.3 h; S032 sin identificar 2.8 h; S032 sin identificar 2.6 h; S022 sin identificar 3.5 h; S021 descanso nocturno 7.7 h; S015 descanso nocturno 10.9 h; S026 descanso nocturno 7.2 h |
| 60757 | 109 | 30–47 | 2024-01-22 17:30 | 2024-01-30 18:49 | S009 | S021 | S001 | 4114.8 | 193.3 | True | S027 descanso nocturno 17.0 h; S126 descanso nocturno 6.3 h; S025 sin identificar 2.1 h; S015 sin identificar 2.6 h; S079 descanso nocturno 10.5 h; S080 sin identificar 3.3 h; S127 descanso nocturno 12.7 h; S080 sin identificar 2.2 h; S014 sin identificar 0.7 h; S056 descanso nocturno 10.0 h; S014 sin identificar 1.3 h; S035 sin identificar 1.5 h; S014 sin identificar 1.6 h; S021 descanso nocturno 12.3 h; S015 descanso nocturno 12.3 h; S128 sin identificar 2.1 h; S026 descanso nocturno 6.6 h |
| 65210 | 196 | 32–44 | 2024-01-19 05:10 | 2024-01-26 19:57 | S001 | S172 | S009 | 3547.9 | 182.8 | True | S170 sin identificar 6.0 h; S007 sin identificar 3.2 h; S046 descanso nocturno 9.9 h; S037 sin identificar 37.1 h; S171 descanso nocturno 4.5 h; S172 sin identificar 7.8 h; S173 descanso nocturno 9.0 h; S063 descanso nocturno 8.3 h; S097 sin identificar 0.5 h; S063 sin identificar 0.4 h; S097 sin identificar 0.4 h; S063 sin identificar 2.9 h |
| 65193 | 141 | 39–53 | 2024-01-25 06:59 | 2024-01-31 17:59 | S001 | S147 | S014 | 2894.0 | 155.0 | False | S025 descanso nocturno 5.2 h; S145 descanso nocturno 9.7 h; S146 sin identificar 2.7 h; S088 sin identificar 34.9 h; S147 sin identificar 2.0 h; S148 descanso nocturno 9.5 h; S089 sin identificar 0.2 h; S089 sin identificar 2.8 h; S149 sin identificar 2.9 h; S056 descanso nocturno 7.9 h; S014 sin identificar 3.3 h; S035 sin identificar 0.3 h; S014 sin identificar 0.3 h; S035 sin identificar 2.2 h |
| 65201 | 187 | 38–47 | 2024-01-22 12:03 | 2024-01-26 21:41 | S001 | S079 | S006 | 2853.6 | 105.6 | True | S162 descanso nocturno 10.0 h; S079 descanso nocturno 8.2 h; S163 sin identificar 0.7 h; S015 descanso nocturno 9.2 h; S164 descanso nocturno 5.2 h; S007 sin identificar 2.3 h; S044 sin identificar 0.7 h; S007 sin identificar 1.3 h |
| 60757 | 106 | 1–12 | 2023-12-31 18:00 | 2024-01-09 17:22 | S182 | S001 | S001 | 2583.3 | 215.4 | True | S021 sin identificar 94.5 h; S022 descanso nocturno 14.3 h; S032 sin identificar 0.1 h; S078 descanso nocturno 12.6 h; S032 sin identificar 3.1 h; S032 sin identificar 3.2 h; S022 sin identificar 3.8 h; S021 descanso nocturno 9.7 h; S015 descanso nocturno 9.2 h; S025 sin identificar 2.5 h; S026 descanso nocturno 8.4 h |
| 60755 | 102 | 1–8 | 2023-12-31 18:00 | 2024-01-08 16:20 | S181 | S075 | S009 | 2456.7 | 190.3 | True | S021 descanso nocturno 18.5 h; S022 sin identificar 51.0 h; S021 descanso nocturno 7.4 h; S015 descanso nocturno 10.1 h; S025 sin identificar 3.2 h; S026 descanso nocturno 9.0 h; S075 sin identificar 37.7 h |
| 65201 | 180 | 1–5 | 2023-12-31 18:02 | 2024-01-03 17:59 | S032 | S001 | S001 | 2244.0 | 72.0 | True | S022 descanso nocturno 11.4 h; S015 descanso nocturno 4.9 h; S025 sin identificar 2.4 h; S026 descanso nocturno 6.1 h |

## Por asset

| assetId | rutas | ida | regreso | trayecto | local | km_total | horas_mov | km_mediana_ruta |
|---|---|---|---|---|---|---|---|---|
| 29563 | 71 | 21 | 21 | 29 | 0 | 6618.7 | 181.2 | 129.7 |
| 46917 | 64 | 20 | 43 | 0 | 1 | 2467.8 | 47.3 | 32.4 |
| 48811 | 65 | 16 | 25 | 17 | 7 | 8212.1 | 209.0 | 17.7 |
| 60753 | 75 | 23 | 39 | 11 | 2 | 13093.9 | 295.4 | 9.9 |
| 60755 | 51 | 12 | 13 | 22 | 4 | 12673.2 | 274.8 | 155.5 |
| 60757 | 49 | 12 | 14 | 19 | 4 | 8146.1 | 197.9 | 62.2 |
| 60793 | 63 | 20 | 26 | 17 | 0 | 11678.5 | 301.0 | 106.9 |
| 65191 | 44 | 14 | 17 | 13 | 0 | 10642.8 | 227.3 | 203.2 |
| 65193 | 53 | 11 | 13 | 22 | 7 | 8556.7 | 217.5 | 17.0 |
| 65195 | 50 | 16 | 19 | 14 | 1 | 8038.2 | 186.7 | 115.4 |
| 65197 | 45 | 14 | 17 | 12 | 2 | 7773.1 | 213.9 | 103.0 |
| 65201 | 57 | 14 | 18 | 20 | 5 | 9931.6 | 240.3 | 87.9 |
| 65210 | 51 | 13 | 17 | 18 | 3 | 10994.5 | 290.2 | 128.2 |
| 66184 | 47 | 13 | 15 | 14 | 5 | 8882.2 | 234.5 | 163.8 |

## Sitios más visitados

| sitio | lat | lon | visitas |
|---|---|---|---|
| S001 | 20.5182 | -100.90082 | 329 |
| S002 | 17.97457 | -102.19045 | 48 |
| S003 | 19.26766 | -98.20245 | 46 |
| S004 | 19.5465 | -99.05228 | 44 |
| S005 | 17.94938 | -102.19935 | 44 |
| S006 | 19.03118 | -104.23321 | 42 |
| S007 | 19.07836 | -104.28387 | 30 |
| S008 | 25.67394 | -100.44374 | 30 |
| S009 | 25.60941 | -100.91429 | 30 |
| S010 | 25.51036 | -100.96189 | 27 |

## Pares origen → destino más frecuentes

| sitio_inicio | sitio_fin | rutas | assets | km_mediana | horas_mediana |
|---|---|---|---|---|---|
| S004 | S003 | 15 | 1 | 131.4 | 2.4 |
| S003 | S011 | 12 | 1 | 4.6 | 0.4 |
| S012 | S008 | 10 | 1 | 6.8 | 1.1 |
| S003 | S004 | 10 | 1 | 152.8 | 4.2 |
| S011 | S004 | 9 | 1 | 129.9 | 4.4 |
| S008 | S012 | 8 | 1 | 7.0 | 0.3 |
| S001 | S018 | 8 | 2 | 27.3 | 0.5 |
| S001 | S020 | 8 | 5 | 243.2 | 5.3 |
| S018 | S001 | 8 | 2 | 27.3 | 0.5 |
| S005 | S002 | 7 | 3 | 15.1 | 0.9 |
| S021 | S015 | 6 | 3 | 704.6 | 12.8 |
| S001 | S016 | 6 | 2 | 116.0 | 3.0 |
| S028 | S001 | 6 | 1 | 48.3 | 0.8 |
| S001 | S006 | 6 | 4 | 570.2 | 10.9 |
| S001 | S028 | 6 | 1 | 48.4 | 0.8 |

## Retornos más largos (ejemplos)

| assetId | ruta_n | tipo | inicio | fin | distancia_km | sitio_inicio | sitio_fin | retorno_metodo |
|---|---|---|---|---|---|---|---|---|
| 60757 | 12 | ida | 2024-01-09 05:48 | 2024-01-09 17:22 | 667.6 | S026 | S001 | carretera: QUERETARO - SALAMANCA (2180) |
| 66184 | 28 | ida | 2024-01-22 14:47 | 2024-01-23 00:43 | 579.5 | S178 | S048 | carretera: ENT. (AUT. URUAPAN - LAZARO CARDENAS) - LA ORILLA (2890) |
| 65193 | 31 | ida | 2024-01-21 05:06 | 2024-01-21 15:52 | 569.4 | S026 | S141 | carretera: IRAPUATO – LEÓN (2180) |
| 65195 | 24 | regreso | 2024-01-16 10:36 | 2024-01-16 20:52 | 538.3 | S037 | S051 | carretera: ARMERIA - MANZANILLO (DIRECTO) (140) |
| 60753 | 60 | ida | 2024-01-25 04:45 | 2024-01-25 15:34 | 494.5 | S017 | S008 | carretera: OJO CALIENTE - MONTERREY (2270) |
| 60753 | 10 | ida | 2024-01-06 04:48 | 2024-01-06 12:40 | 493.4 | S017 | S008 | carretera: OJO CALIENTE - MONTERREY (2270) |
| 66184 | 2 | ida | 2024-01-03 04:27 | 2024-01-03 14:39 | 444.8 | S005 | S001 | geométrico |
| 65195 | 12 | ida | 2024-01-13 05:21 | 2024-01-13 14:41 | 442.7 | S046 | S150 | carretera: AGUASCALIENTES - ENT. LUIS MOYA (70) |
| 65197 | 9 | ida | 2024-01-10 05:51 | 2024-01-10 18:19 | 424.3 | S013 | S073 | carretera: LIBRAMIENTO VERACRUZ (540-L) |
| 66184 | 33 | ida | 2024-01-24 05:13 | 2024-01-24 18:05 | 403.7 | S024 | S179 | geométrico |

## Limitaciones

- El catálogo de carreteras cubre la red federal; en ciudad el retorno se detecta solo por geometría.
- Un retorno por una vía distinta a la de ida (circuito) no repasa el camino previo, así que no se corta; esa ruta
  queda como `trayecto` hasta la siguiente parada larga.
- Los huecos de GPS (puntos sin coordenadas, sección 4) reducen la densidad de puntos; si el retorno cae en un hueco,
  el ápice se ubica en el último punto con coordenadas.
- Los umbrales están al inicio del script y se pueden ajustar.
