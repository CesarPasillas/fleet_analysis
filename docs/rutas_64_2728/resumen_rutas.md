# Rutas: inicio, fin y punto de retorno

Generado: 2026-10-11 19:11 · script `backend/scripts/detect_route_turnarounds.py`
Detalle por ruta: `rutas_inicio_fin.csv` · viajes: `viajes.csv` · escalas: `escalas.csv` · sitios: `sitios.csv` · mapa: `mapa_rutas.html`

## Alcance de los datos

- Asset 2728 del cliente 64.
- Con rutas: 1. Sin rutas (sin movimiento ≥ 2 km en todo el periodo): ninguno.
- Periodo: 2024-01-02 14:27 a 2024-01-31 14:49 (hora America/Mexico_City).

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

- Rutas detectadas: **39** · ida: 9 · regreso: 9 · trayecto: 19 · local: 2
- Regresos: **9** · retorno detectado en movimiento: 3 (confirmados por carretera: 1, solo geométricos: 2) · regreso tras parada larga: 6
- Regresos que vuelven a ≤ 5 km del origen de su ida: 6 de 9
- Distancia total: 3,238 km · mediana por ruta: 25.8 km · mediana de duración: 1.5 h
- Fines de ruta por motivo: parada larga: 36; retorno detectado: 3

## Viajes (base → base)

- Viajes: **9** · regresan a una base: 9 · con escalas: 9
- Assets sin base clara (ningún sitio con ≥ 10% de sus inicios/fines; sus viajes juntan muchas rutas):
  
- Escalas dentro de viajes: 21

| tipo_escala | escalas |
|---|---|
| sin identificar | 5 |
| descanso nocturno | 5 |
| combustible/descanso | 4 |
| carga/descarga | 3 |
| entrega probable | 2 |
| pernocta en base | 2 |

### Viajes más largos

| assetId | viaje_n | rutas | inicio | fin | sitio_origen | sitio_destino | sitio_fin | km | horas_total | regresa_a_base | escalas |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 2728 | 8 | 31–34 | 2024-01-25 18:40 | 2024-01-27 00:15 | S002 | S019 | S001 | 720.6 | 29.6 | True | S018 descanso nocturno 4.7 h; S019 entrega probable 7.2 h; S020 sin identificar 3.3 h |
| 2728 | 3 | 8–9 | 2024-01-05 05:14 | 2024-01-11 12:18 | S001 | S008 | S002 | 602.3 | 151.1 | True | S008 carga/descarga 138.2 h |
| 2728 | 5 | 19–23 | 2024-01-17 19:55 | 2024-01-19 21:05 | S002 | S015 | S002 | 576.3 | 49.2 | True | S001 pernocta en base 8.2 h; S013 combustible/descanso 2.2 h; S014 combustible/descanso 3.6 h; S015 descanso nocturno 21.7 h |
| 2728 | 4 | 13–17 | 2024-01-14 20:50 | 2024-01-16 18:51 | S001 | S011 | S001 | 456.2 | 46.0 | True | S009 descanso nocturno 8.2 h; S010 entrega probable 2.3 h; S011 combustible/descanso 4.5 h; S012 descanso nocturno 16.2 h |
| 2728 | 2 | 4–7 | 2024-01-03 16:32 | 2024-01-04 22:11 | S002 | S006 | S001 | 420.3 | 29.6 | True | S005 descanso nocturno 10.2 h; S006 carga/descarga 0.7 h; S007 carga/descarga 9.2 h |
| 2728 | 6 | 24–25 | 2024-01-22 13:26 | 2024-01-22 18:25 | S002 | S016 | S002 | 137.1 | 5.0 | True | S016 combustible/descanso 1.5 h |
| 2728 | 7 | 26–29 | 2024-01-23 18:38 | 2024-01-24 15:58 | S002 | S017 | S001 | 118.9 | 21.3 | True | S001 pernocta en base 9.5 h; S017 sin identificar 2.8 h; S003 sin identificar 3.0 h |
| 2728 | 1 | 1–2 | 2024-01-02 14:27 | 2024-01-02 17:37 | S002 | S004 | S001 | 44.5 | 3.2 | True | S004 sin identificar 1.1 h |
| 2728 | 9 | 36–37 | 2024-01-29 08:58 | 2024-01-30 15:28 | S002 | S003 | S001 | 22.3 | 30.5 | True | S003 sin identificar 28.1 h |

## Por asset

| assetId | rutas | ida | regreso | trayecto | local | km_total | horas_mov | km_mediana_ruta |
|---|---|---|---|---|---|---|---|---|
| 2728 | 39 | 9 | 9 | 19 | 2 | 3237.9 | 88.0 | 25.8 |

## Sitios más visitados

| sitio | lat | lon | visitas |
|---|---|---|---|
| S001 | 20.63486 | -103.33926 | 21 |
| S002 | 20.58515 | -103.28534 | 19 |
| S003 | 20.64816 | -103.34858 | 4 |
| S012 | 21.46591 | -104.86074 | 2 |
| S019 | 20.75346 | -105.44839 | 2 |
| S018 | 21.19559 | -105.09917 | 2 |
| S017 | 20.49555 | -103.48882 | 2 |
| S016 | 20.48646 | -103.55495 | 2 |
| S015 | 19.48553 | -104.64119 | 2 |
| S014 | 19.76553 | -104.35172 | 2 |

## Pares origen → destino más frecuentes

| sitio_inicio | sitio_fin | rutas | assets | km_mediana | horas_mediana |
|---|---|---|---|---|---|
| S001 | S002 | 6 | 1 | 15.0 | 0.6 |
| S002 | S001 | 5 | 1 | 15.0 | 0.8 |
| S003 | S001 | 2 | 1 | 5.3 | 1.0 |
| S008 | S002 | 1 | 1 | 301.6 | 4.8 |
| S019 | S020 | 1 | 1 | 29.6 | 1.3 |
| S018 | S019 | 1 | 1 | 75.4 | 1.5 |
| S017 | S003 | 1 | 1 | 25.8 | 2.2 |
| S016 | S002 | 1 | 1 | 60.1 | 1.6 |
| S015 | S002 | 1 | 1 | 280.3 | 5.6 |
| S014 | S015 | 1 | 1 | 59.1 | 1.2 |
| S013 | S014 | 1 | 1 | 45.2 | 2.4 |
| S012 | S001 | 1 | 1 | 231.1 | 8.0 |
| S011 | S012 | 1 | 1 | 8.6 | 2.8 |
| S010 | S011 | 1 | 1 | 7.3 | 0.4 |
| S009 | S010 | 1 | 1 | 24.0 | 0.4 |

## Retornos más largos (ejemplos)

| assetId | ruta_n | tipo | inicio | fin | distancia_km | sitio_inicio | sitio_fin | retorno_metodo |
|---|---|---|---|---|---|---|---|---|
| 2728 | 24 | ida | 2024-01-22 13:26 | 2024-01-22 15:17 | 77.0 | S002 | S016 | geométrico |
| 2728 | 5 | ida | 2024-01-04 05:19 | 2024-01-04 08:35 | 73.5 | S005 | S006 | carretera: EL TRAPICHE - COLIMA (410) |
| 2728 | 1 | ida | 2024-01-02 14:27 | 2024-01-02 15:29 | 18.0 | S002 | S004 | geométrico |

## Limitaciones

- El catálogo de carreteras cubre la red federal; en ciudad el retorno se detecta solo por geometría.
- Un retorno por una vía distinta a la de ida (circuito) no repasa el camino previo, así que no se corta; esa ruta
  queda como `trayecto` hasta la siguiente parada larga.
- Los huecos de GPS (puntos sin coordenadas, sección 4) reducen la densidad de puntos; si el retorno cae en un hueco,
  el ápice se ubica en el último punto con coordenadas.
- Los umbrales están al inicio del script y se pueden ajustar.
