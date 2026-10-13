# Validación con eventos de entrega

Generado: 2026-10-12 12:56 · script `backend/scripts/validate_delivery_events.py`
Rutas y escalas: `rutas_inicio_fin.csv`, `escalas.csv` (detect_route_turnarounds.py) · eventos: `eventos_entrega_clasificados.csv`

Cliente 64 · assets: 29563, 46917, 48811, 60753, 60755, 60757, 60793, 65191, 65193, 65195, 65197, 65201, 65210, 66184 · eventos en el periodo: 19,146

## ¿Las entregas ocurren con el camión detenido?

Si los eventos fueran reales, casi ninguno debería caer **en movimiento**. Si cayeran al azar en el tiempo,
el % en movimiento sería parecido al % del tiempo que el camión se mueve.

- Prueba directa con el GPS (no depende de cómo se cortaron las rutas): en ±5 min alrededor del evento, ¿el
  camión se movió menos de 150 m? Eventos DELIVERY_STARTED + ORDER_DELIVERED con camión **detenido: 48.0%**
  (con GPS suficiente: 10,459 de 10,459). En 3,000 momentos al azar por asset: **73.7%** detenido.
- Con las rutas detectadas: 50.1% de los eventos caen dentro de una ruta con el camión en movimiento; el camión
  pasa en ruta el 32.4% del tiempo (las rutas incluyen paradas cortas de menos de 2 h).

Por tipo de evento (% de cada evento por estado del camión):

| evento | detenido en base | detenido en escala | en ruta, en movimiento | parada corta en ruta | total |
|---|---|---|---|---|---|
| DELIVERY_COMPLETED | 5.1 | 2.6 | 84.6 | 7.7 | 39 |
| DELIVERY_FAILED | 0.0 | 0.0 | 100.0 | 0.0 | 6214 |
| DELIVERY_STARTED | 37.1 | 25.4 | 23.7 | 13.8 | 2681 |
| ORDER_DELIVERED | 14.0 | 14.9 | 59.2 | 11.9 | 7778 |
| ORDER_EN_ROUTE | 0.0 | 0.0 | 100.0 | 0.0 | 2434 |

## Por asset (DELIVERY_STARTED + ORDER_DELIVERED)

`razón escala obs/esp` > 1: los eventos se concentran en las escalas más de lo que daría el azar.

| assetId | eventos | % detenido (GPS) | % detenido al azar | % en ruta en movimiento | % tiempo en ruta | % en escala | % tiempo en escala | % en base | % tiempo en base | razón escala obs/esp |
|---|---|---|---|---|---|---|---|---|---|---|
| 29563 | 209 | 81.3 | 80.2 | 15.3 | 25.8 | 9.1 | 23.7 | 54.1 | 50.5 | 0.38 |
| 46917 | 207 | 87.4 | 93.4 | 8.7 | 6.8 | 0.0 | 0.0 | 87.0 | 93.2 | 0.0 |
| 48811 | 718 | 37.6 | 76.5 | 58.9 | 30.5 | 14.5 | 30.9 | 14.1 | 38.6 | 0.47 |
| 60753 | 716 | 23.3 | 67.2 | 75.1 | 41.7 | 3.1 | 8.6 | 11.2 | 49.6 | 0.36 |
| 60755 | 793 | 26.2 | 69.8 | 73.0 | 36.9 | 11.1 | 42.8 | 5.5 | 20.3 | 0.26 |
| 60757 | 641 | 36.0 | 79.1 | 62.4 | 26.9 | 16.4 | 39.7 | 12.3 | 33.4 | 0.41 |
| 60793 | 762 | 52.1 | 68.6 | 46.2 | 43.3 | 17.7 | 31.1 | 20.1 | 25.6 | 0.57 |
| 65191 | 1130 | 49.4 | 71.8 | 48.9 | 32.1 | 20.4 | 29.8 | 23.4 | 38.2 | 0.68 |
| 65193 | 707 | 68.5 | 72.7 | 29.3 | 36.0 | 29.3 | 38.0 | 26.9 | 26.0 | 0.77 |
| 65195 | 960 | 58.1 | 70.7 | 39.5 | 34.0 | 13.2 | 14.4 | 35.9 | 51.5 | 0.92 |
| 65197 | 219 | 64.4 | 69.3 | 34.2 | 30.4 | 17.4 | 16.9 | 22.4 | 52.8 | 1.03 |
| 65201 | 897 | 49.6 | 73.6 | 49.5 | 32.5 | 19.3 | 28.7 | 21.2 | 38.9 | 0.67 |
| 65210 | 1130 | 43.0 | 64.9 | 55.8 | 45.5 | 16.9 | 26.5 | 9.5 | 28.0 | 0.64 |
| 66184 | 1370 | 52.8 | 74.6 | 44.7 | 33.3 | 29.4 | 47.7 | 13.5 | 19.0 | 0.62 |

## Escalas dentro de viajes con al menos una entrega

| tipo_escala | escalas | con_entrega | pct |
|---|---|---|---|
| sin identificar | 224 | 182 | 81.2 |
| descanso nocturno | 137 | 134 | 97.8 |
| pernocta en base | 1 | 0 | 0.0 |

## Lectura

- Si el % detenido de los eventos es parecido al % detenido al azar y la razón obs/esp ≈ 1, los eventos
  de entrega no siguen las paradas del camión y **no sirven como verdad de terreno**.
- Si el % detenido es mucho mayor que al azar y la razón > 1, los eventos confirman las escalas detectadas y
  sirven para medir qué tan bien se clasifican.

## Prueba adicional: ¿hay una parada cerca del evento? (2026-10-12)

Por si el sistema registra la entrega un poco antes de llegar o al arrancar después de entregar: ¿hubo una
parada (≥ 5 min quieto en ≤ 150 m) en los 20 min **antes** o **después** del evento? Promedio de los 14 assets,
comparado con 1,000 momentos al azar por asset:

| evento | parada en los 20 min antes | parada en los 20 min después |
|---|---|---|
| momento al azar | 78.8% | 78.6% |
| DELIVERY_STARTED | 58.3% | 59.7% |
| ORDER_DELIVERED | 54.1% | 50.7% |

## Conclusión

Los eventos de entrega **no sirven como verdad de terreno** para estos assets:

- Con el camión detenido: 48% de los eventos contra 74% de los momentos al azar. Caen **menos** en paradas que el azar.
- Ni con margen de ±20 min se asocian a paradas (54–60% contra 79% al azar).
- DELIVERY_FAILED y ORDER_EN_ROUTE caen 100% en movimiento, y el 98% de los descansos nocturnos tendría "entregas".
- Volumen irreal: 15–35 entregas por camión por día en camiones que hacen viajes de cientos de km.

Es el mismo patrón que otros eventos del dataset (p. ej. 11,146 FORWARD_COLLISION en un mes en el asset 2728):
parecen generados sin relación con lo que hace el vehículo. La validación del método se apoya entonces en la
revisión manual de sitios (Google Maps) y en la consistencia del comportamiento (p. ej. pernocta: 9 de 10
estancias nocturnas en la Pensión La Llave, confirmada manualmente).
