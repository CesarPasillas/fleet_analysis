# Metodologia algoritmica para deteccion y prediccion de rutas en Fleet Analysis

## Resumen
Este documento describe, en formato academico, la arquitectura algoritmica implementada en el sistema Fleet Analysis. El sistema combina dos enfoques: (i) algoritmos geoespaciales y de grafos para deteccion y reconstruccion de rutas, y (ii) un modelo de aprendizaje automatico supervisado para clasificacion de ruta. El resultado es una solucion hibrida donde la geometria de ruta se obtiene principalmente por metodos deterministas, mientras que la etiqueta de ruta se obtiene mediante una red neuronal multicapa.

## 1. Introduccion
El problema operativo consiste en transformar telemetria vehicular (eventos, posicion GPS y marcas de tiempo) en dos salidas:

1. Rutas completas entre eventos de inicio y fin.
2. Prediccion de clase de ruta probable para apoyo analitico.

Desde el punto de vista metodologico, el sistema no usa un unico algoritmo de ML para todo el flujo. La deteccion espacial y temporal de rutas se resuelve con logica algoritmica clasica. El componente de ML se usa en una etapa especifica de clasificacion.

## 2. Datos y preprocesamiento
El pipeline trabaja sobre datos POS, catalogo de tipos de evento y geometria vial.

- Los eventos de interes para una ruta completa son:
  - inicio: eventType = 519
  - fin: eventType = 520
- El timestamp operativo se define priorizando GPS timestamp y, cuando falta, server timestamp.
- Los registros se ordenan cronologicamente para mantener coherencia temporal.

La conversion de tiempo incluye heuristicas de unidad (s, ms, us, ns) para evitar errores de escala y overflow numerico en fuentes heterogeneas.

## 3. Deteccion de rutas completas
La deteccion de rutas usa un escaneo secuencial sobre la serie temporal:

1. Al encontrar 519 se abre un segmento.
2. Al encontrar 520 se cierra el segmento y se marca como ruta completa.
3. Si aparece 520 sin 519 previo, se marca inconsistencia (fin_sin_inicio).
4. Si termina la serie con 519 abierto, se marca inconsistencia (inicio_sin_fin).

Este procedimiento es una maquina de estados finitos con complejidad lineal respecto al numero de registros del activo-cliente analizado.

## 4. Algoritmo de correspondencia a carreteras (map matching)
Cada punto GPS se asocia a la carretera mas cercana mediante distancia punto-polilinea:

- Se usa distancia geodesica aproximada sobre segmentos de polilinea.
- Se aplica un radio estricto inicial y, si falla, un radio de respaldo para escenarios GPS ruidosos o escasos.

Formalmente, para un punto p y un conjunto de carreteras R, se selecciona:

p -> arg min d(p, r), r en R, sujeto a d(p, r) <= tau

con tau estricto y tau de respaldo.

## 5. Inferencia de carreteras por interpolacion
Para mejorar cobertura en trayectorias con baja frecuencia GPS, el sistema interpola muestras entre puntos consecutivos y vuelve a aplicar map matching. Luego agrega etiquetas por frecuencia y umbrales de presencia.

Este mecanismo incrementa robustez semantica de las carreteras inferidas sin depender de un modelo entrenado adicional.

## 6. Reconstruccion de ruta por grafo
La reconstruccion geometrica usa un grafo vial:

- Nodos: vertices de polilineas viales (con muestreo para reducir cardinalidad).
- Aristas: tramos consecutivos, ponderados por distancia.
- Conectores opcionales: enlaces de cercania con penalizacion para mejorar conectividad.
- Subgrafo local: seleccion de carreteras dentro de una ventana espacial alrededor del segmento para reducir costo computacional.

### 6.1 Camino minimo
El calculo de ruta entre nodos usa Dijkstra (camino de costo minimo no negativo):

C(P) = sum w(e)

Se minimiza C(P) entre nodo origen y nodo destino.

### 6.2 Estrategia por anclas
Si falla la conexion directa origen-destino, el algoritmo usa anclas intermedias derivadas de puntos GPS y concatena caminos minimos por tramos. Esta estrategia reduce fallos en redes parcialmente desconectadas o con sampling irregular.

### 6.3 Optimizaciones de rendimiento
Para controlar latencia, el sistema aplica:

- Muestreo de vertices del grafo (stride).
- Limite superior de nodos para generar conectores.
- Limite de numero de rutas completas que reciben reconstruccion de grafo.
- Cache de subgrafo local por caja espacial (bounding box).

## 7. Priorizacion de ruta principal
Las rutas completas se ordenan por criterios multi-factor:

1. mayor road_match_ratio
2. mayor cantidad de carreteras asociadas
3. mayor cantidad de puntos GPS
4. mayor cantidad de eventos
5. mayor distancia

La primera ruta ordenada se marca como ruta principal para consumo de frontend y analisis.

## 8. Componente de aprendizaje automatico
El modulo de ML clasifica la ruta esperada a partir de variables agregadas:

- distance_km
- traffic_level
- weather_index
- hour_of_day
- day_of_week
- vehicle_load_kg

### 8.1 Algoritmo utilizado
El algoritmo de ML utilizado es una red neuronal feed-forward tipo MLP (Multilayer Perceptron) implementada con TensorFlow/Keras:

- Capa de entrada: dimension de features
- Capa oculta 1: Dense(64, ReLU)
- Capa oculta 2: Dense(32, ReLU)
- Capa de salida: Dense(K, Softmax)
- Optimizador: Adam
- Funcion de perdida: sparse_categorical_crossentropy

Antes del entrenamiento se aplica estandarizacion (StandardScaler), se divide en train/test y se valida sobre holdout.

### 8.2 Naturaleza del modelo
Este modelo no reconstruye geometria vial. Su funcion es clasificar una etiqueta de ruta (route_id). Por tanto, en la arquitectura actual:

- Geometria de trayecto: algoritmos de grafo y heuristicas geoespaciales.
- Clase de ruta: modelo MLP supervisado.

## 9. Respuesta explicita a la pregunta de algoritmo
El sistema usa una arquitectura hibrida. Los algoritmos principales son:

1. Dijkstra para camino minimo en el grafo vial.
2. Map matching por distancia punto-polilinea.
3. Interpolacion de puntos para inferencia de carreteras.
4. MLP (red neuronal multicapa) para clasificacion de ruta.

Si la pregunta es especificamente por ML, el algoritmo de ML es MLP con capas densas y salida Softmax.

## 10. Discusion tecnica
La ventaja del enfoque hibrido es su interpretabilidad operativa: el trazado espacial se puede auditar por carretera y punto GPS, mientras que la clasificacion provee una capa probabilistica para decidir ruta esperada.

La limitacion principal es que, a diferencia de navegadores comerciales, el sistema no integra todavia restricciones de trafico en tiempo real, sentido vial legal ni costos avanzados de maniobra. Sin embargo, la base algoritmica actual permite extender estos elementos sin reemplazar el pipeline completo.

## 11. Conclusiones
1. El sistema si utiliza ML, pero no para todo el flujo.
2. La deteccion y reconstruccion de ruta son principalmente algoritmos geoespaciales y de grafos.
3. El ML actual corresponde a una red neuronal MLP para clasificacion de ruta.
4. El diseno hibrido es adecuado para escenarios industriales donde se requiere balance entre robustez geometrica y prediccion estadistica.
