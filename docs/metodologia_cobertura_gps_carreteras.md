# Metodología: Análisis de Cobertura GPS por Carretera

**Proyecto:** Fleet Analysis — PhD  
**Módulo:** Detección de zonas sin cobertura GPS y clasificación por severidad en red vial

---

## 1. Problema que resuelve

Los dispositivos GPS instalados en vehículos de carga registran su posición continuamente. Cuando el vehículo entra en una zona sin cobertura celular (montaña, túnel, área rural sin infraestructura), el dispositivo pierde la señal y no puede transmitir. El registro queda sin coordenadas (`latitude = NULL`).

El objetivo es identificar **qué zonas geográficas** y **qué tramos de carretera** concentran esta pérdida de señal, usando el historial de toda la flota como evidencia colectiva.

---

## 2. Detección de registros sin GPS

Cada registro en la tabla `merged_pos_eventtype` contiene:

| Campo | Descripción |
|---|---|
| `assetId` | ID del vehículo |
| `clientId` | ID de la empresa propietaria |
| `serverTimestamp` | Cuándo llegó el dato al servidor |
| `gpsTimestamp` | Cuándo tomó la posición el GPS |
| `latitude` / `longitude` | Coordenadas. **NULL cuando no hay señal** |

Un registro se considera **sin cobertura GPS** cuando `latitude IS NULL` o `longitude IS NULL` o `ABS(latitude) < 0.01` (coordenada cero por defecto del dispositivo).

En el dataset completo: **17.1 millones de registros** sin GPS de un total de 806 millones (2.1%).

---

## 3. Interpolación de posición aproximada

Un registro sin GPS no tiene coordenadas, pero el vehículo *estaba en algún lugar*. Para ubicarlo geográficamente se usa la técnica de **islands (islas)**:

1. Se ordenan todos los registros de cada asset por `serverTimestamp`.
2. Se cuentan los registros GPS válidos acumulados por asset (`valid_group`).
3. Cada registro sin GPS hereda la posición del **último GPS válido anterior** del mismo asset.

```
Tiempo →    [GPS: 22.01,-100.84]  [NULL]  [NULL]  [GPS: 22.03,-100.82]
valid_group        1                 1       1            2
posición aprox.  22.01,-100.84  22.01,-100.84  22.01,-100.84  22.03,-100.82
```

Esto coloca cada pérdida de señal en el último punto conocido antes de que desapareciera la cobertura — la mejor aproximación disponible sin más información.

---

## 4. Grilla geográfica de 1 km

Las posiciones aproximadas se agrupan en una **grilla regular de 0.01° × 0.01°**, equivalente a aproximadamente **1 km² por celda** en latitudes de México (~19°–32° N).

Fórmula de asignación de celda:
```
grid_lat = ROUND(ROUND(lat / 0.01) * 0.01, 5)
grid_lon = ROUND(ROUND(lon / 0.01) * 0.01, 5)
```

Cada celda acumula:
- Conjunto de `assetId` distintos que perdieron señal ahí
- Conjunto de `clientId` distintos
- Conteo total de eventos (registros sin GPS)

---

## 5. Clasificación de hotspots

Una vez acumuladas todas las celdas con pérdida de señal, se clasifican:

| Categoría | Criterio | Significado |
|---|---|---|
| **Zona sin cobertura (estructural)** | ≥ 2 assets distintos | El problema es la zona geográfica, no un dispositivo. Múltiples vehículos independientes lo confirman. |
| **Evento aislado** | 1 asset | Solo un vehículo tuvo pérdida ahí. Puede ser falla del dispositivo, ruta poco transitada, o zona realmente sin cobertura pero con poco tráfico. |

### Campos de cada hotspot

| Campo | Descripción |
|---|---|
| **lat / lon** | Centro de la celda de 1 km² |
| **Assets** | Vehículos únicos que perdieron señal en esa celda |
| **Clientes** | Empresas distintas a las que pertenecen esos vehículos |
| **Eventos** | Total de registros GPS nulos en esa celda (incluye repeticiones) |
| **Categoría** | `zona_sin_cobertura` o `evento_aislado` |

**Ejemplo — zona estructural:**
> ⛔ Zona sin cobertura  
> Assets: 523 · Clientes: 9 · Eventos: 8,804  
> → 523 vehículos de 9 empresas distintas perdieron señal en ese kilómetro. Confirmado como problema de infraestructura celular.

**Ejemplo — evento aislado:**
> ⚡ Evento aislado  
> Asset: 81400 · Eventos: 71  
> → Solo el vehículo 81400 perdió señal en esa celda, y lo hizo 71 veces. Puede significar:
> - Ruta poco transitada por otros vehículos (el problema es real pero solo ese asset pasa por ahí)
> - Falla intermitente del dispositivo GPS de ese vehículo específico
> - Zona con cobertura marginal donde solo ciertos dispositivos fallan
>
> En el evento aislado **no se muestra "Clientes"** porque con un solo asset el dato es redundante (siempre sería 1). Se muestra directamente el `assetId` para poder investigar si el patrón se repite en ese vehículo en otras zonas.

---

## 6. Match con catálogo de carreteras

El catálogo `carreteras_raw` contiene 935 segmentos viales de la red nacional, cada uno con:
- `name` — nombre oficial del tramo
- `dataset` — clasificación (TIPO_A, TIPO_B, TIPO_C, TIPO_D)
- `p_type` — subtipo (A2, A4, B2, C, D...)
- `p_longitud` — longitud en km
- `geometry` — geometría en formato WKT LINESTRING

### Algoritmo de match

1. Se parsean todos los LINESTRING y se discretizan sus puntos en celdas de 0.01°.
2. Se construye un índice inverso: `celda → conjunto de nombres de carretera`.
3. Para cada hotspot, se busca en su celda y las **8 celdas vecinas** (radio de ~1-2 km) qué carreteras están presentes.
4. Se acumulan por carretera: conteo de zonas estructurales, eventos aislados y eventos totales.

Las 8 celdas vecinas se incluyen para compensar la discretización: si la carretera pasa por el borde de una celda, el hotspot podría quedar en la celda adyacente.

---

## 7. Clasificación de severidad por carretera

Con base en el número de **zonas estructurales** (hotspots con ≥2 assets) encontradas sobre o cerca de cada carretera:

| Severidad | Criterio | Interpretación |
|---|---|---|
| **Alta** | ≥ 10 zonas estructurales | Carretera con problemas crónicos y sistemáticos de cobertura GPS. Múltiples tramos afectados. |
| **Media** | 3 – 9 zonas estructurales | Problemas frecuentes pero localizados. Requiere atención. |
| **Baja** | 1 – 2 zonas estructurales | Problemas puntuales. Pueden ser esquinas geográficas, cruces de montaña, etc. |
| **Aislada** | 0 zonas estructurales, solo eventos aislados | No hay evidencia colectiva de problema estructural. |
| **Sin problemas** | 0 hotspots asociados | La carretera no aparece en ningún hotspot del análisis. |

> **Nota:** La severidad se basa en **zonas estructurales**, no en eventos totales, porque una zona estructural (confirmada por ≥2 vehículos independientes) es evidencia mucho más sólida de un problema real de infraestructura celular que un alto conteo de eventos de un solo asset.

---

## 8. Resultados del análisis (última ejecución)

| Métrica | Valor |
|---|---|
| Clientes analizados | 1,579 |
| Assets analizados | 38,964 |
| Registros procesados | 806,855,420 |
| Registros sin GPS | 17,105,313 (2.1%) |
| Grilla utilizada | 0.01° (~1 km²) |
| Total hotspots identificados | 73,761 |
| → Zonas estructurales (≥2 assets) | 46,494 |
| → Eventos aislados (1 asset) | 27,267 |
| Carreteras analizadas | 857 |
| Carreteras con problemas | 401 (46.8%) |
| Carreteras sin problemas | 456 (53.2%) |

### Top carreteras por severidad alta

| Carretera | Zonas sin cobertura | Eventos GPS perdidos |
|---|---|---|
| ACAPULCO - VERACRUZ | 68 | 1,035,469 |
| MAZATLAN - MATAMOROS | 58 | 328,128 |
| MEXICO - QUERETARO (DIRECTO) | 49 | 310,294 |
| SAN LUIS POTOSI - MATEHUALA | 49 | 274,215 |
| MEXICO - PUEBLA (DIRECTO) | 44 | 271,130 |

---

## 9. Limitaciones y consideraciones

- **Posición aproximada**: la posición asignada a un evento sin GPS es la del *último* punto válido conocido. Si el tramo sin cobertura es largo (ej. 50 km de montaña), todos los eventos de ese tramo quedan asignados al punto de entrada, no distribuidos a lo largo del tramo.
- **Muestra de asset IDs**: el índice de carreteras guarda hasta 10 asset IDs por celda por lote. Celdas con cientos de assets mostrarán conteos aproximados de clientes/assets únicos.
- **Catálogo de carreteras**: el catálogo incluye red federal. Carreteras estatales, brechas y caminos rurales no están representados — hotspots en esas zonas no se asocian a ninguna carretera del catálogo.
- **Radio de match**: el radio de 8 celdas vecinas (≈1-2 km) puede asociar un hotspot a una carretera cercana aunque no directamente sobre ella. En intersecciones o zonas urbanas con alta densidad vial, un hotspot puede asociarse a varias carreteras simultáneamente.
