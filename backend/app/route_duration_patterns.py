"""
Caso 5 — Detección de demoras anómalas en rutas vehiculares.

Nota de alcance (igual que el Caso 4): la metodología original considera factores
contextuales opcionales (día de la semana, hora de salida, conductor). El dataset no
tiene driverId (ver route_stop_patterns.py), y con los volúmenes de viajes por ruta
típicos en este dataset (decenas, no miles), segmentar además por día/hora dejaría
muy pocas muestras por bucket para ser estadísticamente significativo. Por eso este
módulo aprende el tiempo habitual por ruta (assetId + clientId + ruta) sin esa
segmentación adicional — documentado aquí en vez de forzar un desglose frágil.

Fases (ver caso de estudio):
  1. Agrupa los recorridos históricos del asset en "rutas habituales" (reutiliza el
     clustering de origen/destino de route_learning.py — misma unidad que el Caso 4).
  2. Filtra recorridos atípicos previos (IQR sobre duration_hours) antes de calcular
     el tiempo "normal", para que un viaje ya anómalo no contamine el patrón.
  3. Calcula el perfil temporal esperado: promedio, mediana, desviación estándar y
     percentiles del conjunto filtrado.
  4. Evalúa cada viaje contra ese perfil y marca demora anómala cuando excede el
     límite superior de la banda de normalidad.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from app.route_learning import cluster_points, extract_route_endpoints, is_meaningful_trip

_DEFAULT_ORIGIN_RADIUS_KM = 8.0
_DEFAULT_DEST_RADIUS_KM = 8.0
_MIN_TRIPS_FOR_PATTERN = 3
_IQR_MULTIPLIER = 1.5
_DURATION_Z = 2.0


def _iqr_bounds(values: pd.Series) -> tuple[float, float]:
    q1, q3 = values.quantile(0.25), values.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return float(values.min()), float(values.max())
    return float(q1 - _IQR_MULTIPLIER * iqr), float(q3 + _IQR_MULTIPLIER * iqr)


def _duration_stats(durations: list[float]) -> dict[str, Any]:
    arr = pd.Series(durations, dtype="float64")
    # mean_hours/std_hours se usan después para calcular %-sobre-promedio y la banda
    # de normalidad — 4 decimales evita que rutas muy cortas (minutos) redondeen a
    # 0.0 y disparen una división degenerada (mean_hours > 0 pasaría a False).
    return {
        "count": len(arr),
        "mean_hours": round(float(arr.mean()), 4),
        "median_hours": round(float(arr.median()), 2),
        "std_hours": round(float(arr.std()) if len(arr) > 1 else 0.0, 4),
        "min_hours": round(float(arr.min()), 2),
        "max_hours": round(float(arr.max()), 2),
        "p10_hours": round(float(arr.quantile(0.10)), 2),
        "p90_hours": round(float(arr.quantile(0.90)), 2),
    }


def build_route_duration_patterns(
    routes: list[dict[str, Any]],
    origin_radius_km: float = _DEFAULT_ORIGIN_RADIUS_KM,
    dest_radius_km: float = _DEFAULT_DEST_RADIUS_KM,
    min_trips_for_pattern: int = _MIN_TRIPS_FOR_PATTERN,
    duration_z: float = _DURATION_Z,
) -> dict[str, Any]:
    """Agrupa los recorridos por ruta habitual y aprende el tiempo normal de viaje
    (banda de normalidad) para cada una, tras filtrar recorridos atípicos previos."""
    valid_idx: list[int] = []
    endpoints: list[tuple[float, float, float, float]] = []
    for i, r in enumerate(routes):
        if not is_meaningful_trip(r):
            continue
        ep = extract_route_endpoints(r)
        if ep is not None and r.get("duration_hours") is not None:
            valid_idx.append(i)
            endpoints.append(ep)

    if not valid_idx:
        return {
            "total_routes_input": len(routes), "total_route_groups": 0,
            "origin_radius_km": origin_radius_km, "dest_radius_km": dest_radius_km,
            "route_patterns": [],
        }

    origin_assign, _ = cluster_points([(e[0], e[1]) for e in endpoints], origin_radius_km)
    dest_assign, _ = cluster_points([(e[2], e[3]) for e in endpoints], dest_radius_km)

    route_groups: dict[tuple[int, int], list[int]] = {}
    for local_i, route_i in enumerate(valid_idx):
        key = (origin_assign[local_i], dest_assign[local_i])
        route_groups.setdefault(key, []).append(route_i)

    patterns: list[dict[str, Any]] = []
    for (oid, did), route_indices in route_groups.items():
        if len(route_indices) < min_trips_for_pattern:
            continue

        durations = [(i, float(routes[i]["duration_hours"])) for i in route_indices]
        raw_series = pd.Series([d for _, d in durations], dtype="float64")

        # Fase de filtrado: excluir recorridos atípicos previos (IQR) antes de
        # calcular el tiempo "normal", para no contaminar el patrón con viajes ya
        # anómalos (pérdida de señal prolongada, incidentes, desvíos extraordinarios).
        lower, upper = _iqr_bounds(raw_series)
        clean = [(i, d) for i, d in durations if lower <= d <= upper]
        excluded = [i for i, d in durations if not (lower <= d <= upper)]

        # Si el filtrado deja muy pocos puntos, usar el conjunto crudo (mejor una
        # banda amplia que una calculada con 1-2 muestras).
        stats_source = clean if len(clean) >= max(3, min_trips_for_pattern - 1) else durations
        stats = _duration_stats([d for _, d in stats_source])

        upper_band = round(stats["mean_hours"] + duration_z * stats["std_hours"], 2)
        lower_band = round(max(0.0, stats["mean_hours"] - duration_z * stats["std_hours"]), 2)

        patterns.append({
            "origin_cluster_id": oid,
            "destination_cluster_id": did,
            "total_trips": len(route_indices),
            "route_indices": route_indices,
            "excluded_outlier_indices": excluded,
            "duration_stats": stats,
            "normal_band_hours": [lower_band, upper_band],
        })

    patterns.sort(key=lambda p: -p["total_trips"])
    return {
        "total_routes_input": len(routes),
        "total_route_groups": len(patterns),
        "origin_radius_km": origin_radius_km,
        "dest_radius_km": dest_radius_km,
        "route_patterns": patterns,
    }


def _severity_from_zscore(z: float) -> str:
    # is_delayed ya exige z > duration_z (2 std, ver normal_band_hours), así que
    # cualquier alerta parte de "media" — "alta" es una demora realmente extrema.
    if z >= 4.0:
        return "alta"
    return "media"


def flag_atypical_duration(route_pattern: dict[str, Any], route: dict[str, Any]) -> dict[str, Any] | None:
    """Compara la duración de un viaje contra la banda de normalidad de su ruta
    habitual. Devuelve None si el viaje no tiene duration_hours."""
    duration_hours = route.get("duration_hours")
    if duration_hours is None:
        return None
    duration_hours = float(duration_hours)

    mean_hours = route_pattern["duration_stats"]["mean_hours"]
    std_hours = route_pattern["duration_stats"]["std_hours"]
    _, upper_band = route_pattern["normal_band_hours"]

    is_delayed = duration_hours > upper_band
    # %-sobre-promedio es solo informativo: con una línea base casi cero (rutas muy
    # cortas) puede dispararse a valores enormes y sin sentido — la severidad usa
    # el z-score (desviaciones estándar), que es estable en ese caso.
    pct_over = round((duration_hours - mean_hours) / mean_hours * 100, 1) if mean_hours > 0 else None
    z_score = round((duration_hours - mean_hours) / std_hours, 2) if std_hours > 0 else None

    return {
        "duration_hours": round(duration_hours, 2),
        "expected_mean_hours": round(mean_hours, 2),
        "normal_band_hours": route_pattern["normal_band_hours"],
        "is_delayed": is_delayed,
        "pct_over_average": pct_over,
        "z_score": z_score,
        "severity": _severity_from_zscore(z_score) if (is_delayed and z_score is not None) else ("media" if is_delayed else "ninguna"),
    }
