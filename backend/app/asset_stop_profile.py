"""
Perfil histórico de paradas 'normales' por assetId + clientId.

Precursor directo del Caso 4 (paradas atípicas por vehículo+conductor+ruta):
aprende qué ubicaciones, categorías y duraciones son habituales para un
vehículo específico, y permite marcar cuándo una parada se aparta de ese
patrón — sin necesitar aún driverId ni segmentación por ruta.
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from app.stop_detection import detect_and_classify_stops

_GRID = 0.003  # ~300 m, alineado al radio de clasificación geográfica de stop_detection
_MIN_VISITS_FREQUENT = 2
_DEFAULT_LOCATION_RADIUS_M = 500.0
_DEFAULT_DURATION_Z = 2.0


def _cell(lat: float, lon: float) -> tuple[float, float]:
    return (round(round(lat / _GRID) * _GRID, 5), round(round(lon / _GRID) * _GRID, 5))


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def build_asset_stop_profile(
    df: pd.DataFrame,
    min_stop_minutes: float = 5.0,
    use_geo: bool = True,
) -> dict[str, Any]:
    """Construye el perfil histórico de paradas normales para un asset+cliente."""
    result = detect_and_classify_stops(df, min_stop_minutes=min_stop_minutes, use_geo=use_geo)
    return build_profile_from_stops(result.get("stops", []), error=result.get("error"))


def build_profile_from_stops(
    stops: list[dict[str, Any]],
    error: str | None = None,
) -> dict[str, Any]:
    """Igual que build_asset_stop_profile, pero a partir de paradas ya clasificadas
    (evita repetir la detección + clasificación geográfica cuando el caller ya la tiene)."""
    if error or not stops:
        return {
            "total_stops": 0,
            "total_stop_minutes": 0.0,
            "frequent_locations": [],
            "all_locations": [],
            "categories": [],
            "error": error,
        }

    total_stops = len(stops)

    # ── Ubicaciones frecuentes (clustering por celda ~300 m) ────────────────
    loc_agg: dict[tuple[float, float], dict[str, Any]] = {}
    for s in stops:
        lat, lon = s.get("latitude"), s.get("longitude")
        if lat is None or lon is None:
            continue
        cell = _cell(lat, lon)
        entry = loc_agg.setdefault(cell, {
            "lat": cell[0], "lon": cell[1],
            "visit_count": 0, "total_minutes": 0.0, "categories": {},
        })
        entry["visit_count"] += 1
        entry["total_minutes"] += s["duration_minutes"]
        cat = s["final_category"]
        entry["categories"][cat] = entry["categories"].get(cat, 0) + 1

    all_locations = []
    for entry in loc_agg.values():
        dominant_cat = max(entry["categories"], key=entry["categories"].get)
        all_locations.append({
            "lat": entry["lat"],
            "lon": entry["lon"],
            "visit_count": entry["visit_count"],
            "pct_of_stops": round(entry["visit_count"] / total_stops * 100, 1),
            "dominant_category": dominant_cat,
            "avg_duration_minutes": round(entry["total_minutes"] / entry["visit_count"], 1),
            "is_frequent": entry["visit_count"] >= _MIN_VISITS_FREQUENT,
        })
    all_locations.sort(key=lambda l: -l["visit_count"])
    frequent_locations = [l for l in all_locations if l["is_frequent"]]

    # ── Distribución de categorías + estadísticas de duración ───────────────
    cat_durations: dict[str, list[float]] = {}
    for s in stops:
        cat_durations.setdefault(s["final_category"], []).append(s["duration_minutes"])

    categories = []
    for cat, durs in cat_durations.items():
        arr = pd.Series(durs, dtype="float64")
        categories.append({
            "category": cat,
            "count": len(durs),
            "pct_of_stops": round(len(durs) / total_stops * 100, 1),
            "avg_duration_minutes": round(float(arr.mean()), 1),
            "median_duration_minutes": round(float(arr.median()), 1),
            "std_duration_minutes": round(float(arr.std()) if len(arr) > 1 else 0.0, 1),
            "min_duration_minutes": round(float(arr.min()), 1),
            "max_duration_minutes": round(float(arr.max()), 1),
        })
    categories.sort(key=lambda c: -c["count"])

    return {
        "total_stops": total_stops,
        "total_stop_minutes": round(sum(s["duration_minutes"] for s in stops), 2),
        "frequent_locations": frequent_locations,
        "all_locations": all_locations,
        "categories": categories,
        "error": None,
    }


def flag_atypical_stops(
    profile: dict[str, Any],
    stops: list[dict[str, Any]],
    location_radius_m: float = _DEFAULT_LOCATION_RADIUS_M,
    duration_z_threshold: float = _DEFAULT_DURATION_Z,
) -> list[dict[str, Any]]:
    """
    Marca cada parada de `stops` como típica o atípica respecto al `profile` histórico.

    Criterios (ver caso de estudio de paradas atípicas):
      - ubicacion_no_habitual: no cae dentro del radio de ninguna ubicación frecuente
      - tipo_no_habitual: la categoría final no aparece en el historial del asset
      - duracion_anomala: la duración se aleja > duration_z_threshold desviaciones
        estándar del promedio histórico para esa categoría
    """
    freq_locs = profile.get("frequent_locations", [])
    known_cats = {c["category"]: c for c in profile.get("categories", [])}

    flagged: list[dict[str, Any]] = []
    for s in stops:
        reasons: list[str] = []
        lat, lon = s.get("latitude"), s.get("longitude")
        cat = s.get("final_category")

        if freq_locs:
            near_known = lat is not None and lon is not None and any(
                _haversine_m(lat, lon, loc["lat"], loc["lon"]) <= location_radius_m
                for loc in freq_locs
            )
            if not near_known:
                reasons.append("ubicacion_no_habitual")

        if known_cats and cat not in known_cats:
            reasons.append("tipo_no_habitual")

        if cat in known_cats:
            stat = known_cats[cat]
            mean = stat["avg_duration_minutes"]
            std = stat["std_duration_minutes"]
            dur = s.get("duration_minutes", 0.0)
            if std > 0 and abs(dur - mean) > duration_z_threshold * std:
                reasons.append("duracion_anomala")
            elif std == 0 and stat["count"] >= 3 and abs(dur - mean) > max(mean * 0.5, 10.0):
                reasons.append("duracion_anomala")

        flagged.append({**s, "is_atypical": bool(reasons), "atypical_reasons": reasons})

    return flagged
