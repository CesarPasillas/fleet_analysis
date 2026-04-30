from __future__ import annotations

import math
import time
from typing import Any

import numpy as np
import pandas as pd
import requests

_MIN_STOP_SECONDS: float = 300.0
_MAX_STEP_M: float = 50.0
_MIN_RECORDS: int = 3
_GEO_RADIUS_M: int = 300
_MAX_GEO_CALLS: int = 100
_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
_OVERPASS_TIMEOUT = 8
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
_NOMINATIM_TIMEOUT = 6

_EVENT_CATEGORIES: dict[str, str] = {
    "IGNITION_OFF": "motor_apagado",
    "PARKING_OVERTIME": "estacionamiento_prolongado",
    "NIGHT_PARKING_VIOLATION": "pernocta_fuera_de_base",
    "UNAUTHORIZED_STOP": "parada_no_autorizada",
    "EXCESSIVE_IDLE": "ralenti",
    "IDLE_STOP": "ralenti",
    "REPORT_IDLE": "ralenti",
    "IDLE_FUEL_CONSUMPTION": "ralenti",
    "GEOFENCE_STOP_OVERTIME": "parada_en_geocerca",
    "CHECKPOINT_REACHED": "punto_de_control",
    "DELIVERY_COMPLETED": "entrega",
}

_OSM_TAGS: list[tuple[str, str, str]] = [
    ("amenity", "fuel",         "gasolinera"),
    ("shop",    "fuel",         "gasolinera"),
    ("amenity", "restaurant",   "restaurante"),
    ("amenity", "fast_food",    "restaurante"),
    ("amenity", "cafe",         "restaurante"),
    ("amenity", "food_court",   "restaurante"),
    ("tourism", "hotel",        "hotel"),
    ("tourism", "motel",        "hotel"),
    ("tourism", "hostel",       "hotel"),
    ("shop",    "convenience",  "tienda_conveniencia"),
    ("shop",    "supermarket",  "tienda_conveniencia"),
]

_geo_cache: dict[tuple[float, float, int], tuple[str | None, str | None]] = {}
_nominatim_cache: dict[tuple[float, float], str | None] = {}


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _to_ms(series: pd.Series) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    med = float(vals.abs().median())
    if med >= 1e17:
        return vals / 1_000_000
    if med >= 1e14:
        return vals / 1_000
    if med >= 1e11:
        return vals
    return vals * 1_000


def _iso(ms: float) -> str | None:
    try:
        return pd.Timestamp(ms, unit="ms", tz="UTC").isoformat()
    except Exception:
        return None


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _extract_rest_segments(at_rest: np.ndarray, min_recs: int) -> list[tuple[int, int]]:
    segments: list[tuple[int, int]] = []
    in_stop = False
    s_start = 0
    n = len(at_rest)
    for i in range(n):
        if at_rest[i] and not in_stop:
            in_stop = True
            s_start = i
        elif not at_rest[i] and in_stop:
            if i - s_start >= min_recs:
                segments.append((s_start, i - 1))
            in_stop = False
    if in_stop and n - s_start >= min_recs:
        segments.append((s_start, n - 1))
    return segments


def _build_stop_record(
    seg: pd.DataFrame,
    lats: np.ndarray,
    lons: np.ndarray,
    seg_s: int,
    seg_e: int,
    key_col: str | None,
    stop_index: int,
) -> dict[str, Any]:
    t0 = float(seg["_ms"].iloc[0])
    t1 = float(seg["_ms"].iloc[-1])
    dur_s = (t1 - t0) / 1000.0
    keys: list[str] = []
    if key_col:
        keys = seg[key_col].dropna().unique().tolist()
    return {
        "stop_index": stop_index,
        "start_timestamp": _iso(t0),
        "end_timestamp": _iso(t1),
        "duration_minutes": round(dur_s / 60.0, 2),
        "duration_seconds": dur_s,
        "latitude": float(np.median(lats[seg_s: seg_e + 1])),
        "longitude": float(np.median(lons[seg_s: seg_e + 1])),
        "num_records": seg_e - seg_s + 1,
        "event_keys": keys,
    }


def _raw_stops(
    df: pd.DataFrame,
    min_stop_s: float,
    max_step_m: float,
    min_recs: int,
) -> list[dict[str, Any]]:
    gps_col = _find_col(df, ["gpsTimestamp", "gps_timestamp", "GPSTimeStamp"])
    lat_col = _find_col(df, ["latitude", "lat"])
    lon_col = _find_col(df, ["longitude", "lon"])
    key_col = _find_col(df, ["Key", "event_key"])

    if not gps_col or not lat_col or not lon_col:
        return []

    cols = [c for c in [gps_col, lat_col, lon_col, key_col] if c]
    w = df[cols].copy()
    w["_ms"] = _to_ms(w[gps_col])
    w = w.dropna(subset=["_ms", lat_col, lon_col])
    w = w[(w["_ms"] > 0) & (w[lat_col] != 0) & (w[lon_col] != 0)]
    w = w.sort_values("_ms").reset_index(drop=True)

    if len(w) < min_recs:
        return []

    lats = w[lat_col].values.astype(float)
    lons = w[lon_col].values.astype(float)

    steps = np.zeros(len(w))
    for i in range(1, len(w)):
        steps[i] = _haversine_m(lats[i - 1], lons[i - 1], lats[i], lons[i])

    at_rest = steps < max_step_m
    at_rest[0] = True

    segments = _extract_rest_segments(at_rest, min_recs)

    results: list[dict[str, Any]] = []
    for seg_s, seg_e in segments:
        seg = w.iloc[seg_s: seg_e + 1]
        record = _build_stop_record(seg, lats, lons, seg_s, seg_e, key_col, len(results))
        if record["duration_seconds"] >= min_stop_s:
            results.append(record)

    return results


def _classify_event(keys: list[str]) -> str | None:
    for k in keys:
        if k in _EVENT_CATEGORIES:
            return _EVENT_CATEGORIES[k]
    return None


def _classify_geo(lat: float, lon: float) -> tuple[str | None, str | None]:
    cache_key = (round(lat, 4), round(lon, 4), _GEO_RADIUS_M)
    if cache_key in _geo_cache:
        return _geo_cache[cache_key]

    query = (
        f"[out:json][timeout:{_OVERPASS_TIMEOUT}];\n"
        f"(\n"
        f'  node["amenity"~"fuel|restaurant|fast_food|cafe|food_court"]'
        f"(around:{_GEO_RADIUS_M},{lat},{lon});\n"
        f'  node["tourism"~"hotel|motel|hostel"]'
        f"(around:{_GEO_RADIUS_M},{lat},{lon});\n"
        f'  node["shop"~"convenience|supermarket|fuel"]'
        f"(around:{_GEO_RADIUS_M},{lat},{lon});\n"
        f'  way["amenity"~"fuel|restaurant|fast_food|cafe"]'
        f"(around:{_GEO_RADIUS_M},{lat},{lon});\n"
        f'  way["tourism"~"hotel|motel|hostel"]'
        f"(around:{_GEO_RADIUS_M},{lat},{lon});\n"
        f'  way["shop"~"convenience|supermarket|fuel"]'
        f"(around:{_GEO_RADIUS_M},{lat},{lon});\n"
        f");\n"
        f"out center tags;"
    )

    try:
        resp = requests.post(
            _OVERPASS_URL,
            data={"data": query},
            timeout=_OVERPASS_TIMEOUT + 2,
        )
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
    except Exception:
        _geo_cache[cache_key] = (None, None)
        return None, None

    for el in elements:
        tags = el.get("tags", {})
        for tag_key, tag_val, category in _OSM_TAGS:
            if tags.get(tag_key) == tag_val:
                name = tags.get("name") or tags.get("brand") or tag_val
                _geo_cache[cache_key] = (category, name)
                return category, name

    _geo_cache[cache_key] = (None, None)
    return None, None


def _reverse_geocode(lat: float, lon: float) -> str | None:
    cache_key = (round(lat, 3), round(lon, 3))
    if cache_key in _nominatim_cache:
        return _nominatim_cache[cache_key]

    try:
        resp = requests.get(
            _NOMINATIM_URL,
            params={"lat": lat, "lon": lon, "format": "json", "zoom": 16, "addressdetails": 1},
            headers={"User-Agent": "fleet-analysis-phd/1.0"},
            timeout=_NOMINATIM_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        _nominatim_cache[cache_key] = None
        return None

    addr = data.get("address", {})
    parts = [
        addr.get("road") or addr.get("highway"),
        addr.get("suburb") or addr.get("village") or addr.get("town") or addr.get("city"),
        addr.get("state"),
    ]
    label = ", ".join(p for p in parts if p)
    result = label or data.get("display_name", "").split(",")[0] or None
    _nominatim_cache[cache_key] = result
    return result


def _query_geo_for_stop(
    stop: dict[str, Any],
    use_geo: bool,
    geo_calls: int,
    max_geo_calls: int,
) -> tuple[str | None, str | None, str | None, int]:
    if not use_geo or not stop["latitude"] or not stop["longitude"]:
        return None, None, None, geo_calls

    geo_cat, geo_place = None, None
    if geo_calls < max_geo_calls:
        geo_cat, geo_place = _classify_geo(stop["latitude"], stop["longitude"])
        geo_calls += 1
        time.sleep(0.25)

    nearby_address = None
    if geo_cat is None:
        nearby_address = _reverse_geocode(stop["latitude"], stop["longitude"])
        time.sleep(0.25)

    return geo_cat, geo_place, nearby_address, geo_calls


def _resolve_category(geo_cat: str | None, event_cat: str | None) -> tuple[str, str]:
    if geo_cat:
        return geo_cat, "alta"
    if event_cat:
        return event_cat, "media"
    return "parada_en_carretera", "baja"


def detect_and_classify_stops(
    df: pd.DataFrame,
    min_stop_minutes: float = 5.0,
    max_step_m: float = _MAX_STEP_M,
    min_records: int = _MIN_RECORDS,
    use_geo: bool = True,
    max_geo_calls: int = _MAX_GEO_CALLS,
) -> dict[str, Any]:
    if df is None or df.empty:
        return {"total_records": 0, "total_stops": 0, "total_stop_minutes": 0.0, "stops": [], "error": None}

    stops = _raw_stops(df, min_stop_minutes * 60.0, max_step_m, min_records)

    geo_calls = 0
    for stop in stops:
        event_cat = _classify_event(stop["event_keys"])
        geo_cat, geo_place, nearby_address, geo_calls = _query_geo_for_stop(
            stop, use_geo, geo_calls, max_geo_calls
        )
        final_cat, confidence = _resolve_category(geo_cat, event_cat)

        stop["event_type_category"] = event_cat
        stop["geo_category"] = geo_cat
        stop["nearby_place"] = geo_place or nearby_address
        stop["final_category"] = final_cat
        stop["confidence"] = confidence

    return {
        "total_records": len(df),
        "total_stops": len(stops),
        "total_stop_minutes": round(sum(s["duration_minutes"] for s in stops), 2),
        "stops": stops,
        "error": None,
    }
