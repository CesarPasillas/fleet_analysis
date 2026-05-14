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
_OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"
_OVERPASS_URL_FALLBACK = "https://overpass-api.de/api/interpreter"
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
    # Combustible
    ("amenity", "fuel",             "gasolinera"),
    ("shop",    "fuel",             "gasolinera"),
    # Alimentos
    ("amenity", "restaurant",       "restaurante"),
    ("amenity", "fast_food",        "restaurante"),
    ("amenity", "cafe",             "restaurante"),
    ("amenity", "food_court",       "restaurante"),
    # Hospedaje
    ("tourism", "hotel",            "hotel"),
    ("tourism", "motel",            "hotel"),
    ("tourism", "hostel",           "hotel"),
    # Comercio
    ("shop",    "convenience",      "tienda_conveniencia"),
    ("shop",    "supermarket",      "tienda_conveniencia"),
    ("shop",    "wholesale",        "tienda_conveniencia"),
    # Salud
    ("amenity", "pharmacy",         "farmacia"),
    ("amenity", "hospital",         "hospital"),
    ("amenity", "clinic",           "hospital"),
    # Taller y servicios vehiculares
    ("shop",    "car_repair",       "taller_mecanico"),
    ("shop",    "tyres",            "taller_mecanico"),
    ("amenity", "car_wash",         "taller_mecanico"),
    # Logística / carga
    ("landuse", "industrial",       "terminal_carga"),
    ("building","warehouse",        "terminal_carga"),
    ("amenity", "weighbridge",      "terminal_carga"),
]

# Duración mínima (segundos) para heurísticas temporales cuando OSM no halla POI
_HEURISTIC_PERNOCTA_S: float = 4 * 3600    # ≥4 h
_HEURISTIC_PARADA_LARGA_S: float = 2 * 3600  # ≥2 h

_geo_cache: dict[tuple[float, float, int], tuple[str | None, str | None]] = {}
# Maps (lat, lon) → (category_or_None, place_name_or_None)
_nominatim_cache: dict[tuple[float, float], tuple[str | None, str | None]] = {}

_HIGHWAY_TYPES = {
    "motorway", "trunk", "primary", "secondary", "tertiary",
    "motorway_link", "trunk_link", "primary_link",
}


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


def _build_overpass_query(lat: float, lon: float) -> str:
    area = f"(around:{_GEO_RADIUS_M},{lat},{lon})"
    # Use exact-match filters (faster than regex ~) with nwr (node/way/relation)
    tag_filters = [
        '["amenity"="fuel"]', '["shop"="fuel"]',
        '["amenity"="restaurant"]', '["amenity"="fast_food"]',
        '["amenity"="cafe"]', '["amenity"="food_court"]',
        '["tourism"="hotel"]', '["tourism"="motel"]', '["tourism"="hostel"]',
        '["shop"="convenience"]', '["shop"="supermarket"]', '["shop"="wholesale"]',
        '["amenity"="pharmacy"]',
        '["amenity"="hospital"]', '["amenity"="clinic"]',
        '["shop"="car_repair"]', '["shop"="tyres"]', '["amenity"="car_wash"]',
        '["building"="warehouse"]', '["landuse"="industrial"]', '["amenity"="weighbridge"]',
    ]
    parts = "\n".join(f"  nwr{f}{area};" for f in tag_filters)
    return f"[out:json][timeout:{_OVERPASS_TIMEOUT}];\n(\n{parts}\n);\nout center tags;"


_OVERPASS_HEADERS = {"User-Agent": "fleet-analysis-phd/1.0 (research)"}


def _classify_geo(lat: float, lon: float) -> tuple[str | None, str | None]:
    cache_key = (round(lat, 4), round(lon, 4), _GEO_RADIUS_M)
    if cache_key in _geo_cache:
        return _geo_cache[cache_key]

    query = _build_overpass_query(lat, lon)

    elements = None
    for url in (_OVERPASS_URL, _OVERPASS_URL_FALLBACK):
        try:
            resp = requests.post(
                url, data={"data": query}, headers=_OVERPASS_HEADERS,
                timeout=_OVERPASS_TIMEOUT + 4,
            )
            resp.raise_for_status()
            elements = resp.json().get("elements", [])
            break
        except Exception:
            continue
    if elements is None:
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


def _classify_via_nominatim(lat: float, lon: float) -> tuple[str | None, str | None]:
    """Return (category, place_name) using Nominatim reverse-geocoding.

    category = 'en_carretera' when the nearest OSM feature is a major road;
    None otherwise (street address returned as place_name).
    """
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
        _nominatim_cache[cache_key] = (None, None)
        return None, None

    addr = data.get("address", {})
    osm_class = data.get("class", "")
    osm_type = data.get("type", "")

    road = addr.get("road") or addr.get("highway")
    city = addr.get("suburb") or addr.get("village") or addr.get("town") or addr.get("city")
    state = addr.get("state")

    # Detect when the nearest OSM object IS a highway/major road
    is_highway = osm_class == "highway" and osm_type in _HIGHWAY_TYPES
    if is_highway and road:
        result: tuple[str | None, str | None] = ("en_carretera", road)
    else:
        parts = [road, city, state]
        label = ", ".join(p for p in parts if p) or data.get("display_name", "").split(",")[0] or None
        result = (None, label)

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
        # _classify_via_nominatim returns ("en_carretera", road_name) on highways,
        # or (None, address_string) for regular locations
        nom_cat, nom_place = _classify_via_nominatim(stop["latitude"], stop["longitude"])
        time.sleep(0.25)
        if nom_cat == "en_carretera":
            geo_cat = "en_carretera"
            geo_place = nom_place
        else:
            nearby_address = nom_place

    return geo_cat, geo_place, nearby_address, geo_calls


def _duration_heuristic(duration_s: float, start_ts: str | None) -> str | None:
    """Infer category from stop duration and start hour when OSM finds no POI."""
    hour = None
    if start_ts:
        try:
            hour = pd.Timestamp(start_ts).hour
        except Exception:
            pass

    if duration_s >= _HEURISTIC_PERNOCTA_S:
        # Long stop at night → overnight rest outside base
        if hour is not None and (hour >= 20 or hour < 7):
            return "pernocta_fuera_de_base"
        return "parada_larga"

    if duration_s >= _HEURISTIC_PARADA_LARGA_S:
        return "parada_larga"

    return None


def _resolve_category(
    geo_cat: str | None,
    event_cat: str | None,
    duration_s: float = 0.0,
    start_ts: str | None = None,
) -> tuple[str, str]:
    if geo_cat:
        return geo_cat, "alta"
    if event_cat:
        return event_cat, "media"
    heuristic = _duration_heuristic(duration_s, start_ts)
    if heuristic:
        return heuristic, "media"
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
        final_cat, confidence = _resolve_category(
            geo_cat, event_cat,
            duration_s=stop["duration_seconds"],
            start_ts=stop["start_timestamp"],
        )

        stop["event_type_category"] = event_cat
        stop["geo_category"] = geo_cat
        stop["nearby_place"] = geo_place or nearby_address
        stop["final_category"] = final_cat
        stop["confidence"] = confidence

    # Aggregate category summary
    cat_summary: dict[str, dict[str, Any]] = {}
    for s in stops:
        cat = s["final_category"]
        if cat not in cat_summary:
            cat_summary[cat] = {"category": cat, "count": 0, "total_minutes": 0.0}
        cat_summary[cat]["count"] += 1
        cat_summary[cat]["total_minutes"] += s["duration_minutes"]
    for v in cat_summary.values():
        v["total_minutes"] = round(v["total_minutes"], 2)
    category_summary = sorted(cat_summary.values(), key=lambda x: -x["count"])

    return {
        "total_records": len(df),
        "total_stops": len(stops),
        "total_stop_minutes": round(sum(s["duration_minutes"] for s in stops), 2),
        "category_summary": category_summary,
        "stops": stops,
        "error": None,
    }
