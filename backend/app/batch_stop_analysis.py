"""
Batch stop analysis across ALL clients and assets.

Phases (each saved independently so the job can resume or skip):
  1. Detection  — DuckDB window functions, batches of 20 clients
                  Result: batch_stop_phase1.json  (raw grid-cell counts)
  2. OSM        — Overpass batch queries for top 2 000 locations
                  Result: batch_stop_analysis.json (final classified result)

Smart resume:
  - If phase1 file exists and record count matches, skip to phase 2
  - If only OSM reclassification is requested, load phase1 and rerun phase 2

Record count stored in metadata → UI can warn when new data is available.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb
import requests

_log = logging.getLogger(__name__)

_DATA_DIR     = Path(__file__).resolve().parents[1] / "data"
_DB_PATH      = _DATA_DIR / "fleet_analytics.duckdb"
_PHASE1_PATH  = _DATA_DIR / "batch_stop_phase1.json"
_RESULT_PATH  = _DATA_DIR / "batch_stop_analysis.json"
_STATUS_PATH  = _DATA_DIR / "batch_stop_analysis_status.json"
_GEO_CACHE    = _DATA_DIR / "geo_cache"

_GRID         = 0.01
_MIN_DUR_MIN  = 5.0
_MIN_RECORDS  = 3
_CLIENT_BATCH = 20
_MAX_OSM_LOCS = 2000
_OSM_BATCH    = 15

_OVERPASS_URL          = "https://overpass.kumi.systems/api/interpreter"
_OVERPASS_URL_FALLBACK = "https://overpass-api.de/api/interpreter"
_OVERPASS_HEADERS      = {"User-Agent": "fleet-analysis-phd/1.0 (research)"}

_OSM_TAGS: list[tuple[str, str, str]] = [
    ("amenity", "fuel",        "gasolinera"),
    ("shop",    "fuel",        "gasolinera"),
    ("amenity", "restaurant",  "restaurante"),
    ("amenity", "fast_food",   "restaurante"),
    ("amenity", "cafe",        "restaurante"),
    ("tourism", "hotel",       "hotel"),
    ("tourism", "motel",       "hotel"),
    ("tourism", "hostel",      "hotel"),
    ("shop",    "convenience", "tienda_conveniencia"),
    ("shop",    "supermarket", "tienda_conveniencia"),
    ("amenity", "pharmacy",    "farmacia"),
    ("amenity", "hospital",    "hospital"),
    ("amenity", "clinic",      "hospital"),
    ("shop",    "car_repair",  "taller_mecanico"),
    ("amenity", "car_wash",    "taller_mecanico"),
    ("landuse", "industrial",  "terminal_carga"),
    ("building","warehouse",   "terminal_carga"),
    ("amenity", "weighbridge", "terminal_carga"),
]

_CAT_ICONS = {
    "gasolinera":                "⛽",
    "hotel":                     "🏨",
    "restaurante":               "🍽",
    "tienda_conveniencia":       "🏪",
    "farmacia":                  "💊",
    "hospital":                  "🏥",
    "taller_mecanico":           "🔧",
    "terminal_carga":            "🏭",
    "ralenti":                   "⏳",
    "motor_apagado":             "🔌",
    "entrega":                   "📦",
    "punto_de_control":          "✅",
    "parada_en_geocerca":        "📍",
    "estacionamiento_prolongado":"🅿",
    "pernocta_fuera_de_base":    "🌙",
    "parada_no_autorizada":      "⛔",
    "parada_en_carretera":       "🛣",
    "parada_larga":              "⏸",
}

_job_lock = threading.Lock()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _write_status(phase: str, progress: float, message: str,
                  done: bool = False, phase1_done: bool = False,
                  phase2_done: bool = False, record_count: int = 0) -> None:
    try:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _STATUS_PATH.write_text(json.dumps({
            "phase":        phase,
            "progress":     round(progress, 3),
            "message":      message,
            "done":         done,
            "phase1_done":  phase1_done,
            "phase2_done":  phase2_done,
            "record_count": record_count,
            "updated_at":   time.time(),
        }))
    except Exception:
        pass


def get_status() -> dict:
    if not _STATUS_PATH.exists():
        return {"phase": "idle", "progress": 0, "message": "", "done": False,
                "phase1_done": False, "phase2_done": False, "record_count": 0}
    try:
        return json.loads(_STATUS_PATH.read_text())
    except Exception:
        return {"phase": "idle", "progress": 0, "message": "", "done": False,
                "phase1_done": False, "phase2_done": False, "record_count": 0}


def get_cached_result() -> dict | None:
    if not _RESULT_PATH.exists():
        return None
    try:
        return json.loads(_RESULT_PATH.read_text())
    except Exception:
        return None


def get_current_record_count() -> int:
    """Fast count of total records in DuckDB (used to detect new data)."""
    if not _DB_PATH.exists():
        return 0
    try:
        con = duckdb.connect(str(_DB_PATH), read_only=True)
        n = con.execute("SELECT COUNT(*) FROM merged_pos_eventtype").fetchone()[0]
        con.close()
        return int(n)
    except Exception:
        return 0


# ── Disk geo cache ────────────────────────────────────────────────────────────

def _geo_cache_path(lat: float, lon: float) -> Path:
    return _GEO_CACHE / f"{round(lat, 4)}_{round(lon, 4)}.json"


def _load_geo_cache(lat: float, lon: float) -> tuple[str | None, str | None] | None:
    p = _geo_cache_path(lat, lon)
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text())
        return d.get("category"), d.get("place")
    except Exception:
        return None


def _save_geo_cache(lat: float, lon: float, cat: str | None, place: str | None) -> None:
    try:
        _GEO_CACHE.mkdir(parents=True, exist_ok=True)
        _geo_cache_path(lat, lon).write_text(json.dumps({"category": cat, "place": place}))
    except Exception:
        pass


# ── OSM ───────────────────────────────────────────────────────────────────────

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlam = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _osm_classify_batch(locations: list[tuple[float, float]]) -> dict[tuple[float, float], tuple[str | None, str | None]]:
    tag_filters = [
        '["amenity"="fuel"]','["shop"="fuel"]','["amenity"="restaurant"]',
        '["amenity"="fast_food"]','["amenity"="cafe"]','["tourism"="hotel"]',
        '["tourism"="motel"]','["tourism"="hostel"]','["shop"="convenience"]',
        '["shop"="supermarket"]','["amenity"="pharmacy"]','["amenity"="hospital"]',
        '["amenity"="clinic"]','["shop"="car_repair"]','["amenity"="car_wash"]',
        '["landuse"="industrial"]','["building"="warehouse"]','["amenity"="weighbridge"]',
    ]
    parts = []
    for lat, lon in locations:
        area = f"(around:300,{lat},{lon})"
        for f in tag_filters:
            parts.append(f"  nwr{f}{area};")
    timeout = min(30, 8 + 2 * len(locations))
    query   = f"[out:json][timeout:{timeout}];\n(\n" + "\n".join(parts) + "\n);\nout center tags;"

    elements = None
    for url in (_OVERPASS_URL, _OVERPASS_URL_FALLBACK):
        try:
            resp = requests.post(url, data={"data": query}, headers=_OVERPASS_HEADERS, timeout=timeout)
            resp.raise_for_status()
            elements = resp.json().get("elements", [])
            break
        except Exception:
            continue

    results: dict[tuple[float, float], tuple[str | None, str | None]] = {loc: (None, None) for loc in locations}
    if elements is None:
        return {}

    for el in elements:
        tags = el.get("tags", {})
        if el.get("type") == "node":
            el_lat, el_lon = el.get("lat"), el.get("lon")
        else:
            center = el.get("center", {})
            el_lat, el_lon = center.get("lat"), center.get("lon")
        if el_lat is None or el_lon is None:
            continue
        cat, place = None, None
        for tag_key, tag_val, category in _OSM_TAGS:
            if tags.get(tag_key) == tag_val:
                place = tags.get("name") or tags.get("brand") or tag_val
                cat   = category
                break
        if cat is None:
            continue
        for loc in locations:
            if results[loc][0] is not None:
                continue
            if _haversine_m(loc[0], loc[1], el_lat, el_lon) <= 300:
                results[loc] = (cat, place)
    return results


# ── Phase 1: DuckDB stop detection ───────────────────────────────────────────

def _detect_stops_batch(client_ids: list[int]) -> list[dict]:
    placeholders = ", ".join("?" * len(client_ids))
    sql = f"""
    WITH ordered AS (
        SELECT clientId, assetId, serverTimestamp, latitude, longitude,
               LAG(latitude)        OVER w AS prev_lat,
               LAG(longitude)       OVER w AS prev_lon
        FROM merged_pos_eventtype
        WHERE clientId IN ({placeholders})
          AND latitude  IS NOT NULL AND ABS(latitude)  > 0.01
          AND longitude IS NOT NULL AND ABS(longitude) > 0.01
          AND serverTimestamp IS NOT NULL
        WINDOW w AS (PARTITION BY assetId ORDER BY serverTimestamp)
    ),
    with_move AS (
        SELECT *,
               CASE WHEN ABS(latitude  - prev_lat) < 0.001
                     AND ABS(longitude - prev_lon) < 0.001
                    THEN 0 ELSE 1 END AS is_moving
        FROM ordered WHERE prev_lat IS NOT NULL
    ),
    grouped AS (
        SELECT *,
               SUM(is_moving) OVER (PARTITION BY assetId ORDER BY serverTimestamp) AS stop_grp
        FROM with_move
    ),
    stop_segs AS (
        SELECT clientId, assetId, stop_grp,
               AVG(latitude)  AS lat, AVG(longitude) AS lon,
               (MAX(serverTimestamp) - MIN(serverTimestamp)) / 60000.0 AS dur_min,
               COUNT(*) AS n_recs
        FROM grouped WHERE is_moving = 0
        GROUP BY clientId, assetId, stop_grp
        HAVING dur_min >= {_MIN_DUR_MIN} AND n_recs >= {_MIN_RECORDS}
    )
    SELECT
        ROUND(ROUND(lat / {_GRID}) * {_GRID}, 5) AS grid_lat,
        ROUND(ROUND(lon / {_GRID}) * {_GRID}, 5) AS grid_lon,
        COUNT(*)                 AS stop_count,
        COUNT(DISTINCT assetId)  AS unique_assets,
        COUNT(DISTINCT clientId) AS unique_clients,
        SUM(dur_min)             AS total_dur_min
    FROM stop_segs
    GROUP BY grid_lat, grid_lon
    """
    try:
        con  = duckdb.connect(str(_DB_PATH), read_only=True)
        rows = con.execute(sql, client_ids).fetchall()
        con.close()
        return [{"grid_lat": r[0], "grid_lon": r[1], "stop_count": r[2],
                 "unique_assets": r[3], "unique_clients": r[4], "total_dur_min": r[5]}
                for r in rows]
    except Exception as exc:
        _log.warning("Batch error clients %s: %s", client_ids[:3], exc)
        return []


def _run_phase1(client_ids: list[int], record_count: int) -> dict[tuple[float, float], dict]:
    total = len(client_ids)
    accumulator: dict[tuple[float, float], dict] = {}
    processed = 0

    for i in range(0, total, _CLIENT_BATCH):
        batch = client_ids[i: i + _CLIENT_BATCH]
        rows  = _detect_stops_batch(batch)
        for r in rows:
            key  = (r["grid_lat"], r["grid_lon"])
            cell = accumulator.setdefault(key, {
                "grid_lat": r["grid_lat"], "grid_lon": r["grid_lon"],
                "stop_count": 0, "unique_assets": 0, "unique_clients": 0, "total_dur_min": 0.0,
            })
            cell["stop_count"]     += r["stop_count"]
            cell["unique_assets"]  += r["unique_assets"]
            cell["unique_clients"] += r["unique_clients"]
            cell["total_dur_min"]  += r["total_dur_min"]
        processed += len(batch)
        prog = 0.70 * processed / total
        _write_status("running", prog,
                      f"Fase 1/2 — detección: {processed}/{total} clientes · {len(accumulator)} zonas",
                      phase1_done=False, record_count=record_count)

    # Persist phase 1 so it can be reused
    phase1_payload = {
        "cells":        list(accumulator.values()),
        "record_count": record_count,
        "client_count": total,
        "computed_at":  time.time(),
    }
    try:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _PHASE1_PATH.write_text(json.dumps(phase1_payload, ensure_ascii=False))
    except Exception as exc:
        _log.error("Could not save phase1: %s", exc)

    _log.info("Phase 1 done: %d stop cells", len(accumulator))
    return accumulator


# ── Phase 2: OSM classification ───────────────────────────────────────────────

def _run_phase2(all_cells: list[tuple[tuple[float, float], dict]], record_count: int, client_count: int) -> None:
    osm_candidates = [(lat, lon) for (lat, lon), _ in all_cells[:_MAX_OSM_LOCS]]
    osm_results: dict[tuple[float, float], tuple[str | None, str | None]] = {}

    # Check disk cache first
    to_fetch: list[tuple[float, float]] = []
    for loc in osm_candidates:
        cached = _load_geo_cache(loc[0], loc[1])
        if cached is not None:
            osm_results[loc] = cached
        else:
            to_fetch.append(loc)

    _log.info("Phase 2 OSM: %d from cache, %d to fetch", len(osm_results), len(to_fetch))

    for j in range(0, len(to_fetch), _OSM_BATCH):
        batch_locs = to_fetch[j: j + _OSM_BATCH]
        batch_res  = _osm_classify_batch(batch_locs)
        for loc, (cat, place) in batch_res.items():
            osm_results[loc] = (cat, place)
            _save_geo_cache(loc[0], loc[1], cat, place)
        done_osm = len(osm_results)
        prog = 0.70 + 0.30 * min(1.0, (j + _OSM_BATCH) / max(len(to_fetch), 1))
        _write_status("running", prog,
                      f"Fase 2/2 — OSM: {done_osm}/{len(osm_candidates)} ubicaciones clasificadas",
                      phase1_done=True, record_count=record_count)
        if j + _OSM_BATCH < len(to_fetch):
            time.sleep(1)

    # Build final result
    hotspots: list[dict] = []
    cat_agg:  dict[str, dict] = {}

    for (lat, lon), cell in all_cells:
        osm_cat, osm_place = osm_results.get((lat, lon), (None, None))
        final_cat = osm_cat or "parada_en_carretera"

        hotspots.append({
            "lat":           lat,  "lon":           lon,
            "stop_count":    cell["stop_count"],
            "unique_assets": cell["unique_assets"],
            "unique_clients":cell["unique_clients"],
            "avg_dur_min":   round(cell["total_dur_min"] / max(cell["stop_count"], 1), 1),
            "category":      final_cat,
            "place":         osm_place,
            "icon":          _CAT_ICONS.get(final_cat, "📌"),
        })

        entry = cat_agg.setdefault(final_cat, {
            "category": final_cat, "icon": _CAT_ICONS.get(final_cat, "📌"),
            "stop_count": 0, "unique_assets": 0,
        })
        entry["stop_count"]    += cell["stop_count"]
        entry["unique_assets"] += cell["unique_assets"]

    total_stops = sum(h["stop_count"] for h in hotspots)
    categories  = sorted(cat_agg.values(), key=lambda c: -c["stop_count"])
    for c in categories:
        c["pct"] = round(c["stop_count"] / total_stops * 100, 2) if total_stops else 0

    result = {
        "total_clients":    client_count,
        "total_stop_cells": len(all_cells),
        "total_stops":      total_stops,
        "record_count":     record_count,
        "computed_at":      time.time(),
        "categories":       categories,
        "top_hotspots":     hotspots[:2000],
    }

    try:
        _RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        _log.error("Could not save result: %s", exc)

    _write_status("done", 1.0,
                  f"Completado: {len(all_cells):,} zonas · {len(categories)} categorías · {total_stops:,} paradas",
                  done=True, phase1_done=True, phase2_done=True, record_count=record_count)
    _log.info("Batch stop analysis done: %d cells, %d cats", len(all_cells), len(categories))


# ── Main job ──────────────────────────────────────────────────────────────────

def _run_job(force: bool = False) -> None:
    if not _DB_PATH.exists():
        _write_status("error", 0, "DuckDB no disponible.", done=True)
        return

    record_count = get_current_record_count()

    # Decide automatically whether phase 1 can be skipped
    if force:
        skip_phase1 = False
        reason = "recálculo forzado"
    elif _PHASE1_PATH.exists():
        try:
            p1_meta      = json.loads(_PHASE1_PATH.read_text())
            cached_count = p1_meta.get("record_count", -1)
            if cached_count == record_count:
                skip_phase1 = True
                reason = f"mismos registros ({record_count:,}) — saltando Fase 1"
            else:
                skip_phase1 = False
                reason = f"registros cambiaron ({cached_count:,} → {record_count:,})"
        except Exception:
            skip_phase1 = False
            reason = "caché Fase 1 ilegible"
    else:
        skip_phase1 = False
        reason = "sin caché de Fase 1"

    _write_status("running", 0.0, f"Iniciando… ({reason})", record_count=record_count)

    if skip_phase1:
        try:
            p1 = json.loads(_PHASE1_PATH.read_text())
            accumulator = {(c["grid_lat"], c["grid_lon"]): c for c in p1["cells"]}
            client_count = p1.get("client_count", 0)
            _log.info("Skipping phase 1 — loaded %d cells from disk", len(accumulator))
            _write_status("running", 0.70,
                          f"Fase 1 ya completada ({len(accumulator):,} zonas). Iniciando OSM…",
                          phase1_done=True, record_count=record_count)
        except Exception as exc:
            _log.warning("Could not load phase1 cache: %s — running full job", exc)
            skip_phase1 = False

    if not skip_phase1:
        try:
            con         = duckdb.connect(str(_DB_PATH), read_only=True)
            all_clients = [r[0] for r in con.execute(
                "SELECT DISTINCT clientId FROM merged_pos_eventtype ORDER BY clientId"
            ).fetchall()]
            con.close()
        except Exception as exc:
            _write_status("error", 0, str(exc), done=True)
            return

        _write_status("running", 0.01,
                      f"{len(all_clients)} clientes encontrados. Iniciando detección de paradas…",
                      record_count=record_count)
        accumulator = _run_phase1(all_clients, record_count)
        client_count = len(all_clients)
        _write_status("running", 0.70,
                      f"Fase 1 completa: {len(accumulator):,} zonas de parada. Iniciando OSM…",
                      phase1_done=True, record_count=record_count)

    # Sort by stop count for OSM prioritization
    all_cells = sorted(accumulator.items(), key=lambda kv: -kv[1]["stop_count"])
    _run_phase2(all_cells, record_count, client_count)


# ── Public API ────────────────────────────────────────────────────────────────

def start_batch_job(force: bool = False) -> dict:
    with _job_lock:
        current = get_status()
        if current.get("phase") == "running":
            return {"started": False, "reason": "Ya hay un job en curso.", "status": current}
        _write_status("running", 0.0, "Iniciando…")
        t = threading.Thread(target=_run_job, kwargs={"force": force}, daemon=True)
        t.start()
        return {"started": True, "status": get_status()}
