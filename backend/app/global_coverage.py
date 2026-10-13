"""
Global GPS coverage analysis across all clients and assets.

Uses DuckDB window functions to interpolate positions for null-GPS records,
then grids and aggregates to identify structural signal-loss hotspots.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

import duckdb

_log = logging.getLogger(__name__)

_GLOBAL_CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "global_coverage.json"
_STATUS_PATH = Path(__file__).resolve().parents[1] / "data" / "global_coverage_status.json"
_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "fleet_analytics.duckdb"

_GRID_SIZE = 0.01        # ~1 km grid cells
_BATCH_SIZE = 50         # clients per DuckDB query batch
_MIN_ASSETS = 2          # min assets to classify as structural zone

_job_lock = threading.Lock()


# ── Status helpers ────────────────────────────────────────────────────────────

def _write_status(status: str, progress: float, message: str = "", done: bool = False) -> None:
    try:
        _STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STATUS_PATH.write_text(json.dumps({
            "status": status,
            "progress": round(progress, 3),
            "message": message,
            "done": done,
            "updated_at": time.time(),
        }))
    except Exception:
        pass


def get_status() -> dict:
    if not _STATUS_PATH.exists():
        return {"status": "idle", "progress": 0, "message": "", "done": False}
    try:
        return json.loads(_STATUS_PATH.read_text())
    except Exception:
        return {"status": "idle", "progress": 0, "message": "", "done": False}


def get_cached_result() -> dict | None:
    if not _GLOBAL_CACHE_PATH.exists():
        return None
    try:
        return json.loads(_GLOBAL_CACHE_PATH.read_text())
    except Exception:
        return None


# ── Core computation ──────────────────────────────────────────────────────────

def _batch_query(client_ids: list[int], grid: float) -> list[dict]:
    """
    Run one DuckDB query for a batch of clients.
    Uses the 'islands' technique: for each null-GPS record, assign the position
    of the last preceding valid GPS record (same valid_group).
    """
    placeholders = ", ".join("?" * len(client_ids))
    sql = f"""
    WITH ordered AS (
        SELECT
            clientId,
            assetId,
            serverTimestamp,
            latitude,
            longitude,
            -- Count valid GPS records seen so far per asset (creates group IDs)
            SUM(CASE
                    WHEN latitude IS NOT NULL
                     AND longitude IS NOT NULL
                     AND ABS(latitude) > 0.01
                     AND ABS(longitude) > 0.01
                    THEN 1 ELSE 0
                END)
            OVER (PARTITION BY assetId ORDER BY serverTimestamp
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS valid_group
        FROM merged_pos_eventtype
        WHERE clientId IN ({placeholders})
          AND serverTimestamp IS NOT NULL
    ),
    last_valid AS (
        -- Last known valid GPS position per (asset, valid_group)
        SELECT assetId, valid_group,
               MAX(latitude)  AS grp_lat,
               MAX(longitude) AS grp_lon
        FROM ordered
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND ABS(latitude) > 0.01
          AND ABS(longitude) > 0.01
        GROUP BY assetId, valid_group
    ),
    lost AS (
        -- Join null-GPS records to their last known position
        SELECT o.clientId, o.assetId,
               lv.grp_lat AS approx_lat,
               lv.grp_lon AS approx_lon
        FROM ordered o
        JOIN last_valid lv
          ON lv.assetId = o.assetId
         AND lv.valid_group = o.valid_group
        WHERE (o.latitude IS NULL
            OR o.longitude IS NULL
            OR ABS(o.latitude) < 0.01)
    ),
    gridded AS (
        SELECT
            clientId,
            assetId,
            ROUND(ROUND(approx_lat / {grid}) * {grid}, 5) AS grid_lat,
            ROUND(ROUND(approx_lon / {grid}) * {grid}, 5) AS grid_lon
        FROM lost
    )
    SELECT
        grid_lat,
        grid_lon,
        COUNT(DISTINCT clientId)  AS client_count,
        COUNT(DISTINCT assetId)   AS asset_count,
        COUNT(*)                  AS event_count,
        LIST(DISTINCT CAST(clientId AS VARCHAR))[:10] AS sample_clients,
        LIST(DISTINCT CAST(assetId  AS VARCHAR))[:10] AS sample_assets
    FROM gridded
    GROUP BY grid_lat, grid_lon
    HAVING asset_count >= 1
    """
    try:
        con = duckdb.connect(str(_DB_PATH), read_only=True)
        rows = con.execute(sql, client_ids).fetchall()
        con.close()
        return [
            {
                "grid_lat":       r[0],
                "grid_lon":       r[1],
                "client_count":   r[2],
                "asset_count":    r[3],
                "event_count":    r[4],
                "sample_clients": r[5],
                "sample_assets":  r[6],
            }
            for r in rows
        ]
    except Exception as exc:
        _log.warning("Batch error for clients %s: %s", client_ids[:3], exc)
        return []


def _merge_batches(
    accumulator: dict[tuple[float, float], dict],
    batch_rows: list[dict],
) -> None:
    """Merge batch results into a running accumulator keyed by (grid_lat, grid_lon)."""
    for row in batch_rows:
        key = (row["grid_lat"], row["grid_lon"])
        if key not in accumulator:
            accumulator[key] = {
                "grid_lat":    row["grid_lat"],
                "grid_lon":    row["grid_lon"],
                "client_count": 0,
                "asset_count":  0,
                "event_count":  0,
                "client_ids":   set(),
                "asset_ids":    set(),
            }
        cell = accumulator[key]
        cell["event_count"] += row["event_count"]
        cell["client_ids"].update(row["sample_clients"])
        cell["asset_ids"].update(row["sample_assets"])
        # client_count and asset_count need re-computing at the end from sets


def _run_job() -> None:
    if not _DB_PATH.exists():
        _write_status("error", 0, "DuckDB no disponible.", done=True)
        return

    _write_status("running", 0.0, "Obteniendo lista de clientes…")

    try:
        con = duckdb.connect(str(_DB_PATH), read_only=True)
        all_clients = [r[0] for r in con.execute(
            "SELECT DISTINCT clientId FROM merged_pos_eventtype ORDER BY clientId"
        ).fetchall()]
        con.close()
    except Exception as exc:
        _write_status("error", 0, f"Error al consultar clientes: {exc}", done=True)
        return

    total_clients = len(all_clients)
    _log.info("Global coverage: %d clientes a procesar en lotes de %d", total_clients, _BATCH_SIZE)
    _write_status("running", 0.01, f"{total_clients} clientes encontrados. Procesando…")

    accumulator: dict[tuple[float, float], dict] = {}
    processed = 0

    for i in range(0, total_clients, _BATCH_SIZE):
        batch = all_clients[i: i + _BATCH_SIZE]
        batch_rows = _batch_query(batch, _GRID_SIZE)
        _merge_batches(accumulator, batch_rows)
        processed += len(batch)
        progress = processed / total_clients
        _write_status(
            "running", progress,
            f"Procesados {processed}/{total_clients} clientes — "
            f"{len(accumulator)} celdas con pérdida de señal",
        )
        _log.info("Global coverage: %.0f%% (%d/%d clients, %d cells)",
                  progress * 100, processed, total_clients, len(accumulator))

    # Build final hotspot list
    hotspots = []
    for cell in accumulator.values():
        client_ids = sorted(cell["client_ids"])
        asset_ids = sorted(cell["asset_ids"])
        n_clients = len(client_ids)
        n_assets = len(asset_ids)
        category = "zona_sin_cobertura" if n_assets >= _MIN_ASSETS else "evento_aislado"
        hotspots.append({
            "lat":          cell["grid_lat"],
            "lon":          cell["grid_lon"],
            "client_count": n_clients,
            "asset_count":  n_assets,
            "event_count":  cell["event_count"],
            "category":     category,
            "client_ids":   client_ids[:20],
            "asset_ids":    asset_ids[:20],
        })

    hotspots.sort(key=lambda h: (-h["asset_count"], -h["event_count"]))

    structural_list = [h for h in hotspots if h["category"] == "zona_sin_cobertura"]
    isolated_list   = [h for h in hotspots if h["category"] == "evento_aislado"]
    isolated_list.sort(key=lambda h: -h["event_count"])

    result = {
        "total_clients":        total_clients,
        "total_hotspots":       len(hotspots),
        "structural_zones":     len(structural_list),
        "isolated_events":      len(isolated_list),
        "structural_hotspots":  structural_list[:1000],
        "isolated_hotspots":    isolated_list[:1000],
        "computed_at":          time.time(),
        "grid_size_deg":        _GRID_SIZE,
    }

    try:
        _GLOBAL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _GLOBAL_CACHE_PATH.write_text(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        _log.error("No se pudo guardar global_coverage.json: %s", exc)

    _write_status("done", 1.0,
                  f"Completado: {len(hotspots)} zonas · {len(structural_list)} estructurales · {len(isolated_list)} aisladas",
                  done=True)
    _log.info("Global coverage job done: %d hotspots", len(hotspots))


# ── Public API ────────────────────────────────────────────────────────────────

def start_global_coverage_job() -> dict:
    """Launch background job. Returns immediately with job status."""
    with _job_lock:
        current = get_status()
        if current.get("status") == "running":
            return {"started": False, "reason": "Ya hay un job en curso.", "status": current}

        _write_status("running", 0.0, "Iniciando…")
        t = threading.Thread(target=_run_job, daemon=True)
        t.start()
        return {"started": True, "status": get_status()}
