"""
Match GPS signal-loss hotspots against road segments (carreteras_raw).

Strategy: build a grid-cell → road_name index from road LINESTRING points,
then look up each hotspot's cell (and its 8 neighbors for edge cases).
Grid size matches the hotspot grid (0.01° ≈ 1 km).
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

import duckdb

_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "fleet_analytics.duckdb"
_COV_PATH = Path(__file__).resolve().parents[1] / "data" / "global_coverage.json"

_GRID = 0.01   # must match global_coverage._GRID_SIZE

_road_grid_cache: dict[tuple[float, float], set[str]] | None = None


# ── Geometry helpers ──────────────────────────────────────────────────────────

def _parse_linestring(wkt: str) -> list[tuple[float, float]]:
    """Parse 'LINESTRING(lon lat, lon lat, ...)' → list of (lat, lon)."""
    coords_str = re.sub(r"^LINESTRING\s*\(", "", wkt.strip(), flags=re.I).rstrip(")")
    points = []
    for pair in coords_str.split(","):
        parts = pair.strip().split()
        if len(parts) >= 2:
            try:
                lon, lat = float(parts[0]), float(parts[1])
                points.append((lat, lon))
            except ValueError:
                pass
    return points


def _cell(lat: float, lon: float, grid: float) -> tuple[float, float]:
    return (round(round(lat / grid) * grid, 5), round(round(lon / grid) * grid, 5))


def _neighbors(cell: tuple[float, float], grid: float) -> list[tuple[float, float]]:
    """Return the cell and its 8 neighbors (for boundary matching)."""
    r, c = cell
    return [
        _cell(r + dr * grid, c + dc * grid, grid)
        for dr in (-1, 0, 1)
        for dc in (-1, 0, 1)
    ]


def get_road_grid_index() -> dict[tuple[float, float], set[str]]:
    """Cell → set of road names, built once from carreteras_raw and cached in-process."""
    global _road_grid_cache
    if _road_grid_cache is not None:
        return _road_grid_cache

    cell_to_roads: dict[tuple[float, float], set[str]] = defaultdict(set)
    if _DB_PATH.exists():
        try:
            con = duckdb.connect(str(_DB_PATH), read_only=True)
            rows = con.execute(
                "SELECT name, geometry FROM carreteras_raw WHERE geometry IS NOT NULL"
            ).fetchall()
            con.close()
            for name, geometry in rows:
                if not geometry or not name:
                    continue
                for lat, lon in _parse_linestring(geometry):
                    cell_to_roads[_cell(lat, lon, _GRID)].add(name)
        except Exception:
            pass

    _road_grid_cache = cell_to_roads
    return _road_grid_cache


def match_road_for_coordinates(coords: list[list[float]]) -> str | None:
    """Majority-vote road name among the cells covered by a list of [lat, lon] points."""
    if not coords:
        return None
    index = get_road_grid_index()
    if not index:
        return None

    counts: dict[str, int] = defaultdict(int)
    for lat, lon in coords:
        cell = _cell(lat, lon, _GRID)
        for ncell in _neighbors(cell, _GRID):
            for name in index.get(ncell, ()):
                counts[name] += 1

    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


# ── Core match ────────────────────────────────────────────────────────────────

def compute_road_coverage_match(match_radius_cells: int = 1) -> dict:
    """
    Returns per-road summary of GPS signal-loss hotspots.

    match_radius_cells=1 means we look at the hotspot cell + its 8 neighbors
    (effectively a ~1-3 km match radius depending on grid size).
    """
    if not _DB_PATH.exists():
        return {"error": "DuckDB no disponible.", "roads": []}
    if not _COV_PATH.exists():
        return {"error": "No hay resultado de cobertura global. Calcula primero la cobertura global.", "roads": []}

    # ── Load road segments ────────────────────────────────────────────────────
    try:
        con = duckdb.connect(str(_DB_PATH), read_only=True)
        roads = con.execute(
            "SELECT name, dataset, p_type, p_longitud, geometry FROM carreteras_raw WHERE geometry IS NOT NULL"
        ).fetchall()
        con.close()
    except Exception as exc:
        return {"error": str(exc), "roads": []}

    # Build grid index: cell → set of road names
    cell_to_roads: dict[tuple[float, float], set[str]] = defaultdict(set)
    road_meta: dict[str, dict] = {}

    for name, dataset, p_type, p_longitud, geometry in roads:
        if not geometry or not name:
            continue
        road_meta[name] = {
            "name":     name,
            "tipo":     dataset or "",
            "p_type":   p_type or "",
            "longitud_km": round(float(p_longitud), 1) if p_longitud else None,
        }
        for lat, lon in _parse_linestring(geometry):
            cell_to_roads[_cell(lat, lon, _GRID)].add(name)

    # ── Load hotspots ─────────────────────────────────────────────────────────
    try:
        cov = json.loads(_COV_PATH.read_text())
    except Exception as exc:
        return {"error": f"No se pudo leer global_coverage.json: {exc}", "roads": []}

    structural = cov.get("structural_hotspots", [])
    isolated   = cov.get("isolated_hotspots",   [])

    # ── Match hotspots → roads ────────────────────────────────────────────────
    # road_name → {structural_count, isolated_count, total_events, asset_set, hotspot_cells}
    road_stats: dict[str, dict] = defaultdict(lambda: {
        "structural_count": 0,
        "isolated_count":   0,
        "structural_events": 0,
        "isolated_events":  0,
        "max_assets":        0,
        "hotspot_cells":     [],
    })

    def _match_hotspots(hotspot_list: list[dict], category: str) -> None:
        for h in hotspot_list:
            hcell = _cell(h["lat"], h["lon"], _GRID)
            matched_roads: set[str] = set()
            for ncell in _neighbors(hcell, _GRID):
                matched_roads.update(cell_to_roads.get(ncell, set()))
            for road_name in matched_roads:
                s = road_stats[road_name]
                if category == "structural":
                    s["structural_count"]  += 1
                    s["structural_events"] += h.get("event_count", 0)
                else:
                    s["isolated_count"]    += 1
                    s["isolated_events"]   += h.get("event_count", 0)
                s["max_assets"] = max(s["max_assets"], h.get("asset_count", 0))

    _match_hotspots(structural, "structural")
    _match_hotspots(isolated,   "isolated")

    # ── Build result list ─────────────────────────────────────────────────────
    result_roads = []
    for road_name, stats in road_stats.items():
        total_hotspots = stats["structural_count"] + stats["isolated_count"]
        total_events   = stats["structural_events"] + stats["isolated_events"]
        if total_hotspots == 0:
            continue
        meta = road_meta.get(road_name, {})

        # Severity: based on structural zones (more critical)
        if stats["structural_count"] >= 10:
            severity = "alta"
        elif stats["structural_count"] >= 3:
            severity = "media"
        elif stats["structural_count"] >= 1:
            severity = "baja"
        else:
            severity = "aislada"

        result_roads.append({
            "name":               road_name,
            "tipo":               meta.get("tipo", ""),
            "p_type":             meta.get("p_type", ""),
            "longitud_km":        meta.get("longitud_km"),
            "structural_zones":   stats["structural_count"],
            "isolated_events":    stats["isolated_count"],
            "total_hotspots":     total_hotspots,
            "structural_events":  stats["structural_events"],
            "isolated_ev_count":  stats["isolated_events"],
            "total_events":       total_events,
            "max_assets_in_zone": stats["max_assets"],
            "severity":           severity,
        })

    result_roads.sort(key=lambda r: (-r["structural_zones"], -r["total_events"]))

    # Roads with no hotspot match (all-clean roads)
    matched_road_names = {r["name"] for r in result_roads}
    unaffected = len(road_meta) - len(matched_road_names)

    return {
        "total_roads_analyzed":  len(road_meta),
        "roads_with_issues":     len(result_roads),
        "roads_unaffected":      unaffected,
        "roads":                 result_roads,
        "structural_hotspots_total": len(structural),
        "isolated_hotspots_total":   len(isolated),
    }
