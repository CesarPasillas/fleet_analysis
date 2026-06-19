"""
Historical stop analysis across all assets and routes.
Uses EventType keys directly from DuckDB — no per-asset stop detection needed.
"""
from __future__ import annotations

import duckdb
from pathlib import Path

_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "fleet_analytics.duckdb"
_GRID    = 0.01   # ~1 km

_KEY_TO_CAT = {
    "IGNITION_OFF":           "motor_apagado",
    "PARKING_OVERTIME":       "estacionamiento_prolongado",
    "NIGHT_PARKING_VIOLATION":"pernocta_fuera_de_base",
    "UNAUTHORIZED_STOP":      "parada_no_autorizada",
    "EXCESSIVE_IDLE":         "ralenti",
    "IDLE_STOP":              "ralenti",
    "REPORT_IDLE":            "ralenti",
    "IDLE_FUEL_CONSUMPTION":  "ralenti",
    "GEOFENCE_STOP_OVERTIME": "parada_en_geocerca",
    "CHECKPOINT_REACHED":     "punto_de_control",
    "DELIVERY_COMPLETED":     "entrega",
}

_CAT_ICONS = {
    "ralenti":                   "⏳",
    "motor_apagado":             "🔌",
    "entrega":                   "📦",
    "punto_de_control":          "✅",
    "parada_en_geocerca":        "📍",
    "estacionamiento_prolongado":"🅿",
    "pernocta_fuera_de_base":    "🌙",
    "parada_no_autorizada":      "⛔",
}

_STOP_KEYS = list(_KEY_TO_CAT.keys())


def get_historical_stop_analysis(top_locations: int = 30) -> dict:
    if not _DB_PATH.exists():
        return {"error": "DuckDB no disponible."}

    placeholders = ", ".join(f"'{k}'" for k in _STOP_KEYS)

    try:
        con = duckdb.connect(str(_DB_PATH), read_only=True)

        # ── 1. Category distribution ──────────────────────────────────────
        cat_rows = con.execute(f"""
            SELECT
                Key,
                COUNT(*)                AS total_events,
                COUNT(DISTINCT assetId) AS unique_assets,
                COUNT(DISTINCT clientId)AS unique_clients
            FROM merged_pos_eventtype
            WHERE Key IN ({placeholders})
              AND latitude  IS NOT NULL AND ABS(latitude)  > 0.01
              AND longitude IS NOT NULL AND ABS(longitude) > 0.01
            GROUP BY Key
            ORDER BY total_events DESC
        """).fetchall()

        # Aggregate by mapped category
        cat_agg: dict[str, dict] = {}
        for key, events, assets, clients in cat_rows:
            cat = _KEY_TO_CAT.get(key, key)
            if cat not in cat_agg:
                cat_agg[cat] = {"category": cat, "icon": _CAT_ICONS.get(cat, "📌"),
                                "total_events": 0, "unique_assets": 0,
                                "unique_clients": 0, "event_keys": []}
            cat_agg[cat]["total_events"]   += events
            cat_agg[cat]["unique_assets"]  += assets
            cat_agg[cat]["unique_clients"] += clients
            cat_agg[cat]["event_keys"].append(key)

        categories = sorted(cat_agg.values(), key=lambda c: -c["total_events"])
        total_events = sum(c["total_events"] for c in categories)
        for c in categories:
            c["pct"] = round(c["total_events"] / total_events * 100, 2) if total_events else 0

        # ── 2. Top geographic locations ───────────────────────────────────
        loc_rows = con.execute(f"""
            SELECT
                ROUND(ROUND(latitude  / {_GRID}) * {_GRID}, 5) AS grid_lat,
                ROUND(ROUND(longitude / {_GRID}) * {_GRID}, 5) AS grid_lon,
                COUNT(*)                AS total_events,
                COUNT(DISTINCT assetId) AS unique_assets,
                COUNT(DISTINCT clientId)AS unique_clients,
                LIST(DISTINCT Key)[:5]  AS top_keys
            FROM merged_pos_eventtype
            WHERE Key IN ({placeholders})
              AND latitude  IS NOT NULL AND ABS(latitude)  > 0.01
              AND longitude IS NOT NULL AND ABS(longitude) > 0.01
            GROUP BY grid_lat, grid_lon
            ORDER BY total_events DESC
            LIMIT {top_locations}
        """).fetchall()

        con.close()

        top_locs = []
        for r in loc_rows:
            grid_lat, grid_lon, events, assets, clients, keys = r
            # Dominant category in this cell based on key list
            cat_counts: dict[str, int] = {}
            for k in (keys or []):
                c = _KEY_TO_CAT.get(k, k)
                cat_counts[c] = cat_counts.get(c, 0) + 1
            dominant = max(cat_counts, key=cat_counts.get) if cat_counts else "ralenti"
            top_locs.append({
                "lat":            grid_lat,
                "lon":            grid_lon,
                "total_events":   events,
                "unique_assets":  assets,
                "unique_clients": clients,
                "dominant_cat":   dominant,
                "icon":           _CAT_ICONS.get(dominant, "📌"),
            })

        return {
            "total_stop_events": total_events,
            "total_categories":  len(categories),
            "categories":        categories,
            "top_locations":     top_locs,
        }

    except Exception as exc:
        return {"error": str(exc)}
