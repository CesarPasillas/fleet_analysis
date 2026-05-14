from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from app.analytics_store import get_default_duckdb_path
from app.route_cache import get_cached_coverage, store_cached_coverage

_log = logging.getLogger(__name__)

_CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "route_geometry_training_batch.csv"


def _normalize_label(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            decoded = json.loads(text)
            if isinstance(decoded, list):
                return " | ".join(str(i).strip() for i in decoded if str(i).strip())
        except json.JSONDecodeError:
            pass
        return text
    return str(value).strip()


def _load_df() -> pd.DataFrame:
    if not _CSV_PATH.exists():
        return pd.DataFrame()

    df = pd.read_csv(_CSV_PATH)

    label_col = next(
        (c for c in ["route_id", "route_sequence", "inferred_roads"] if c in df.columns),
        None,
    )
    if label_col is None:
        df["route_label"] = ""
    else:
        df["route_label"] = df[label_col].apply(_normalize_label)

    df["route_label"] = df["route_label"].fillna("").astype(str).str.strip()

    if "start_timestamp" in df.columns:
        df["start_dt"] = pd.to_datetime(df["start_timestamp"], errors="coerce", utc=True)
        df["year_month"] = df["start_dt"].dt.to_period("M").astype(str)

    for col in ("distance_km", "duration_hours"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ("asset_id", "client_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def _top_routes_rows(grp: pd.DataFrame, extra_cols: list[str] | None = None) -> list[dict]:
    rows = []
    for _, row in grp.iterrows():
        entry: dict = {
            "route_label": row["route_label"],
            "trip_count": int(row["trip_count"]),
            "avg_distance_km": round(float(row["avg_distance_km"]), 2) if pd.notna(row.get("avg_distance_km")) else None,
            "avg_duration_hours": round(float(row["avg_duration_hours"]), 2) if pd.notna(row.get("avg_duration_hours")) else None,
        }
        for col in (extra_cols or []):
            val = row.get(col)
            entry[col] = int(val) if pd.notna(val) else 0
        rows.append(entry)
    return rows


def get_fleet_summary(
    limit: int = 20,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    df = _load_df()
    if df.empty:
        return {"error": "CSV de rutas no encontrado.", "stats": {}, "top_routes": [], "monthly_counts": []}

    if date_from and "start_dt" in df.columns:
        df = df[df["start_dt"] >= pd.Timestamp(date_from, tz="UTC")]
    if date_to and "start_dt" in df.columns:
        df = df[df["start_dt"] <= pd.Timestamp(date_to, tz="UTC")]

    df_l = df[df["route_label"] != ""].copy()

    stats = {
        "total_trips": int(len(df_l)),
        "unique_routes": int(df_l["route_label"].nunique()),
        "unique_assets": int(df_l["asset_id"].nunique()) if "asset_id" in df_l.columns else 0,
        "unique_clients": int(df_l["client_id"].nunique()) if "client_id" in df_l.columns else 0,
    }

    agg: dict = {"trip_count": ("route_label", "count")}
    if "distance_km" in df_l.columns:
        agg["avg_distance_km"] = ("distance_km", "mean")
    if "duration_hours" in df_l.columns:
        agg["avg_duration_hours"] = ("duration_hours", "mean")
    if "asset_id" in df_l.columns:
        agg["unique_assets"] = ("asset_id", "nunique")
    if "client_id" in df_l.columns:
        agg["unique_clients"] = ("client_id", "nunique")

    grp = (
        df_l.groupby("route_label")
        .agg(**agg)
        .reset_index()
        .sort_values("trip_count", ascending=False)
        .head(limit)
    )

    top_routes = _top_routes_rows(grp, extra_cols=["unique_assets", "unique_clients"])

    monthly_counts: list[dict] = []
    if "year_month" in df_l.columns:
        monthly = (
            df_l.groupby("year_month")
            .size()
            .reset_index(name="trip_count")
            .sort_values("year_month")
        )
        monthly_counts = [
            {"month": r["year_month"], "trip_count": int(r["trip_count"])}
            for _, r in monthly.iterrows()
        ]

    return {"stats": stats, "top_routes": top_routes, "monthly_counts": monthly_counts}


def get_client_assets(client_id: int, limit: int = 200) -> tuple[list[dict], int]:
    db_path = get_default_duckdb_path()
    if db_path.exists():
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            total = con.execute(
                "SELECT COUNT(DISTINCT assetId) FROM merged_pos_eventtype WHERE clientId = ?",
                [client_id],
            ).fetchone()[0]
            rows = con.execute(
                """
                SELECT assetId, COUNT(*) AS record_count
                FROM merged_pos_eventtype
                WHERE clientId = ?
                GROUP BY assetId
                ORDER BY record_count DESC
                LIMIT ?
                """,
                [client_id, limit],
            ).fetchall()
            con.close()
            if rows:
                assets = [{"asset_id": int(r[0]), "trip_count": int(r[1]), "unique_routes": 0} for r in rows]
                return assets, int(total)
        except Exception:
            pass

    # Fallback: training CSV
    df = _load_df()
    if df.empty or "client_id" not in df.columns or "asset_id" not in df.columns:
        return [], 0
    df_c = df[df["client_id"] == client_id].copy()
    total_csv = int(df_c["asset_id"].nunique())
    grp = (
        df_c.groupby("asset_id")
        .agg(trip_count=("route_label", "count"), unique_routes=("route_label", "nunique"))
        .reset_index()
        .sort_values("trip_count", ascending=False)
        .head(limit)
    )
    assets = [
        {"asset_id": int(r["asset_id"]), "trip_count": int(r["trip_count"]), "unique_routes": int(r["unique_routes"])}
        for _, r in grp.iterrows()
    ]
    return assets, total_csv


def get_client_summary(client_id: int, limit: int = 20, asset_ids: list[int] | None = None) -> dict:
    df = _load_df()
    if df.empty:
        return {"error": "CSV de rutas no encontrado.", "stats": {}, "top_routes": [], "monthly_counts": []}

    if "client_id" not in df.columns:
        return {"error": "No hay columna client_id.", "stats": {}, "top_routes": [], "monthly_counts": []}

    df_c = df[(df["client_id"] == client_id) & (df["route_label"] != "")].copy()

    if asset_ids:
        df_c = df_c[df_c["asset_id"].isin(asset_ids)]

    if df_c.empty:
        return {
            "error": f"Sin rutas para clientId={client_id}.",
            "stats": {},
            "top_routes": [],
            "monthly_counts": [],
        }

    stats = {
        "total_trips": int(len(df_c)),
        "unique_routes": int(df_c["route_label"].nunique()),
        "unique_assets": int(df_c["asset_id"].nunique()) if "asset_id" in df_c.columns else 0,
    }

    agg: dict = {"trip_count": ("route_label", "count")}
    if "distance_km" in df_c.columns:
        agg["avg_distance_km"] = ("distance_km", "mean")
    if "duration_hours" in df_c.columns:
        agg["avg_duration_hours"] = ("duration_hours", "mean")
    if "asset_id" in df_c.columns:
        agg["unique_assets"] = ("asset_id", "nunique")

    grp = (
        df_c.groupby("route_label")
        .agg(**agg)
        .reset_index()
        .sort_values("trip_count", ascending=False)
        .head(limit)
    )

    top_routes = _top_routes_rows(grp, extra_cols=["unique_assets"])

    monthly_counts: list[dict] = []
    if "year_month" in df_c.columns:
        monthly = (
            df_c.groupby("year_month")
            .size()
            .reset_index(name="trip_count")
            .sort_values("year_month")
        )
        monthly_counts = [
            {"month": r["year_month"], "trip_count": int(r["trip_count"])}
            for _, r in monthly.iterrows()
        ]

    return {
        "client_id": client_id,
        "stats": stats,
        "top_routes": top_routes,
        "monthly_counts": monthly_counts,
    }


# ---------------------------------------------------------------------------
# Análisis agregado de cobertura GPS por cliente
# ---------------------------------------------------------------------------

def _interpolate_positions_for_asset(df: pd.DataFrame) -> list[tuple[float, float, int]]:
    """
    Para un DataFrame de un asset, devuelve (lat, lon, asset_id) estimados
    de los registros que no tienen coordenadas GPS, usando interpolación temporal.
    """
    if "latitude" not in df.columns or "longitude" not in df.columns:
        return []

    # Ordenar por timestamp disponible
    ts_col = next((c for c in ("gpsTimestamp", "serverTimestamp") if c in df.columns), None)
    if ts_col:
        df = df.copy()
        df["_ts_ms"] = pd.to_numeric(df[ts_col], errors="coerce")
        df = df.sort_values("_ts_ms").reset_index(drop=True)

    lats = pd.to_numeric(df["latitude"], errors="coerce")
    lons = pd.to_numeric(df["longitude"], errors="coerce")
    has_gps = lats.notna() & lons.notna()

    # Índices y tiempos de puntos con GPS real
    valid_idx = np.where(has_gps)[0]
    if len(valid_idx) < 2:
        return []

    asset_id_val = int(df["assetId"].iloc[0]) if "assetId" in df.columns else 0
    ts_vals = df["_ts_ms"].values if "_ts_ms" in df.columns else np.arange(len(df), dtype=float)

    results: list[tuple[float, float, int]] = []

    for i in np.where(~has_gps)[0]:
        t_cur = float(ts_vals[i]) if np.isfinite(float(ts_vals[i])) else i

        # Punto válido anterior y siguiente
        prev_candidates = valid_idx[valid_idx < i]
        next_candidates = valid_idx[valid_idx > i]
        if len(prev_candidates) == 0 or len(next_candidates) == 0:
            continue

        pi = prev_candidates[-1]
        ni = next_candidates[0]
        t_prev = float(ts_vals[pi])
        t_next = float(ts_vals[ni])
        if t_next <= t_prev:
            continue

        ratio = max(0.0, min(1.0, (t_cur - t_prev) / (t_next - t_prev)))
        est_lat = float(lats.iloc[pi]) + ratio * (float(lats.iloc[ni]) - float(lats.iloc[pi]))
        est_lon = float(lons.iloc[pi]) + ratio * (float(lons.iloc[ni]) - float(lons.iloc[pi]))
        results.append((round(est_lat, 6), round(est_lon, 6), asset_id_val))

    return results


def get_signal_loss_coverage(
    client_id: int,
    limit_assets: int = 20,
    max_records_per_asset: int = 30_000,
    grid_size_deg: float = 0.01,
    force_refresh: bool = False,
) -> dict:
    """
    Analiza la cobertura GPS de los top-N assets de un cliente.

    Para cada asset carga sus registros, identifica los eventos sin coordenadas
    GPS (perdida de señal) e interpola su posición usando los puntos válidos
    circundantes. Agrupa en celdas de `grid_size_deg` grados (~1 km) y clasifica:
      - zona_sin_cobertura : ≥2 assets distintos sufrieron pérdida en esa celda
      - evento_aislado     : solo 1 asset tuvo pérdida en esa celda
    """
    if not force_refresh:
        cached = get_cached_coverage(client_id)
        if cached is not None:
            return cached

    db_path = get_default_duckdb_path()
    if not db_path.exists():
        return {"error": "DuckDB no disponible.", "hotspots": [], "total_assets_analyzed": 0}

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        asset_rows = con.execute(
            "SELECT assetId, COUNT(*) AS cnt FROM merged_pos_eventtype "
            "WHERE clientId = ? GROUP BY assetId ORDER BY cnt DESC LIMIT ?",
            [client_id, limit_assets],
        ).fetchall()
        con.close()
    except Exception as exc:
        return {"error": str(exc), "hotspots": [], "total_assets_analyzed": 0}

    if not asset_rows:
        return {"error": f"Sin assets para clientId={client_id}.", "hotspots": [], "total_assets_analyzed": 0}

    # cell (round_lat, round_lon) → set de asset_ids
    cell_assets: dict[tuple[float, float], set[int]] = defaultdict(set)
    cell_events: dict[tuple[float, float], int] = defaultdict(int)

    total_analyzed = 0
    total_lost_points = 0
    errors = 0

    for (asset_id, _) in asset_rows:
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            df = con.execute(
                "SELECT * FROM merged_pos_eventtype WHERE clientId = ? AND assetId = ? LIMIT ?",
                [client_id, int(asset_id), max_records_per_asset],
            ).df()
            con.close()

            if df.empty:
                continue

            lost_pts = _interpolate_positions_for_asset(df)
            total_lost_points += len(lost_pts)

            for (lat, lon, aid) in lost_pts:
                cell_lat = round(round(lat / grid_size_deg) * grid_size_deg, 5)
                cell_lon = round(round(lon / grid_size_deg) * grid_size_deg, 5)
                cell = (cell_lat, cell_lon)
                cell_assets[cell].add(aid)
                cell_events[cell] += 1

            total_analyzed += 1
        except Exception as exc:
            _log.warning("Error procesando assetId=%s: %s", asset_id, exc)
            errors += 1

    hotspots = []
    for (cell_lat, cell_lon), asset_set in cell_assets.items():
        n_assets = len(asset_set)
        n_events = cell_events[(cell_lat, cell_lon)]
        hotspots.append(
            {
                "lat": cell_lat,
                "lon": cell_lon,
                "asset_count": n_assets,
                "event_count": n_events,
                "category": "zona_sin_cobertura" if n_assets >= 2 else "evento_aislado",
                "asset_ids": sorted(asset_set),
            }
        )

    hotspots.sort(key=lambda h: (-h["asset_count"], -h["event_count"]))

    structural = sum(1 for h in hotspots if h["category"] == "zona_sin_cobertura")

    result = {
        "client_id": client_id,
        "total_assets_analyzed": total_analyzed,
        "total_hotspots": len(hotspots),
        "structural_zones": structural,
        "isolated_events": len(hotspots) - structural,
        "total_lost_points": total_lost_points,
        "hotspots": hotspots[:1000],
        "errors": errors,
        "cached_at": pd.Timestamp.utcnow().isoformat(),
        "from_cache": False,
    }
    store_cached_coverage(client_id, result)
    return result


def get_fleet_heatmap(limit_routes: int = 12, limit_clients: int = 10) -> dict:
    df = _load_df()
    if df.empty or "client_id" not in df.columns:
        return {"routes": [], "clients": [], "matrix": []}

    df_l = df[df["route_label"] != ""].copy()

    top_routes = df_l["route_label"].value_counts().head(limit_routes).index.tolist()
    top_clients = df_l["client_id"].value_counts().head(limit_clients).index.tolist()

    df_sub = df_l[df_l["route_label"].isin(top_routes) & df_l["client_id"].isin(top_clients)]

    if df_sub.empty:
        return {"routes": top_routes, "clients": [str(c) for c in top_clients], "matrix": []}

    pivot = (
        df_sub.groupby(["client_id", "route_label"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=top_clients, columns=top_routes, fill_value=0)
    )

    return {
        "routes": top_routes,
        "clients": [str(c) for c in top_clients],
        "matrix": [[int(v) for v in row] for row in pivot.values.tolist()],
    }
