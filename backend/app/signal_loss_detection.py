from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


_SERVER_TOLERANCE_MS: int = 2_000    # 2 s → misma ráfaga de transmisión (ventana estricta)
_MIN_GPS_SPAN_SECONDS: float = 300.0 # 5 min → span GPS mínimo para calificar como pérdida real
_MIN_BATCH_RECORDS: int = 3          # mínimo de registros en la misma ráfaga


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _to_ms(series: pd.Series) -> pd.Series:
    """Normaliza a milisegundos usando la misma heurística que route_detection._to_readable_datetime."""
    vals = pd.to_numeric(series, errors="coerce")
    med = float(vals.abs().median())
    if med >= 1e17:
        return vals / 1_000_000  # ns → ms
    if med >= 1e14:
        return vals / 1_000      # µs → ms
    if med >= 1e11:
        return vals              # ya está en ms
    return vals * 1_000          # s → ms


def _iso(ms: float) -> str | None:
    try:
        return pd.Timestamp(ms, unit="ms", tz="UTC").isoformat()
    except Exception:
        return None


def _assign_batch_ids(server_ms_sorted: np.ndarray, tolerance_ms: int) -> np.ndarray:
    """
    Asigna IDs de ráfaga a registros ya ordenados por serverTimestamp.
    Registros dentro de tolerance_ms entre sí comparten el mismo batch ID.
    """
    ids = np.ones(len(server_ms_sorted), dtype=np.int64)
    batch = 1
    for i in range(1, len(server_ms_sorted)):
        if (server_ms_sorted[i] - server_ms_sorted[i - 1]) > tolerance_ms:
            batch += 1
        ids[i] = batch
    return ids


def _empty_result(error: str | None = None) -> dict[str, Any]:
    return {
        "total_records": 0,
        "signal_loss_segments_count": 0,
        "total_signal_loss_minutes": 0.0,
        "segments": [],
        "all_points": [],
        "error": error,
    }


def _prepare_working_df(
    df: pd.DataFrame,
    gps_col: str,
    srv_col: str,
    lat_col: str | None,
    lon_col: str | None,
) -> pd.DataFrame:
    cols = [c for c in [gps_col, srv_col, lat_col, lon_col] if c]
    w = df[cols].copy()
    w["_gps_ms"] = _to_ms(w[gps_col])
    w["_srv_ms"] = _to_ms(w[srv_col])
    w = w.dropna(subset=["_gps_ms", "_srv_ms"])
    return w[(w["_gps_ms"] > 0) & (w["_srv_ms"] > 0)].copy()


def _build_segments(
    batch_stats: pd.DataFrame,
    loss_batch_ids: set[int],
    w: pd.DataFrame,
    lat_col: str | None,
    lon_col: str | None,
) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    for _, row in batch_stats[batch_stats["_batch_id"].isin(loss_batch_ids)].iterrows():
        batch_rows = w[w["_batch_id"] == row["_batch_id"]]
        coords: list[list[float]] = []
        if lat_col and lon_col:
            coords = batch_rows[[lat_col, lon_col]].dropna().values.tolist()
        segments.append(
            {
                "gps_start": _iso(float(row["gps_min"])),
                "gps_end": _iso(float(row["gps_max"])),
                "server_received_at": _iso(float(row["srv_val"])),
                "duration_minutes": round(float(row["gps_span_s"]) / 60.0, 2),
                "num_records": int(row["count"]),
                "coordinates": coords,
            }
        )
    segments.sort(key=lambda s: s["gps_start"] or "")
    return segments


def _build_all_points(
    w: pd.DataFrame,
    lat_col: str | None,
    lon_col: str | None,
) -> list[dict[str, Any]]:
    all_points: list[dict[str, Any]] = []
    for _, row in w.iterrows():
        lat = float(row[lat_col]) if lat_col and pd.notna(row.get(lat_col)) else None
        lon = float(row[lon_col]) if lon_col and pd.notna(row.get(lon_col)) else None
        all_points.append(
            {
                "gps_timestamp": _iso(float(row["_gps_ms"])),
                "server_timestamp": _iso(float(row["_srv_ms"])),
                "diff_seconds": round(float(row["_diff_s"]), 1),
                "latitude": lat,
                "longitude": lon,
                "is_signal_loss": bool(row["_is_loss"]),
            }
        )
    return all_points


def detect_signal_loss(
    df: pd.DataFrame,
    server_tolerance_ms: int = _SERVER_TOLERANCE_MS,
    min_gps_span_seconds: float = _MIN_GPS_SPAN_SECONDS,
    min_batch_records: int = _MIN_BATCH_RECORDS,
) -> dict[str, Any]:
    """
    Detecta periodos sin señal comparando GPSTimeStamp vs ServerTimeStamp.

    Señal: múltiples registros con GPSTimeStamps distintos llegan con el mismo
    ServerTimeStamp (±server_tolerance_ms), indicando transmisión en lote tras
    un periodo offline.
    """
    if df is None or df.empty:
        return _empty_result()

    gps_col = _find_col(df, ["gpsTimestamp", "gps_timestamp", "GPSTimeStamp"])
    srv_col = _find_col(df, ["serverTimestamp", "server_timestamp", "ServerTimeStamp"])
    if not gps_col or not srv_col:
        return _empty_result(error="Faltan columnas gpsTimestamp / serverTimestamp en los datos")

    lat_col = _find_col(df, ["latitude", "lat"])
    lon_col = _find_col(df, ["longitude", "lon"])

    w = _prepare_working_df(df, gps_col, srv_col, lat_col, lon_col)
    if w.empty:
        return _empty_result()

    # Asignar batch IDs ordenando por serverTimestamp
    w_by_srv = w.sort_values("_srv_ms").reset_index(drop=True)
    w_by_srv["_batch_id"] = _assign_batch_ids(
        w_by_srv["_srv_ms"].values.astype(float), server_tolerance_ms
    )
    w = w_by_srv.sort_values("_gps_ms").reset_index(drop=True)
    w["_diff_s"] = (w["_srv_ms"] - w["_gps_ms"]) / 1000.0

    batch_stats = (
        w.groupby("_batch_id")
        .agg(count=("_gps_ms", "count"), gps_min=("_gps_ms", "min"),
             gps_max=("_gps_ms", "max"), srv_val=("_srv_ms", "first"))
        .reset_index()
    )
    batch_stats["gps_span_s"] = (batch_stats["gps_max"] - batch_stats["gps_min"]) / 1000.0

    loss_mask = (
        (batch_stats["count"] >= min_batch_records)
        & (batch_stats["gps_span_s"] >= min_gps_span_seconds)
    )
    loss_batch_ids: set[int] = set(batch_stats.loc[loss_mask, "_batch_id"].tolist())
    w["_is_loss"] = w["_batch_id"].isin(loss_batch_ids)

    segments = _build_segments(batch_stats, loss_batch_ids, w, lat_col, lon_col)
    all_points = _build_all_points(w, lat_col, lon_col)

    return {
        "total_records": len(w),
        "signal_loss_segments_count": len(segments),
        "total_signal_loss_minutes": round(sum(s["duration_minutes"] for s in segments), 2),
        "segments": segments,
        "all_points": all_points,
        "error": None,
    }
