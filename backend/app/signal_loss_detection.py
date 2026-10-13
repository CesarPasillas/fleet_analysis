from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


# Un registro "llegó tarde" si el servidor lo recibió más de _LATE_DELAY_SECONDS después de
# tomarse (con señal la diferencia es de pocos segundos). Un corte es una serie seguida (en hora GPS)
# de registros tardíos. Antes se buscaban ráfagas de registros recibidos con <= 2 s entre sí, pero
# el dispositivo suelta lo acumulado poco a poco (asset 2728, 19-ene: 16:27–16:29) y la ráfaga se
# partía: la tabla mostraba un pedazo del corte (16:19–16:28, 5 registros) en vez del corte real
# (16:14–16:27, decenas de registros).
_LATE_DELAY_SECONDS: float = 60.0
_MERGE_GAP_SECONDS: float = 60.0     # registros a tiempo intercalados por menos que esto no cortan el corte
_MIN_GPS_SPAN_SECONDS: float = 300.0 # 5 min → span GPS mínimo para calificar como pérdida real
_MIN_LATE_RECORDS: int = 3           # mínimo de registros acumulados en el corte


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


def _late_runs(gps_ms: np.ndarray, late: np.ndarray, merge_gap_ms: float) -> list[tuple[int, int]]:
    """
    Series de registros tardíos (índices [i, j] en orden GPS). Dos series se unen si los registros
    a tiempo que las separan cubren menos de merge_gap_ms (el dispositivo a veces manda alguno en vivo
    en medio del corte).
    """
    runs: list[tuple[int, int]] = []
    idx = np.flatnonzero(late)
    if len(idx) == 0:
        return runs
    start = prev = int(idx[0])
    for k in idx[1:]:
        k = int(k)
        if k == prev + 1 or gps_ms[k] - gps_ms[prev] <= merge_gap_ms:
            prev = k
            continue
        runs.append((start, prev))
        start = prev = k
    runs.append((start, prev))
    return runs


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


def _match_road_name(coords: list[list[float]]) -> str | None:
    try:
        from app.road_coverage_match import match_road_for_coordinates
        return match_road_for_coordinates(coords)
    except Exception:
        return None


def _build_segments(
    w: pd.DataFrame,
    runs: list[tuple[int, int]],
    lat_col: str | None,
    lon_col: str | None,
) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    for i, j in runs:
        rows = w.iloc[i: j + 1]
        late_rows = rows[rows["_is_late"]]
        coords: list[list[float]] = []
        if lat_col and lon_col:
            coords = late_rows[[lat_col, lon_col]].dropna().values.tolist()
        gps_min, gps_max = float(late_rows["_gps_ms"].min()), float(late_rows["_gps_ms"].max())
        segments.append(
            {
                "gps_start": _iso(gps_min),
                "gps_end": _iso(gps_max),
                # Cuándo volvió la señal: primer registro acumulado que llegó al servidor.
                "server_received_at": _iso(float(late_rows["_srv_ms"].min())),
                "server_received_until": _iso(float(late_rows["_srv_ms"].max())),
                "max_delay_minutes": round(float(late_rows["_diff_s"].max()) / 60.0, 1),
                "duration_minutes": round((gps_max - gps_min) / 60000.0, 2),
                "num_records": int(len(late_rows)),
                "coordinates": coords,
                "road_name": _match_road_name(coords),
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
                "estimated_latitude": None,
                "estimated_longitude": None,
                "is_signal_loss": bool(row["_is_loss"]),
            }
        )
    return all_points


def _interpolate_missing_positions(all_points: list[dict[str, Any]]) -> None:
    """Add estimated_latitude/estimated_longitude for null-GPS points via temporal interpolation."""
    # Build index of (position_in_list, gps_timestamp_seconds, lat, lon) for valid GPS points
    valid: list[tuple[int, float, float, float]] = []
    for i, p in enumerate(all_points):
        if p.get("latitude") is None:
            continue
        ts = p.get("gps_timestamp")
        if not ts:
            continue
        try:
            t = pd.Timestamp(ts).timestamp()
            valid.append((i, t, float(p["latitude"]), float(p["longitude"])))
        except Exception:
            pass

    if len(valid) < 2:
        return

    for i, p in enumerate(all_points):
        if p.get("latitude") is not None:
            continue  # already has real GPS
        ts = p.get("gps_timestamp")
        if not ts:
            continue
        try:
            t_cur = pd.Timestamp(ts).timestamp()
        except Exception:
            continue

        # Find nearest surrounding valid GPS points by list index
        prev = next((v for v in reversed(valid) if v[0] < i), None)
        nxt = next((v for v in valid if v[0] > i), None)
        if prev is None or nxt is None:
            continue
        if nxt[1] <= prev[1]:
            continue

        ratio = max(0.0, min(1.0, (t_cur - prev[1]) / (nxt[1] - prev[1])))
        p["estimated_latitude"] = round(prev[2] + ratio * (nxt[2] - prev[2]), 6)
        p["estimated_longitude"] = round(prev[3] + ratio * (nxt[3] - prev[3]), 6)


def detect_signal_loss(
    df: pd.DataFrame,
    late_delay_seconds: float = _LATE_DELAY_SECONDS,
    min_gps_span_seconds: float = _MIN_GPS_SPAN_SECONDS,
    min_late_records: int = _MIN_LATE_RECORDS,
    merge_gap_seconds: float = _MERGE_GAP_SECONDS,
) -> dict[str, Any]:
    """
    Detecta periodos sin señal comparando GPSTimeStamp vs ServerTimeStamp.

    Sin señal, el dispositivo guarda las posiciones y las manda al recuperarla: el servidor las
    recibe minutos después de tomarse. Un corte = serie seguida (en hora GPS) de registros que
    llegaron más de late_delay_seconds tarde, con >= min_late_records registros y que abarca
    >= min_gps_span_seconds de hora GPS.
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

    w = w.sort_values(["_gps_ms", "_srv_ms"]).reset_index(drop=True)
    w["_diff_s"] = (w["_srv_ms"] - w["_gps_ms"]) / 1000.0
    w["_is_late"] = w["_diff_s"] > late_delay_seconds

    runs = [
        (i, j) for i, j in _late_runs(w["_gps_ms"].to_numpy(dtype=float), w["_is_late"].to_numpy(),
                                      merge_gap_seconds * 1000.0)
        if int(w["_is_late"].iloc[i: j + 1].sum()) >= min_late_records
        and (w["_gps_ms"].iloc[j] - w["_gps_ms"].iloc[i]) / 1000.0 >= min_gps_span_seconds
    ]
    w["_is_loss"] = False
    for i, j in runs:
        w.loc[i:j, "_is_loss"] = w.loc[i:j, "_is_late"]

    segments = _build_segments(w, runs, lat_col, lon_col)
    all_points = _build_all_points(w, lat_col, lon_col)
    _interpolate_missing_positions(all_points)

    return {
        "total_records": len(w),
        "signal_loss_segments_count": len(segments),
        "total_signal_loss_minutes": round(sum(s["duration_minutes"] for s in segments), 2),
        "segments": segments,
        "all_points": all_points,
        "error": None,
    }
