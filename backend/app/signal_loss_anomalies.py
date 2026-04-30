from __future__ import annotations

import math
from typing import Any

import pandas as pd

from app.signal_loss_detection import detect_signal_loss

_MAX_FLEET_SPEED_KMH: float = 120.0
_MIN_STATIONARY_MINUTES: float = 10.0
_MAX_STATIONARY_SPEED_KMH: float = 5.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _classify(speed_kmh: float, duration_minutes: float, max_speed_kmh: float) -> str:
    if speed_kmh > max_speed_kmh:
        return "salto_sospechoso"
    if speed_kmh < _MAX_STATIONARY_SPEED_KMH and duration_minutes >= _MIN_STATIONARY_MINUTES:
        return "apagado_en_reposo"
    return "sin_cobertura"


def _extract_anchor_points(
    all_points: list[dict[str, Any]],
    gps_start: str,
    gps_end: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return the last valid point before the segment and first valid point after."""
    valid = [
        p for p in all_points
        if p.get("gps_timestamp")
        and p.get("latitude") is not None
        and p.get("longitude") is not None
        and not p.get("is_signal_loss", False)
    ]
    before = [p for p in valid if p["gps_timestamp"] < gps_start]
    after = [p for p in valid if p["gps_timestamp"] > gps_end]
    return (before[-1] if before else None, after[0] if after else None)


def classify_signal_loss_anomalies(
    df: pd.DataFrame,
    max_speed_kmh: float = _MAX_FLEET_SPEED_KMH,
) -> dict[str, Any]:
    """
    Extiende detect_signal_loss con clasificación de anomalías por segmento.

    Categorías:
      - sin_cobertura    → pérdida de señal normal (zona sin red)
      - salto_sospechoso → velocidad implícita entre punto anterior/posterior > max_speed_kmh
      - apagado_en_reposo→ gap largo con desplazamiento mínimo (dispositivo apagado adrede)
    """
    loss_result = detect_signal_loss(df)

    if loss_result.get("error") or not loss_result["segments"]:
        return {**loss_result, "anomalies": []}

    all_points: list[dict[str, Any]] = loss_result["all_points"]
    anomalies: list[dict[str, Any]] = []

    for idx, seg in enumerate(loss_result["segments"]):
        gps_start = seg.get("gps_start") or ""
        gps_end = seg.get("gps_end") or ""

        pt_before, pt_after = _extract_anchor_points(all_points, gps_start, gps_end)

        dist_km: float | None = None
        speed_kmh: float | None = None
        category = "sin_cobertura"

        if pt_before and pt_after:
            dist_km = _haversine_km(
                pt_before["latitude"], pt_before["longitude"],
                pt_after["latitude"], pt_after["longitude"],
            )
            duration_h = seg["duration_minutes"] / 60.0
            if duration_h > 0:
                speed_kmh = dist_km / duration_h
                category = _classify(speed_kmh, seg["duration_minutes"], max_speed_kmh)

        anomalies.append(
            {
                "segment_index": idx,
                "gps_start": gps_start,
                "gps_end": gps_end,
                "duration_minutes": seg["duration_minutes"],
                "num_records": seg["num_records"],
                "point_before": pt_before,
                "point_after": pt_after,
                "distance_km": round(dist_km, 2) if dist_km is not None else None,
                "implied_speed_kmh": round(speed_kmh, 1) if speed_kmh is not None else None,
                "category": category,
            }
        )

    return {**loss_result, "anomalies": anomalies}
