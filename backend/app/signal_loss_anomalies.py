from __future__ import annotations

import math
from typing import Any

import pandas as pd

from app.signal_loss_detection import detect_signal_loss

_MAX_FLEET_SPEED_KMH: float = 120.0
# Detenido = el punto con señal después del corte está a menos de esto del de antes. Antes se
# exigía además >= 10 min y la categoría se llamaba "apagado_en_reposo": un corte de 8 min con el
# camión parado caía en "sin_cobertura", y el nombre sugería un apagado intencional cuando en el
# asset 2728 eran estancias en una terminal (Celaya, 6–7 ene). Ahora se separa solo por distancia.
_MAX_STATIONARY_DISTANCE_KM: float = 0.2


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _classify(speed_kmh: float, distance_km: float, max_speed_kmh: float) -> str:
    if speed_kmh > max_speed_kmh:
        return "salto_sospechoso"
    if distance_km < _MAX_STATIONARY_DISTANCE_KM:
        return "sin_senal_detenido"
    return "sin_senal_en_movimiento"


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
      - sin_senal_en_movimiento → el camión avanzó durante el corte (zona sin red en el camino)
      - sin_senal_detenido      → el camión no se movió (< _MAX_STATIONARY_DISTANCE_KM): estacionado
                                  con señal débil o el aparato dejó de transmitir
      - salto_sospechoso        → velocidad implícita entre punto anterior/posterior > max_speed_kmh
      - sin_datos               → falta el punto con señal antes o después del corte
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
        category = "sin_datos"

        if pt_before and pt_after:
            dist_km = _haversine_km(
                pt_before["latitude"], pt_before["longitude"],
                pt_after["latitude"], pt_after["longitude"],
            )
            duration_h = seg["duration_minutes"] / 60.0
            if duration_h > 0:
                speed_kmh = dist_km / duration_h
                category = _classify(speed_kmh, dist_km, max_speed_kmh)

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
