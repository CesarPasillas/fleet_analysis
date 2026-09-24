"""
Caso 4 — Detección de paradas atípicas por vehículo + ruta.

Nota de alcance: la metodología del caso de estudio define la unidad de análisis
como assetId + driverId + ruta. El dataset disponible (merged_pos_eventtype y los
CSV fuente) no tiene ningún campo de conductor (driverId/conductor/operatorId) —
confirmado por inspección directa del esquema. Por eso este módulo reduce la
unidad de análisis a assetId + ruta (sin conductor); todo lo demás de la
metodología (patrón normal de paradas por ruta, detección de anomalías por
ubicación/tipo/cantidad/duración/secuencia, alerta operativa) se implementa igual.

Fases:
  1. Agrupa los recorridos históricos del asset en "rutas habituales" reutilizando
     el clustering de origen/destino de route_learning.py.
  2. Asigna cada parada clasificada (stop_detection.detect_and_classify_stops) al
     recorrido cuya ventana temporal la contiene.
  3. Para cada ruta habitual con suficientes viajes, aprende: número típico de
     paradas, perfil de ubicaciones/categorías/duraciones (reutiliza
     asset_stop_profile.py) y secuencia típica de categorías.
  4. Evalúa cada viaje histórico de esa ruta contra el patrón aprendido y genera
     una alerta cuando se aparta (mismo criterio de severidad simple).
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from app.asset_stop_profile import build_profile_from_stops, flag_atypical_stops
from app.route_learning import cluster_points, extract_route_endpoints, is_meaningful_trip

_DEFAULT_ORIGIN_RADIUS_KM = 8.0
_DEFAULT_DEST_RADIUS_KM = 8.0
_MIN_TRIPS_FOR_PATTERN = 3
_STOP_COUNT_Z = 2.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _parse_ts(ts: Any) -> pd.Timestamp | None:
    if not ts:
        return None
    try:
        return pd.Timestamp(ts)
    except Exception:
        return None


def assign_stops_to_routes(
    routes: list[dict[str, Any]],
    stops: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    """Une cada parada al recorrido cuya ventana [start_timestamp, end_timestamp] la contiene."""
    windows: list[tuple[int, pd.Timestamp, pd.Timestamp]] = []
    for i, r in enumerate(routes):
        t0, t1 = _parse_ts(r.get("start_timestamp")), _parse_ts(r.get("end_timestamp"))
        if t0 is not None and t1 is not None:
            windows.append((i, t0, t1))

    assignment: dict[int, list[dict[str, Any]]] = {i: [] for i, _, _ in windows}
    for s in stops:
        st = _parse_ts(s.get("start_timestamp"))
        if st is None:
            continue
        for i, t0, t1 in windows:
            if t0 <= st <= t1:
                assignment[i].append(s)
                break
    return assignment


def _typical_sequence(trip_stop_lists: list[list[dict[str, Any]]]) -> tuple[list[str], float]:
    """Secuencia de categorías más frecuente entre los viajes (voto mayoritario exacto)."""
    sequences: list[tuple[str, ...]] = []
    for lst in trip_stop_lists:
        if not lst:
            continue
        ordered = sorted(lst, key=lambda s: s.get("start_timestamp") or "")
        sequences.append(tuple(s["final_category"] for s in ordered))
    if not sequences:
        return [], 0.0
    counts: dict[tuple[str, ...], int] = {}
    for seq in sequences:
        counts[seq] = counts.get(seq, 0) + 1
    best_seq = max(counts, key=counts.get)
    return list(best_seq), round(counts[best_seq] / len(sequences) * 100, 1)


def build_route_stop_patterns(
    routes: list[dict[str, Any]],
    stops: list[dict[str, Any]],
    origin_radius_km: float = _DEFAULT_ORIGIN_RADIUS_KM,
    dest_radius_km: float = _DEFAULT_DEST_RADIUS_KM,
    min_trips_for_pattern: int = _MIN_TRIPS_FOR_PATTERN,
) -> dict[str, Any]:
    """Agrupa los recorridos por ruta habitual y aprende el patrón normal de paradas
    (cantidad, ubicación, categoría, duración, secuencia) para cada una."""
    valid_idx: list[int] = []
    endpoints: list[tuple[float, float, float, float]] = []
    for i, r in enumerate(routes):
        if not is_meaningful_trip(r):
            continue
        ep = extract_route_endpoints(r)
        if ep is not None:
            valid_idx.append(i)
            endpoints.append(ep)

    if not valid_idx:
        return {
            "total_routes_input": len(routes), "total_route_groups": 0,
            "origin_radius_km": origin_radius_km, "dest_radius_km": dest_radius_km,
            "route_patterns": [],
        }

    origin_assign, _ = cluster_points([(e[0], e[1]) for e in endpoints], origin_radius_km)
    dest_assign, _ = cluster_points([(e[2], e[3]) for e in endpoints], dest_radius_km)

    route_groups: dict[tuple[int, int], list[int]] = {}
    for local_i, route_i in enumerate(valid_idx):
        key = (origin_assign[local_i], dest_assign[local_i])
        route_groups.setdefault(key, []).append(route_i)

    stop_assignment = assign_stops_to_routes(routes, stops)

    patterns: list[dict[str, Any]] = []
    for (oid, did), route_indices in route_groups.items():
        if len(route_indices) < min_trips_for_pattern:
            continue

        trip_stop_lists = [stop_assignment.get(i, []) for i in route_indices]
        stop_counts = [len(lst) for lst in trip_stop_lists]
        counts_series = pd.Series(stop_counts, dtype="float64")
        all_stops_flat = [s for lst in trip_stop_lists for s in lst]

        typical_seq, typical_seq_pct = _typical_sequence(trip_stop_lists)

        patterns.append({
            "origin_cluster_id": oid,
            "destination_cluster_id": did,
            "total_trips": len(route_indices),
            "route_indices": route_indices,
            "avg_stop_count": round(float(counts_series.mean()), 2),
            "std_stop_count": round(float(counts_series.std()) if len(counts_series) > 1 else 0.0, 2),
            "min_stop_count": int(counts_series.min()),
            "max_stop_count": int(counts_series.max()),
            "typical_sequence": typical_seq,
            "typical_sequence_pct": typical_seq_pct,
            "stop_profile": build_profile_from_stops(all_stops_flat),
        })

    patterns.sort(key=lambda p: -p["total_trips"])
    return {
        "total_routes_input": len(routes),
        "total_route_groups": len(patterns),
        "origin_radius_km": origin_radius_km,
        "dest_radius_km": dest_radius_km,
        "route_patterns": patterns,
    }


def _severity(reason_count: int) -> str:
    if reason_count >= 2:
        return "alta"
    if reason_count == 1:
        return "media"
    return "baja"


def flag_atypical_trip(
    route_pattern: dict[str, Any],
    trip_stops: list[dict[str, Any]],
    stop_count_z: float = _STOP_COUNT_Z,
) -> dict[str, Any]:
    """Compara un viaje (paradas ya clasificadas) contra el patrón normal de su ruta
    habitual y devuelve las anomalías detectadas — ubicación, tipo, cantidad, duración
    y secuencia, tal como describe el caso de estudio de paradas atípicas."""
    reasons: list[str] = []

    n = len(trip_stops)
    mean_c, std_c = route_pattern["avg_stop_count"], route_pattern["std_stop_count"]
    if std_c > 0 and abs(n - mean_c) > stop_count_z * std_c:
        reasons.append("exceso_de_paradas" if n > mean_c else "muy_pocas_paradas")
    elif std_c == 0 and route_pattern["total_trips"] >= 3 and n != mean_c:
        reasons.append("exceso_de_paradas" if n > mean_c else "muy_pocas_paradas")

    flagged_stops = flag_atypical_stops(route_pattern["stop_profile"], trip_stops)
    any_atypical_stop = any(s["is_atypical"] for s in flagged_stops)

    ordered = sorted(trip_stops, key=lambda s: s.get("start_timestamp") or "")
    actual_seq = [s["final_category"] for s in ordered]
    typical_seq = route_pattern.get("typical_sequence") or []
    sequence_matches = actual_seq == typical_seq
    # Solo es señal de anomalía si la secuencia típica es realmente dominante en el
    # historial (>=50%) — si no, este asset/ruta no tiene un orden habitual establecido
    # y comparar contra "la más frecuente" solo generaría ruido.
    sequence_is_established = route_pattern.get("typical_sequence_pct", 0.0) >= 50.0
    if typical_seq and sequence_is_established and not sequence_matches:
        reasons.append("secuencia_no_habitual")

    if any_atypical_stop:
        reasons.append("parada_en_ubicacion_o_tipo_no_habitual")

    return {
        "trip_stop_count": n,
        "expected_stop_count_range": [
            max(0, round(mean_c - stop_count_z * std_c, 1)),
            round(mean_c + stop_count_z * std_c, 1),
        ],
        "sequence_actual": actual_seq,
        "sequence_typical": typical_seq,
        "sequence_matches": sequence_matches,
        "stops": flagged_stops,
        "is_atypical": bool(reasons),
        "reasons": reasons,
        "severity": _severity(len(reasons)),
    }
