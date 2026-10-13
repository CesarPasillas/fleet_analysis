"""
Fase de aprendizaje de rutas históricas (Caso 3 — predicción de ruta vehicular).

El modelo MLP de app/ml/model.py clasifica features -> etiqueta de ruta directamente,
sin pasar por una fase explícita de agrupación de trayectorias ni construir una tabla
de frecuencias origen -> punto_intermedio -> destino como describe la metodología del
caso de estudio. Este módulo implementa esa fase faltante:

  1. Agrupa los recorridos históricos de un assetId+clientId por similitud de
     origen y destino (clustering geográfico, no por igualdad exacta de coordenadas).
  2. Para cada origen aprendido, identifica un "punto de control" (proxy de caseta):
     la carretera de `inferred_roads` que comparten los recorridos hacia >=2 destinos
     distintos desde ese origen — no hay dataset real de casetas, así que se usa la
     carretera compartida más frecuente como señal de "el vehículo va por esta familia
     de rutas".
  3. Dado un origen conocido y las carreteras ya recorridas en el viaje en curso,
     predice la distribución de destinos históricos (ver ejemplo Orizaba -> Caseta
     Esperanza -> {Puebla 48.9%, CDMX 34.8%, Veracruz 16.3%} del caso de estudio).
"""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any

import requests

_DEFAULT_ORIGIN_RADIUS_KM = 8.0
_DEFAULT_DEST_RADIUS_KM = 8.0
_MIN_CHECKPOINT_ROUTES = 2

# Para algunos assets, route_detection.py genera muchísimos "complete_routes" de
# duración/distancia casi nula (deriva GPS, paradas breves reclasificadas como viaje)
# junto a un número mucho menor de viajes reales. Verificado en datos reales: para un
# asset con 27,455 "recorridos", el 98.5% duraba <36s y recorría <0.4km, con un salto
# claro justo antes del percentil 99 hacia duraciones de horas — dos poblaciones
# distintas, no una cola continua. Sin este filtro, esos recorridos espurios dominan
# (por volumen) cualquier agrupación/estadística de rutas y la vuelven inútil.
_MIN_TRIP_DURATION_HOURS = 0.05   # 3 min
_MIN_TRIP_DISTANCE_KM = 1.0


def is_meaningful_trip(route: dict[str, Any]) -> bool:
    """True si el recorrido tiene duración/distancia suficiente para ser un viaje real
    (no ruido de detección). Usado por route_learning/route_stop_patterns/
    route_duration_patterns antes de agrupar u aprender patrones."""
    duration_hours = route.get("duration_hours")
    if duration_hours is not None and duration_hours < _MIN_TRIP_DURATION_HOURS:
        return False
    distance_km = route.get("distance_km")
    if distance_km is not None and distance_km < _MIN_TRIP_DISTANCE_KM:
        return False
    return True


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def extract_route_endpoints(route: dict[str, Any]) -> tuple[float, float, float, float] | None:
    """Origen y destino de un recorrido, desde `polyline` (dataset CSV) o
    `components` (salida en vivo de route_detection.detect_routes_by_asset_client)."""
    polyline = route.get("polyline")
    if isinstance(polyline, list) and len(polyline) >= 2:
        first, last = polyline[0], polyline[-1]
        if first and last and first[0] is not None and last[0] is not None:
            return float(first[0]), float(first[1]), float(last[0]), float(last[1])

    components = route.get("components") or []
    pts = [c for c in components if c.get("latitude") is not None and c.get("longitude") is not None]
    if len(pts) >= 2:
        return (
            float(pts[0]["latitude"]), float(pts[0]["longitude"]),
            float(pts[-1]["latitude"]), float(pts[-1]["longitude"]),
        )
    return None


def cluster_points(points: list[tuple[float, float]], radius_km: float) -> tuple[list[int], list[list[float]]]:
    """Clustering geográfico voraz: cada punto se une al cluster más cercano si cae
    dentro de radius_km de su centroide actual; si no, funda un cluster nuevo."""
    clusters: list[list[float]] = []  # [sum_lat, sum_lon, count]
    assignments: list[int] = []
    for lat, lon in points:
        best_id, best_dist = None, float("inf")
        for cid, (sum_lat, sum_lon, count) in enumerate(clusters):
            dist = _haversine_km(lat, lon, sum_lat / count, sum_lon / count)
            if dist < best_dist:
                best_dist, best_id = dist, cid
        if best_id is not None and best_dist <= radius_km:
            clusters[best_id][0] += lat
            clusters[best_id][1] += lon
            clusters[best_id][2] += 1
            assignments.append(best_id)
        else:
            clusters.append([lat, lon, 1])
            assignments.append(len(clusters) - 1)
    return assignments, clusters


_NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
_PLACE_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "geo_cache" / "places"
_PLACE_LABEL_RADIUS_KM = 2.0   # más fino que el radio de agrupación (8 km): bases a 7.5 km (pensión y
                                # centro de carga del asset 2728) no deben compartir etiqueta
_MAX_PLACE_LOOKUPS = 40   # consultas nuevas a Nominatim por llamada (1 por segundo, política de uso)
_place_cache: dict[tuple[float, float], str | None] = {}


def describe_place(lat: float, lon: float) -> str | None:
    """Nombre legible del lugar (colonia, ciudad, estado) por geocodificación inversa de OSM.
    Caché en memoria y en disco; None si no hay red o no hay resultado."""
    key = (round(lat, 3), round(lon, 3))
    if key in _place_cache:
        return _place_cache[key]
    path = _PLACE_CACHE_DIR / f"{key[0]}_{key[1]}.json"
    if path.exists():
        try:
            _place_cache[key] = json.loads(path.read_text()).get("name")
            return _place_cache[key]
        except Exception:
            pass
    try:
        resp = requests.get(
            _NOMINATIM_URL,
            params={"lat": lat, "lon": lon, "format": "json", "zoom": 14, "addressdetails": 1},
            headers={"User-Agent": "fleet-analysis-phd/1.0"},
            timeout=6,
        )
        resp.raise_for_status()
        addr = resp.json().get("address", {})
    except Exception:
        return None  # sin red: no se guarda, se reintenta en la siguiente llamada
    finally:
        time.sleep(1.0)
    local = addr.get("neighbourhood") or addr.get("suburb") or addr.get("quarter") or addr.get("hamlet")
    city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality") or addr.get("county")
    parts = [p for p in (local, city, addr.get("state")) if p]
    name = ", ".join(dict.fromkeys(parts)) or None
    _place_cache[key] = name
    try:
        _PLACE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"name": name}, ensure_ascii=False))
    except Exception:
        pass
    return name


def label_points(refs: list[dict[str, Any]], radius_km: float) -> None:
    """
    Escribe place_n (desde 1) y place_name en cada dict de refs (con lat/lon). Los centroides a
    <= radius_km comparten número y nombre, numerados en el orden en que aparecen en refs.
    """
    if not refs:
        return
    assign, clusters = cluster_points([(r["lat"], r["lon"]) for r in refs], radius_km)
    number: dict[int, int] = {}
    for cid in assign:
        number.setdefault(cid, len(number) + 1)
    names: dict[int, str | None] = {}
    lookups = 0
    for cid in number:
        s_lat, s_lon, n = clusters[cid]
        lat, lon = s_lat / n, s_lon / n
        cached = (round(lat, 3), round(lon, 3)) in _place_cache or (
            _PLACE_CACHE_DIR / f"{round(lat, 3)}_{round(lon, 3)}.json").exists()
        if cached or lookups < _MAX_PLACE_LOOKUPS:
            lookups += 0 if cached else 1
            names[cid] = describe_place(lat, lon)
    for ref, cid in zip(refs, assign):
        ref["place_n"] = number[cid]
        ref["place_name"] = names.get(cid) or f"{ref['lat']:.4f}, {ref['lon']:.4f}"


def label_places(origins: list[dict[str, Any]], radius_km: float) -> None:
    """
    Etiqueta legible para orígenes y destinos. Los cluster_id internos se numeran por
    separado para orígenes y destinos y empiezan en 0 ("Origen #1" y "Destino #1" eran lugares
    distintos). Aquí un mismo lugar tiene el mismo número sea origen o destino, numerado desde 1
    (orígenes con más viajes primero), con un nombre descriptivo.
    """
    refs: list[dict[str, Any]] = []
    for o in origins:
        refs.append(o)
        refs.extend(o["destinations"])
    label_points(refs, radius_km)


def label_route_patterns(patterns: list[dict[str, Any]]) -> None:
    """Igual que label_places para los patrones origen → destino de route_duration_patterns /
    route_stop_patterns: añade origin_place_n/_name y destination_place_n/_name."""
    refs: list[dict[str, Any]] = []
    for p in patterns:
        refs.append({"lat": p["origin_lat"], "lon": p["origin_lon"]})
        refs.append({"lat": p["destination_lat"], "lon": p["destination_lon"]})
    label_points(refs, _PLACE_LABEL_RADIUS_KM)
    for p, o, d in zip(patterns, refs[0::2], refs[1::2]):
        p["origin_place_n"], p["origin_place_name"] = o["place_n"], o["place_name"]
        p["destination_place_n"], p["destination_place_name"] = d["place_n"], d["place_name"]


def build_route_learning_model(
    routes: list[dict[str, Any]],
    origin_radius_km: float = _DEFAULT_ORIGIN_RADIUS_KM,
    dest_radius_km: float = _DEFAULT_DEST_RADIUS_KM,
    min_checkpoint_routes: int = _MIN_CHECKPOINT_ROUTES,
) -> dict[str, Any]:
    """Construye la tabla de rutas aprendidas: por cada origen frecuente, sus
    destinos históricos con frecuencia, y el punto de control (proxy de caseta)
    que los precede a todos."""
    valid: list[dict[str, Any]] = []
    for route in routes:
        if not is_meaningful_trip(route):
            continue
        endpoints = extract_route_endpoints(route)
        if endpoints is None:
            continue
        o_lat, o_lon, d_lat, d_lon = endpoints
        valid.append({
            "o_lat": o_lat, "o_lon": o_lon, "d_lat": d_lat, "d_lon": d_lon,
            "distance_km": route.get("distance_km"),
            "duration_hours": route.get("duration_hours"),
            "inferred_roads": list(route.get("inferred_roads") or []),
        })

    if not valid:
        return {
            "total_routes_input": len(routes), "total_routes_used": 0,
            "origin_radius_km": origin_radius_km, "dest_radius_km": dest_radius_km,
            "origins": [],
        }

    origin_assign, origin_clusters = cluster_points([(v["o_lat"], v["o_lon"]) for v in valid], origin_radius_km)
    dest_assign, _dest_clusters = cluster_points([(v["d_lat"], v["d_lon"]) for v in valid], dest_radius_km)
    for v, oid, did in zip(valid, origin_assign, dest_assign):
        v["origin_cluster"] = oid
        v["dest_cluster"] = did

    origins_out: list[dict[str, Any]] = []
    for oid, (sum_lat, sum_lon, count) in enumerate(origin_clusters):
        members = [v for v in valid if v["origin_cluster"] == oid]
        if not members:
            continue

        dest_groups: dict[int, list[dict[str, Any]]] = {}
        for m in members:
            dest_groups.setdefault(m["dest_cluster"], []).append(m)

        total_trips = len(members)
        destinations = []
        for did, dmembers in dest_groups.items():
            durations = [m["duration_hours"] for m in dmembers if m.get("duration_hours") is not None]
            distances = [m["distance_km"] for m in dmembers if m.get("distance_km") is not None]
            destinations.append({
                "destination_cluster_id": did,
                "lat": round(sum(m["d_lat"] for m in dmembers) / len(dmembers), 5),
                "lon": round(sum(m["d_lon"] for m in dmembers) / len(dmembers), 5),
                "trip_count": len(dmembers),
                "pct_of_origin_trips": round(len(dmembers) / total_trips * 100, 1),
                "avg_distance_km": round(sum(distances) / len(distances), 1) if distances else None,
                "avg_duration_hours": round(sum(durations) / len(durations), 2) if durations else None,
            })
        destinations.sort(key=lambda d: -d["trip_count"])

        # Punto de control: carretera compartida por recorridos hacia >=2 destinos
        # distintos desde este origen (o hacia el único destino si solo hay uno).
        road_route_counts: dict[str, int] = {}
        road_dest_clusters: dict[str, set[int]] = {}
        for m in members:
            for road in set(m["inferred_roads"]):
                road_route_counts[road] = road_route_counts.get(road, 0) + 1
                road_dest_clusters.setdefault(road, set()).add(m["dest_cluster"])

        min_distinct_dest = min(2, len(dest_groups))
        checkpoint_candidates = sorted(
            (
                {"road": road, "route_count": road_route_counts[road], "distinct_destinations": len(dests)}
                for road, dests in road_dest_clusters.items()
                if len(dests) >= min_distinct_dest and road_route_counts[road] >= min_checkpoint_routes
            ),
            key=lambda c: (-c["route_count"], -c["distinct_destinations"]),
        )

        origins_out.append({
            "origin_cluster_id": oid,
            "lat": round(sum_lat / count, 5),
            "lon": round(sum_lon / count, 5),
            "total_trips": total_trips,
            "checkpoint_road": checkpoint_candidates[0]["road"] if checkpoint_candidates else None,
            "checkpoint_candidates": checkpoint_candidates[:5],
            "destinations": destinations,
        })

    origins_out.sort(key=lambda o: -o["total_trips"])
    label_places(origins_out, _PLACE_LABEL_RADIUS_KM)

    return {
        "total_routes_input": len(routes),
        "total_routes_used": len(valid),
        "origin_radius_km": origin_radius_km,
        "dest_radius_km": dest_radius_km,
        "origins": origins_out,
    }


def predict_destination(
    model: dict[str, Any],
    origin_lat: float,
    origin_lon: float,
    passed_roads: list[str] | None = None,
    origin_match_radius_km: float = 15.0,
) -> dict[str, Any]:
    """Dado el origen del viaje en curso y las carreteras ya recorridas, predice
    la distribución de destinos históricos aprendida para ese origen."""
    origins = model.get("origins") or []
    if not origins:
        return {"matched_origin": False, "predictions": []}

    best_origin, best_dist = None, float("inf")
    for o in origins:
        dist = _haversine_km(origin_lat, origin_lon, o["lat"], o["lon"])
        if dist < best_dist:
            best_dist, best_origin = dist, o

    if best_origin is None or best_dist > origin_match_radius_km:
        return {"matched_origin": False, "predictions": []}

    passed_set = set(passed_roads or [])
    checkpoint_road = best_origin.get("checkpoint_road")

    return {
        "matched_origin": True,
        "origin_cluster_id": best_origin["origin_cluster_id"],
        "origin_distance_km": round(best_dist, 2),
        "total_historical_trips": best_origin["total_trips"],
        "checkpoint_road": checkpoint_road,
        "checkpoint_passed": bool(checkpoint_road) and checkpoint_road in passed_set,
        "predictions": best_origin["destinations"],
    }
