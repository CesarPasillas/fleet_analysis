from __future__ import annotations

import math
import os
import heapq
from typing import Any

import numpy as np
import pandas as pd

from .analytics_store import SnapshotUnavailableError, get_carreteras_geometries, get_default_duckdb_path
from .eventtype_joiner import load_and_merge_pos_eventtype

EVENT_START = 519
EVENT_END = 520
EARTH_RADIUS_M = 6371000.0

_ROAD_INDEX_CACHE_KEY: tuple[str, float] | None = None
_ROAD_INDEX_CACHE: list[dict[str, Any]] = []
_ROUTE_GRAPH_CACHE_KEY: tuple[str, float] | None = None
_ROUTE_GRAPH_NODES: list[tuple[float, float]] = []
_ROUTE_GRAPH_ADJ: dict[int, list[tuple[int, float, str]]] = {}
_LOCAL_ROUTE_GRAPH_CACHE_KEY: tuple[Any, ...] | None = None
_LOCAL_ROUTE_GRAPH_NODES: list[tuple[float, float]] = []
_LOCAL_ROUTE_GRAPH_ADJ: dict[int, list[tuple[int, float, str]]] = {}


def _read_graph_vertex_stride() -> int:
    try:
        stride = int(os.getenv("ROUTE_GRAPH_VERTEX_STRIDE", "6"))
    except ValueError:
        stride = 3
    return max(1, stride)


def _sample_polyline_for_graph(polyline: list[tuple[float, float]], stride: int) -> list[tuple[float, float]]:
    if len(polyline) < 2 or stride <= 1:
        return polyline

    sampled = polyline[::stride]
    if sampled[-1] != polyline[-1]:
        sampled.append(polyline[-1])

    if len(sampled) < 2:
        return polyline

    return sampled


def _to_readable_datetime(series: pd.Series) -> pd.Series:
    numeric_series = pd.to_numeric(series, errors="coerce")
    sample = numeric_series.dropna()

    if sample.empty:
        return pd.to_datetime(numeric_series, errors="coerce", utc=True)

    median_value = sample.abs().median()
    if median_value >= 1e17:
        unit = "ns"
    elif median_value >= 1e14:
        unit = "us"
    elif median_value >= 1e11:
        unit = "ms"
    else:
        unit = "s"

    unit_candidates = [unit, "ms", "s", "us", "ns"]
    seen_units: set[str] = set()

    for candidate in unit_candidates:
        if candidate in seen_units:
            continue
        seen_units.add(candidate)
        try:
            return pd.to_datetime(numeric_series, unit=candidate, errors="coerce", utc=True)
        except (OverflowError, FloatingPointError, ValueError):
            continue

    return pd.to_datetime(series, errors="coerce", utc=True)


def _haversine_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    radius_km = 6371.0
    radians = np.pi / 180.0
    delta_lat = (lat2 - lat1) * radians
    delta_lon = (lon2 - lon1) * radians
    value = np.sin(delta_lat / 2.0) ** 2 + np.cos(lat1 * radians) * np.cos(lat2 * radians) * np.sin(delta_lon / 2.0) ** 2
    return radius_km * (2 * np.arcsin(np.sqrt(value)))


def _total_distance_km(segment: pd.DataFrame) -> float:
    if not {"latitude", "longitude"}.issubset(segment.columns):
        return 0.0

    gps_points = segment.dropna(subset=["latitude", "longitude"]).copy()
    if len(gps_points) < 2:
        return 0.0

    lat1 = gps_points["latitude"].to_numpy()[:-1]
    lon1 = gps_points["longitude"].to_numpy()[:-1]
    lat2 = gps_points["latitude"].to_numpy()[1:]
    lon2 = gps_points["longitude"].to_numpy()[1:]

    return float(np.nansum(_haversine_km(lat1, lon1, lat2, lon2)))


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    distance_km = _haversine_km(
        np.array([lat1], dtype=float),
        np.array([lon1], dtype=float),
        np.array([lat2], dtype=float),
        np.array([lon2], dtype=float),
    )[0]
    return float(distance_km * 1000.0)


def _to_iso(timestamp: Any) -> str | None:
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp).isoformat()


def _parse_linestring_wkt(geometry_wkt: Any) -> list[tuple[float, float]]:
    if geometry_wkt is None:
        return []

    text = str(geometry_wkt).strip()
    if not text:
        return []

    upper_text = text.upper()
    if not upper_text.startswith("LINESTRING"):
        return []

    first_parenthesis = text.find("(")
    last_parenthesis = text.rfind(")")
    if first_parenthesis == -1 or last_parenthesis == -1 or last_parenthesis <= first_parenthesis:
        return []

    content = text[first_parenthesis + 1 : last_parenthesis]
    points: list[tuple[float, float]] = []

    for raw_point in content.split(","):
        coords = raw_point.strip().split()
        if len(coords) < 2:
            continue

        try:
            lon_value = float(coords[0])
            lat_value = float(coords[1])
        except ValueError:
            continue

        if not math.isfinite(lat_value) or not math.isfinite(lon_value):
            continue

        points.append((lat_value, lon_value))

    return points


def _build_polyline_segment_arrays(polyline: list[tuple[float, float]]) -> dict[str, np.ndarray]:
    polyline_array = np.asarray(polyline, dtype=float)
    if polyline_array.ndim != 2 or polyline_array.shape[0] < 2 or polyline_array.shape[1] < 2:
        empty = np.array([], dtype=float)
        return {
            "start_lats": empty,
            "start_lons": empty,
            "end_lats": empty,
            "end_lons": empty,
        }

    return {
        "start_lats": polyline_array[:-1, 0],
        "start_lons": polyline_array[:-1, 1],
        "end_lats": polyline_array[1:, 0],
        "end_lons": polyline_array[1:, 1],
    }


def _get_snapshot_cache_key() -> tuple[str, float] | None:
    snapshot_path = get_default_duckdb_path()
    if not snapshot_path.exists():
        return None

    return (str(snapshot_path), float(snapshot_path.stat().st_mtime))


def _latlon_to_local_xy_m(lat: float, lon: float, origin_lat: float, origin_lon: float) -> tuple[float, float]:
    radians = math.pi / 180.0
    x_value = (lon - origin_lon) * radians * EARTH_RADIUS_M * math.cos(origin_lat * radians)
    y_value = (lat - origin_lat) * radians * EARTH_RADIUS_M
    return x_value, y_value


def _point_segment_distance_m(
    px: float,
    py: float,
    ax: float,
    ay: float,
    bx: float,
    by: float,
) -> float:
    delta_x = bx - ax
    delta_y = by - ay
    segment_length_sq = delta_x * delta_x + delta_y * delta_y

    if segment_length_sq == 0:
        return math.hypot(px - ax, py - ay)

    projection = ((px - ax) * delta_x + (py - ay) * delta_y) / segment_length_sq
    projection = max(0.0, min(1.0, projection))

    closest_x = ax + projection * delta_x
    closest_y = ay + projection * delta_y
    return math.hypot(px - closest_x, py - closest_y)


def _distance_to_polyline_m(latitude: float, longitude: float, polyline: list[tuple[float, float]]) -> float:
    if len(polyline) < 2:
        return float("inf")

    min_distance = float("inf")
    for idx in range(len(polyline) - 1):
        start_lat, start_lon = polyline[idx]
        end_lat, end_lon = polyline[idx + 1]

        start_x, start_y = _latlon_to_local_xy_m(start_lat, start_lon, latitude, longitude)
        end_x, end_y = _latlon_to_local_xy_m(end_lat, end_lon, latitude, longitude)
        distance = _point_segment_distance_m(0.0, 0.0, start_x, start_y, end_x, end_y)

        if distance < min_distance:
            min_distance = distance

    return min_distance


def _distance_to_road_polyline_m(latitude: float, longitude: float, road: dict[str, Any]) -> float:
    segment_arrays = road.get("segment_arrays")
    if not isinstance(segment_arrays, dict):
        return _distance_to_polyline_m(latitude, longitude, road.get("polyline") or [])

    start_lats = segment_arrays.get("start_lats")
    start_lons = segment_arrays.get("start_lons")
    end_lats = segment_arrays.get("end_lats")
    end_lons = segment_arrays.get("end_lons")
    if not all(isinstance(value, np.ndarray) for value in [start_lats, start_lons, end_lats, end_lons]):
        return _distance_to_polyline_m(latitude, longitude, road.get("polyline") or [])
    if start_lats.size == 0:
        return float("inf")

    radians = math.pi / 180.0
    cos_origin = math.cos(latitude * radians)

    start_x = (start_lons - longitude) * radians * EARTH_RADIUS_M * cos_origin
    start_y = (start_lats - latitude) * radians * EARTH_RADIUS_M
    end_x = (end_lons - longitude) * radians * EARTH_RADIUS_M * cos_origin
    end_y = (end_lats - latitude) * radians * EARTH_RADIUS_M

    delta_x = end_x - start_x
    delta_y = end_y - start_y
    segment_length_sq = (delta_x * delta_x) + (delta_y * delta_y)

    numerator = ((-start_x) * delta_x) + ((-start_y) * delta_y)
    projection = np.divide(
        numerator,
        segment_length_sq,
        out=np.zeros_like(numerator, dtype=float),
        where=segment_length_sq != 0,
    )
    projection = np.clip(projection, 0.0, 1.0)

    closest_x = start_x + (projection * delta_x)
    closest_y = start_y + (projection * delta_y)
    distances = np.hypot(closest_x, closest_y)

    if distances.size == 0:
        return float("inf")
    return float(np.nanmin(distances))


def _load_road_index() -> list[dict[str, Any]]:
    global _ROAD_INDEX_CACHE_KEY
    global _ROAD_INDEX_CACHE

    current_cache_key = _get_snapshot_cache_key()
    if _ROAD_INDEX_CACHE_KEY == current_cache_key and _ROAD_INDEX_CACHE:
        return _ROAD_INDEX_CACHE

    try:
        carreteras = get_carreteras_geometries()
    except SnapshotUnavailableError:
        _ROAD_INDEX_CACHE_KEY = current_cache_key
        _ROAD_INDEX_CACHE = []
        return []

    road_index: list[dict[str, Any]] = []
    for _, row in carreteras.iterrows():
        polyline = _parse_linestring_wkt(row.get("geometry"))
        if len(polyline) < 2:
            continue

        segment_arrays = _build_polyline_segment_arrays(polyline)
        latitudes = [point[0] for point in polyline]
        longitudes = [point[1] for point in polyline]
        road_name = str(row.get("name") or "").strip()
        route_name = str(row.get("p_route") or "").strip()
        road_no = str(row.get("p_no") or "").strip()
        road_label = road_name or route_name or road_no or "carretera_sin_nombre"

        road_index.append(
            {
                "road_label": road_label,
                "road_name": road_name,
                "route_name": route_name,
                "road_no": road_no,
                "min_lat": min(latitudes),
                "max_lat": max(latitudes),
                "min_lon": min(longitudes),
                "max_lon": max(longitudes),
                "polyline": polyline,
                "segment_arrays": segment_arrays,
            }
        )

    _ROAD_INDEX_CACHE_KEY = current_cache_key
    _ROAD_INDEX_CACHE = road_index
    return road_index


def _build_route_graph(road_index: list[dict[str, Any]]) -> tuple[list[tuple[float, float]], dict[int, list[tuple[int, float, str]]]]:
    node_ids: dict[tuple[float, float], int] = {}
    nodes: list[tuple[float, float]] = []
    adjacency: dict[int, list[tuple[int, float, str]]] = {}
    vertex_stride = _read_graph_vertex_stride()

    def _node_id(lat: float, lon: float) -> int:
        key = (round(float(lat), 6), round(float(lon), 6))
        existing = node_ids.get(key)
        if existing is not None:
            return existing

        new_id = len(nodes)
        node_ids[key] = new_id
        nodes.append((float(lat), float(lon)))
        adjacency[new_id] = []
        return new_id

    for road in road_index:
        polyline = road.get("polyline") or []
        polyline = _sample_polyline_for_graph(polyline, vertex_stride)
        if len(polyline) < 2:
            continue

        road_label = str(road.get("road_label") or "").strip()
        for point_index in range(len(polyline) - 1):
            start_lat, start_lon = polyline[point_index]
            end_lat, end_lon = polyline[point_index + 1]

            node_start = _node_id(start_lat, start_lon)
            node_end = _node_id(end_lat, end_lon)

            edge_distance_m = _haversine_m(start_lat, start_lon, end_lat, end_lon)
            if edge_distance_m <= 0:
                continue

            adjacency[node_start].append((node_end, edge_distance_m, road_label))
            adjacency[node_end].append((node_start, edge_distance_m, road_label))

    _connect_nearby_graph_nodes(nodes, adjacency)

    return nodes, adjacency


def _graph_connector_config() -> tuple[float, float, int, int]:
    try:
        connect_distance_m = float(os.getenv("ROUTE_GRAPH_CONNECT_DISTANCE_M", "120"))
    except ValueError:
        connect_distance_m = 120.0

    try:
        connect_penalty_m = float(os.getenv("ROUTE_GRAPH_CONNECT_PENALTY_M", "30"))
    except ValueError:
        connect_penalty_m = 30.0

    try:
        max_connectors_per_node = int(os.getenv("ROUTE_GRAPH_MAX_CONNECTORS_PER_NODE", "12"))
    except ValueError:
        max_connectors_per_node = 12

    try:
        max_nodes_for_connectors = int(os.getenv("ROUTE_GRAPH_CONNECTORS_MAX_NODES", "20000"))
    except ValueError:
        max_nodes_for_connectors = 45000

    return (
        max(20.0, connect_distance_m),
        max(0.0, connect_penalty_m),
        max(1, max_connectors_per_node),
        max(1000, max_nodes_for_connectors),
    )


def _cell_key(lat: float, lon: float, cell_size_deg: float) -> tuple[int, int]:
    return int(math.floor(lat / cell_size_deg)), int(math.floor(lon / cell_size_deg))


def _build_node_buckets(nodes: list[tuple[float, float]], cell_size_deg: float) -> dict[tuple[int, int], list[int]]:
    buckets: dict[tuple[int, int], list[int]] = {}
    for node_index, (lat, lon) in enumerate(nodes):
        buckets.setdefault(_cell_key(lat, lon, cell_size_deg), []).append(node_index)
    return buckets


def _neighbor_cells(cell_x: int, cell_y: int) -> list[tuple[int, int]]:
    return [(x, y) for x in range(cell_x - 1, cell_x + 2) for y in range(cell_y - 1, cell_y + 2)]


def _collect_neighbor_candidates(
    node_index: int,
    lat: float,
    lon: float,
    cell_size_deg: float,
    buckets: dict[tuple[int, int], list[int]],
) -> list[int]:
    cell_x, cell_y = _cell_key(lat, lon, cell_size_deg)
    candidates: list[int] = []
    for cell in _neighbor_cells(cell_x, cell_y):
        for other_node in buckets.get(cell, []):
            if other_node > node_index:
                candidates.append(other_node)
    return candidates


def _can_add_connector(
    node_index: int,
    other_node: int,
    existing_neighbors: set[int],
    connector_counts: list[int],
    max_connectors_per_node: int,
) -> bool:
    if other_node in existing_neighbors:
        return False
    if connector_counts[node_index] >= max_connectors_per_node:
        return False
    if connector_counts[other_node] >= max_connectors_per_node:
        return False
    return True


def _add_connector_edge(
    adjacency: dict[int, list[tuple[int, float, str]]],
    connector_counts: list[int],
    node_index: int,
    other_node: int,
    edge_weight: float,
    connector_label: str,
) -> None:
    adjacency[node_index].append((other_node, edge_weight, connector_label))
    adjacency[other_node].append((node_index, edge_weight, connector_label))
    connector_counts[node_index] += 1
    connector_counts[other_node] += 1


def _connect_nearby_graph_nodes(
    nodes: list[tuple[float, float]],
    adjacency: dict[int, list[tuple[int, float, str]]],
) -> None:
    if len(nodes) < 2:
        return

    connect_distance_m, connect_penalty_m, max_connectors_per_node, max_nodes_for_connectors = _graph_connector_config()
    # Connector synthesis is expensive on very large graphs and can stall route
    # responses. Skip it above a configurable node threshold.
    if len(nodes) > max_nodes_for_connectors:
        return

    cell_size_deg = connect_distance_m / 111320.0
    if cell_size_deg <= 0:
        return

    buckets = _build_node_buckets(nodes, cell_size_deg)
    connector_counts = [0 for _ in range(len(nodes))]
    connector_label = "__connector__"

    for node_index, (lat, lon) in enumerate(nodes):
        existing_neighbors = {neighbor for neighbor, _, _ in adjacency.get(node_index, [])}
        candidates = _collect_neighbor_candidates(
            node_index=node_index,
            lat=lat,
            lon=lon,
            cell_size_deg=cell_size_deg,
            buckets=buckets,
        )

        for other_node in candidates:
            if not _can_add_connector(
                node_index=node_index,
                other_node=other_node,
                existing_neighbors=existing_neighbors,
                connector_counts=connector_counts,
                max_connectors_per_node=max_connectors_per_node,
            ):
                continue

            other_lat, other_lon = nodes[other_node]
            distance_m = _haversine_m(lat, lon, other_lat, other_lon)
            if distance_m > connect_distance_m:
                continue

            _add_connector_edge(
                adjacency=adjacency,
                connector_counts=connector_counts,
                node_index=node_index,
                other_node=other_node,
                edge_weight=distance_m + connect_penalty_m,
                connector_label=connector_label,
            )

            if connector_counts[node_index] >= max_connectors_per_node:
                break


def _load_route_graph() -> tuple[list[tuple[float, float]], dict[int, list[tuple[int, float, str]]]]:
    global _ROUTE_GRAPH_CACHE_KEY
    global _ROUTE_GRAPH_NODES
    global _ROUTE_GRAPH_ADJ

    current_cache_key = _get_snapshot_cache_key()
    if _ROUTE_GRAPH_CACHE_KEY == current_cache_key and _ROUTE_GRAPH_NODES:
        return _ROUTE_GRAPH_NODES, _ROUTE_GRAPH_ADJ

    road_index = _load_road_index()
    if not road_index:
        _ROUTE_GRAPH_CACHE_KEY = current_cache_key
        _ROUTE_GRAPH_NODES = []
        _ROUTE_GRAPH_ADJ = {}
        return _ROUTE_GRAPH_NODES, _ROUTE_GRAPH_ADJ

    nodes, adjacency = _build_route_graph(road_index)
    _ROUTE_GRAPH_CACHE_KEY = current_cache_key
    _ROUTE_GRAPH_NODES = nodes
    _ROUTE_GRAPH_ADJ = adjacency
    return _ROUTE_GRAPH_NODES, _ROUTE_GRAPH_ADJ


def _nearest_graph_node(
    latitude: float,
    longitude: float,
    nodes: list[tuple[float, float]],
    max_distance_m: float,
) -> int | None:
    if not nodes:
        return None

    best_node: int | None = None
    best_distance = float("inf")
    for node_index, (node_lat, node_lon) in enumerate(nodes):
        current_distance = _haversine_m(latitude, longitude, node_lat, node_lon)
        if current_distance < best_distance:
            best_distance = current_distance
            best_node = node_index

    if best_node is None or best_distance > max_distance_m:
        return None

    return best_node


def _shortest_path_on_graph(
    adjacency: dict[int, list[tuple[int, float, str]]],
    start_node: int,
    end_node: int,
) -> tuple[list[int], list[str]]:
    if start_node == end_node:
        return [start_node], []

    queue: list[tuple[float, int]] = [(0.0, start_node)]
    distances: dict[int, float] = {start_node: 0.0}
    previous: dict[int, tuple[int, str]] = {}

    while queue:
        current_distance, current_node = heapq.heappop(queue)
        if current_node == end_node:
            break

        if current_distance > distances.get(current_node, float("inf")):
            continue

        for next_node, edge_distance, edge_road_label in adjacency.get(current_node, []):
            candidate_distance = current_distance + edge_distance
            if candidate_distance >= distances.get(next_node, float("inf")):
                continue

            distances[next_node] = candidate_distance
            previous[next_node] = (current_node, edge_road_label)
            heapq.heappush(queue, (candidate_distance, next_node))

    if end_node not in distances:
        return [], []

    path_nodes_rev: list[int] = [end_node]
    path_labels_rev: list[str] = []
    cursor = end_node
    while cursor != start_node:
        parent, road_label = previous[cursor]
        path_labels_rev.append(road_label)
        path_nodes_rev.append(parent)
        cursor = parent

    path_nodes = list(reversed(path_nodes_rev))
    path_labels = list(reversed(path_labels_rev))
    return path_nodes, path_labels


def _road_bbox_distance_score_m(road: dict[str, Any], latitude: float, longitude: float) -> float:
    min_lat = float(road.get("min_lat") or latitude)
    max_lat = float(road.get("max_lat") or latitude)
    min_lon = float(road.get("min_lon") or longitude)
    max_lon = float(road.get("max_lon") or longitude)

    nearest_lat = min(max(latitude, min_lat), max_lat)
    nearest_lon = min(max(longitude, min_lon), max_lon)
    return _haversine_m(latitude, longitude, nearest_lat, nearest_lon)


def _select_local_roads_for_segment(segment: pd.DataFrame, road_index: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not road_index:
        return []

    gps_points = segment.dropna(subset=["latitude", "longitude"])
    if gps_points.empty:
        return road_index

    center_lat = float(gps_points["latitude"].mean())
    center_lon = float(gps_points["longitude"].mean())

    min_lat = float(gps_points["latitude"].min())
    max_lat = float(gps_points["latitude"].max())
    min_lon = float(gps_points["longitude"].min())
    max_lon = float(gps_points["longitude"].max())

    local_padding_m = _read_float_env("ROUTE_GRAPH_LOCAL_PADDING_M", 30000.0)
    local_max_roads = _read_int_env("ROUTE_GRAPH_LOCAL_MAX_ROADS", 220)
    local_padding_m = max(5000.0, local_padding_m)
    local_max_roads = max(50, local_max_roads)

    padding_lat_deg = local_padding_m / 111320.0
    cos_lat = max(0.1, math.cos(math.radians(center_lat)))
    padding_lon_deg = local_padding_m / (111320.0 * cos_lat)

    candidates = [
        road
        for road in road_index
        if float(road.get("max_lat") or -90.0) >= (min_lat - padding_lat_deg)
        and float(road.get("min_lat") or 90.0) <= (max_lat + padding_lat_deg)
        and float(road.get("max_lon") or -180.0) >= (min_lon - padding_lon_deg)
        and float(road.get("min_lon") or 180.0) <= (max_lon + padding_lon_deg)
    ]

    if not candidates:
        candidates = list(road_index)

    if len(candidates) > local_max_roads:
        candidates = sorted(
            candidates,
            key=lambda road: _road_bbox_distance_score_m(road, center_lat, center_lon),
        )[:local_max_roads]

    return candidates


def _segment_bbox_cache_key(gps_points: pd.DataFrame) -> tuple[float, float, float, float]:
    min_lat = round(float(gps_points["latitude"].min()), 3)
    max_lat = round(float(gps_points["latitude"].max()), 3)
    min_lon = round(float(gps_points["longitude"].min()), 3)
    max_lon = round(float(gps_points["longitude"].max()), 3)
    return (min_lat, max_lat, min_lon, max_lon)


def _load_local_route_graph_for_segment(
    segment: pd.DataFrame,
    road_index: list[dict[str, Any]],
) -> tuple[list[tuple[float, float]], dict[int, list[tuple[int, float, str]]]]:
    global _LOCAL_ROUTE_GRAPH_CACHE_KEY
    global _LOCAL_ROUTE_GRAPH_NODES
    global _LOCAL_ROUTE_GRAPH_ADJ

    gps_points = segment.dropna(subset=["latitude", "longitude"])
    if gps_points.empty:
        return [], {}

    local_padding_m = _read_float_env("ROUTE_GRAPH_LOCAL_PADDING_M", 30000.0)
    local_max_roads = _read_int_env("ROUTE_GRAPH_LOCAL_MAX_ROADS", 220)
    vertex_stride = _read_graph_vertex_stride()
    snapshot_cache_key = _get_snapshot_cache_key()
    bbox_key = _segment_bbox_cache_key(gps_points)

    current_cache_key: tuple[Any, ...] = (
        snapshot_cache_key,
        bbox_key,
        int(local_max_roads),
        int(vertex_stride),
        round(float(local_padding_m), 1),
    )

    if _LOCAL_ROUTE_GRAPH_CACHE_KEY == current_cache_key and _LOCAL_ROUTE_GRAPH_NODES:
        return _LOCAL_ROUTE_GRAPH_NODES, _LOCAL_ROUTE_GRAPH_ADJ

    local_road_index = _select_local_roads_for_segment(segment=segment, road_index=road_index)
    nodes, adjacency = _build_route_graph(local_road_index)

    _LOCAL_ROUTE_GRAPH_CACHE_KEY = current_cache_key
    _LOCAL_ROUTE_GRAPH_NODES = nodes
    _LOCAL_ROUTE_GRAPH_ADJ = adjacency
    return _LOCAL_ROUTE_GRAPH_NODES, _LOCAL_ROUTE_GRAPH_ADJ


def _dedupe_consecutive_nodes(node_ids: list[int]) -> list[int]:
    if not node_ids:
        return []

    deduped: list[int] = [node_ids[0]]
    for node_id in node_ids[1:]:
        if node_id != deduped[-1]:
            deduped.append(node_id)
    return deduped


def _build_anchor_nodes_for_segment(
    gps_points: pd.DataFrame,
    nodes: list[tuple[float, float]],
    max_snap_distance_m: float,
    max_anchors: int,
) -> list[int]:
    if gps_points.empty:
        return []

    total_points = len(gps_points)
    if total_points <= max_anchors:
        candidate_indexes = list(range(total_points))
    else:
        stride = max(1, math.ceil(total_points / max_anchors))
        candidate_indexes = list(range(0, total_points, stride))
        if candidate_indexes[-1] != total_points - 1:
            candidate_indexes.append(total_points - 1)

    # Guarantee endpoints are present so route starts/ends where the segment does.
    candidate_indexes = sorted({0, total_points - 1, *candidate_indexes})

    anchors: list[int] = []
    for row_index in candidate_indexes:
        row = gps_points.iloc[row_index]
        anchor_node = _nearest_graph_node(
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
            nodes=nodes,
            max_distance_m=max_snap_distance_m,
        )
        if anchor_node is not None:
            anchors.append(anchor_node)

    return _dedupe_consecutive_nodes(anchors)


def _stitch_route_between_anchors(
    adjacency: dict[int, list[tuple[int, float, str]]],
    anchor_nodes: list[int],
) -> tuple[list[int], list[str]]:
    if len(anchor_nodes) < 2:
        return [], []

    stitched_nodes: list[int] = [anchor_nodes[0]]
    stitched_labels: list[str] = []

    for index in range(len(anchor_nodes) - 1):
        start_node = anchor_nodes[index]
        end_node = anchor_nodes[index + 1]
        if start_node == end_node:
            continue

        path_nodes, path_labels = _shortest_path_on_graph(
            adjacency=adjacency,
            start_node=start_node,
            end_node=end_node,
        )
        if len(path_nodes) < 2:
            continue

        if stitched_nodes[-1] == path_nodes[0]:
            stitched_nodes.extend(path_nodes[1:])
        else:
            stitched_nodes.extend(path_nodes)
        stitched_labels.extend(path_labels)

    return _dedupe_consecutive_nodes(stitched_nodes), stitched_labels


def _predict_graph_route_for_segment(
    segment: pd.DataFrame,
    road_index: list[dict[str, Any]],
) -> tuple[list[list[float]], list[str]]:
    gps_points = segment.dropna(subset=["latitude", "longitude"])
    if len(gps_points) < 2:
        return [], []

    try:
        max_snap_distance_m = float(os.getenv("ROUTE_GRAPH_SNAP_MAX_DISTANCE_M", "2500"))
    except ValueError:
        max_snap_distance_m = 2500.0

    try:
        max_anchors = int(os.getenv("ROUTE_GRAPH_MAX_ANCHORS", "6"))
    except ValueError:
        max_anchors = 10

    max_snap_distance_m = max(300.0, max_snap_distance_m)
    max_anchors = max(3, max_anchors)

    start_point = gps_points.iloc[0]
    end_point = gps_points.iloc[-1]
    start_lat = float(start_point["latitude"])
    start_lon = float(start_point["longitude"])
    end_lat = float(end_point["latitude"])
    end_lon = float(end_point["longitude"])

    nodes, adjacency = _load_local_route_graph_for_segment(
        segment=segment,
        road_index=road_index,
    )
    if not nodes or not adjacency:
        return [], []

    start_node = _nearest_graph_node(start_lat, start_lon, nodes, max_snap_distance_m)
    end_node = _nearest_graph_node(end_lat, end_lon, nodes, max_snap_distance_m)
    path_node_ids: list[int] = []
    path_road_labels: list[str] = []

    # First attempt: direct shortest path from segment start to segment end.
    if start_node is not None and end_node is not None:
        path_node_ids, path_road_labels = _shortest_path_on_graph(adjacency, start_node, end_node)

    # Fallback: stitch smaller shortest paths across intermediate GPS anchors.
    if len(path_node_ids) < 2:
        anchor_nodes = _build_anchor_nodes_for_segment(
            gps_points=gps_points,
            nodes=nodes,
            max_snap_distance_m=max_snap_distance_m,
            max_anchors=max_anchors,
        )

        if start_node is not None:
            anchor_nodes.insert(0, start_node)
        if end_node is not None:
            anchor_nodes.append(end_node)
        anchor_nodes = _dedupe_consecutive_nodes(anchor_nodes)

        path_node_ids, path_road_labels = _stitch_route_between_anchors(
            adjacency=adjacency,
            anchor_nodes=anchor_nodes,
        )

    if len(path_node_ids) < 2:
        return [], []

    path_points = [[float(nodes[node_id][0]), float(nodes[node_id][1])] for node_id in path_node_ids]
    unique_path_roads = list(
        dict.fromkeys(
            [
                label.strip()
                for label in path_road_labels
                if str(label).strip() and str(label).strip() != "__connector__"
            ]
        )
    )
    return path_points, unique_path_roads


def _match_point_to_road(
    latitude: float,
    longitude: float,
    road_index: list[dict[str, Any]],
    max_distance_m: float,
) -> tuple[dict[str, Any] | None, float | None]:
    if not road_index:
        return None, None

    buffer_deg = max_distance_m / 111320.0 + 0.002
    best_road: dict[str, Any] | None = None
    best_distance = float("inf")

    for road in road_index:
        if latitude < road["min_lat"] - buffer_deg or latitude > road["max_lat"] + buffer_deg:
            continue
        if longitude < road["min_lon"] - buffer_deg or longitude > road["max_lon"] + buffer_deg:
            continue

        current_distance = _distance_to_road_polyline_m(latitude, longitude, road)
        if current_distance < best_distance:
            best_distance = current_distance
            best_road = road

    if best_road is None or best_distance > max_distance_m:
        return None, None

    return best_road, float(best_distance)


def _attach_road_matches(filtered: pd.DataFrame) -> pd.DataFrame:
    output = filtered.copy()
    output["matched_road_label"] = None
    output["matched_road_name"] = None
    output["matched_route_name"] = None
    output["matched_road_no"] = None
    output["matched_road_distance_m"] = np.nan

    road_index = _load_road_index()
    if not road_index:
        return output

    try:
        max_distance_m = float(os.getenv("ROUTE_ROAD_MAX_DISTANCE_M", "300"))
    except ValueError:
        max_distance_m = 300.0

    try:
        fallback_max_distance_m = float(os.getenv("ROUTE_ROAD_FALLBACK_MAX_DISTANCE_M", "1200"))
    except ValueError:
        fallback_max_distance_m = 1200.0

    if max_distance_m <= 0:
        max_distance_m = 300.0

    if fallback_max_distance_m < max_distance_m:
        fallback_max_distance_m = max_distance_m

    for row_index, row in output.iterrows():
        latitude = pd.to_numeric(row.get("latitude"), errors="coerce")
        longitude = pd.to_numeric(row.get("longitude"), errors="coerce")
        if pd.isna(latitude) or pd.isna(longitude):
            continue

        road, distance_m = _match_point_to_road(
            latitude=float(latitude),
            longitude=float(longitude),
            road_index=road_index,
            max_distance_m=max_distance_m,
        )

        # If strict matching fails, try a wider radius so the route can still be
        # associated with the nearest known road in sparse/noisy GPS scenarios.
        if road is None and fallback_max_distance_m > max_distance_m:
            road, distance_m = _match_point_to_road(
                latitude=float(latitude),
                longitude=float(longitude),
                road_index=road_index,
                max_distance_m=fallback_max_distance_m,
            )

        if road is None:
            continue

        output.at[row_index, "matched_road_label"] = road["road_label"]
        output.at[row_index, "matched_road_name"] = road["road_name"]
        output.at[row_index, "matched_route_name"] = road["route_name"]
        output.at[row_index, "matched_road_no"] = road["road_no"]
        output.at[row_index, "matched_road_distance_m"] = distance_m

    return output


def _read_float_env(var_name: str, default_value: float) -> float:
    try:
        return float(os.getenv(var_name, str(default_value)))
    except ValueError:
        return default_value


def _read_int_env(var_name: str, default_value: int) -> int:
    try:
        return int(os.getenv(var_name, str(default_value)))
    except ValueError:
        return default_value


def _sample_interpolated_road_labels(
    gps_points: pd.DataFrame,
    road_index: list[dict[str, Any]],
    infer_step_m: float,
    infer_max_distance_m: float,
) -> list[str]:
    sampled_labels: list[str] = []

    lat_values = gps_points["latitude"].to_numpy(dtype=float)
    lon_values = gps_points["longitude"].to_numpy(dtype=float)

    for index in range(len(gps_points) - 1):
        lat_a = float(lat_values[index])
        lon_a = float(lon_values[index])
        lat_b = float(lat_values[index + 1])
        lon_b = float(lon_values[index + 1])

        segment_distance_m = _haversine_m(lat_a, lon_a, lat_b, lon_b)
        steps = int(min(30, max(1, math.ceil(segment_distance_m / infer_step_m))))

        for step in range(steps + 1):
            ratio = step / steps
            lat_sample = lat_a + (lat_b - lat_a) * ratio
            lon_sample = lon_a + (lon_b - lon_a) * ratio
            road, _distance = _match_point_to_road(
                latitude=lat_sample,
                longitude=lon_sample,
                road_index=road_index,
                max_distance_m=infer_max_distance_m,
            )
            if road is None:
                continue
            sampled_labels.append(str(road.get("road_label") or "").strip())

    return sampled_labels


def _count_labels(labels: list[str]) -> tuple[dict[str, int], list[str]]:
    counts: dict[str, int] = {}
    ordered_unique: list[str] = []
    for label in labels:
        counts[label] = counts.get(label, 0) + 1
        if label not in ordered_unique:
            ordered_unique.append(label)
    return counts, ordered_unique


def _filter_labels_by_threshold(
    ordered_unique: list[str],
    counts: dict[str, int],
    total_samples: int,
    min_hits: int,
    min_share: float,
) -> list[str]:
    selected: list[str] = []
    for label in ordered_unique:
        hits = counts.get(label, 0)
        share = (hits / total_samples) if total_samples > 0 else 0.0
        if hits >= min_hits and share >= min_share:
            selected.append(label)
    return selected


def _prepend_base_labels(base_labels: list[str] | None, labels: list[str]) -> list[str]:
    if not base_labels:
        return labels

    merged: list[str] = []
    for label in base_labels:
        if label and label not in merged:
            merged.append(label)
    for label in labels:
        if label not in merged:
            merged.append(label)
    return merged


def _ordered_labels_by_min_hits(
    labels: list[str],
    min_hits: int,
    min_share: float,
    max_roads: int,
    base_labels: list[str] | None = None,
) -> list[str]:
    cleaned_labels = [label for label in labels if label]
    if not cleaned_labels:
        return []

    counts, ordered_unique = _count_labels(cleaned_labels)

    total_samples = len(cleaned_labels)
    min_share = max(0.0, min(1.0, float(min_share)))

    filtered = _filter_labels_by_threshold(
        ordered_unique=ordered_unique,
        counts=counts,
        total_samples=total_samples,
        min_hits=min_hits,
        min_share=min_share,
    )

    if not filtered:
        filtered = ordered_unique

    filtered = _prepend_base_labels(base_labels, filtered)

    if max_roads > 0 and len(filtered) > max_roads:
        filtered = filtered[:max_roads]

    return filtered


def _infer_route_roads(
    segment: pd.DataFrame,
    road_index: list[dict[str, Any]],
    strict_labels: list[str],
) -> list[str]:
    if not road_index:
        return strict_labels

    infer_max_distance_m = _read_float_env("ROUTE_ROAD_INFER_MAX_DISTANCE_M", 700.0)
    infer_step_m = _read_float_env("ROUTE_ROAD_INFER_STEP_M", 60.0)
    infer_min_hits = _read_int_env("ROUTE_ROAD_INFER_MIN_HITS", 1)
    infer_min_share = _read_float_env("ROUTE_ROAD_INFER_MIN_SHARE", 0.0)
    # 0 means unlimited inferred roads (no truncation).
    infer_max_roads = _read_int_env("ROUTE_ROAD_INFER_MAX_ROADS", 0)

    infer_max_distance_m = max(100.0, infer_max_distance_m)
    infer_step_m = max(20.0, infer_step_m)
    infer_min_hits = max(1, infer_min_hits)
    infer_max_roads = max(0, infer_max_roads)

    gps_points = segment.dropna(subset=["latitude", "longitude"]).copy()
    if len(gps_points) < 2:
        return strict_labels

    sampled_labels = list(strict_labels)
    sampled_labels.extend(
        _sample_interpolated_road_labels(
            gps_points=gps_points,
            road_index=road_index,
            infer_step_m=infer_step_m,
            infer_max_distance_m=infer_max_distance_m,
        )
    )

    inferred = _ordered_labels_by_min_hits(
        labels=sampled_labels,
        min_hits=infer_min_hits,
        min_share=infer_min_share,
        max_roads=infer_max_roads,
        base_labels=strict_labels,
    )
    return inferred or strict_labels


def _prepare_asset_subset(asset_id: int, client_id: int, merged: pd.DataFrame | None = None) -> pd.DataFrame:
    merged_data = merged if merged is not None else load_and_merge_pos_eventtype()

    filtered = merged_data.copy()
    filtered = filtered[pd.to_numeric(filtered.get("assetId"), errors="coerce") == asset_id]
    filtered = filtered[pd.to_numeric(filtered.get("clientId"), errors="coerce") == client_id]

    if filtered.empty:
        raise ValueError(f"No hay registros para assetId={asset_id}, clientId={client_id}")

    if "eventType" not in filtered.columns:
        raise KeyError("No existe la columna eventType en los datos cargados")

    if "gpsTimestamp_readable" not in filtered.columns:
        if "gpsTimestamp" not in filtered.columns:
            raise KeyError("No existe gpsTimestamp ni gpsTimestamp_readable en los datos")
        filtered["gpsTimestamp_readable"] = _to_readable_datetime(filtered["gpsTimestamp"])

    if "serverTimestamp_readable" not in filtered.columns:
        if "serverTimestamp" in filtered.columns:
            filtered["serverTimestamp_readable"] = _to_readable_datetime(filtered["serverTimestamp"])
        else:
            filtered["serverTimestamp_readable"] = pd.NaT

    filtered["eventType"] = pd.to_numeric(filtered["eventType"], errors="coerce")
    filtered["orden_timestamp"] = filtered["gpsTimestamp_readable"].where(
        filtered["gpsTimestamp_readable"].notna(),
        filtered["serverTimestamp_readable"],
    )

    filtered = filtered.sort_values(
        by=["orden_timestamp", "gpsTimestamp_readable", "serverTimestamp_readable"],
        ascending=True,
        na_position="last",
    ).reset_index(drop=True)

    return filtered


def _append_incomplete(
    routes_incomplete: list[dict[str, Any]],
    issue_type: str,
    detail: str,
    start_index: int | None,
    end_index: int | None,
    start_timestamp: Any,
    end_timestamp: Any,
) -> None:
    routes_incomplete.append(
        {
            "type": issue_type,
            "start_index": start_index,
            "end_index": end_index,
            "start_timestamp": _to_iso(start_timestamp),
            "end_timestamp": _to_iso(end_timestamp),
            "detail": detail,
        }
    )


def _append_complete(
    routes_complete: list[dict[str, Any]],
    segment: pd.DataFrame,
    start_index: int,
    end_index: int,
    road_index: list[dict[str, Any]],
) -> None:
    start_timestamp = segment.iloc[0]["orden_timestamp"]
    end_timestamp = segment.iloc[-1]["orden_timestamp"]

    duration_hours = np.nan
    if pd.notna(start_timestamp) and pd.notna(end_timestamp):
        duration_hours = (end_timestamp - start_timestamp).total_seconds() / 3600.0

    matched_labels = [
        str(value).strip()
        for value in segment.get("matched_road_label", pd.Series(dtype=object)).dropna().tolist()
        if str(value).strip()
    ]
    unique_matched_labels = list(dict.fromkeys(matched_labels))
    graph_route_points: list[list[float]] = []
    graph_route_roads: list[str] = []
    inferred_roads = _infer_route_roads(
        segment=segment,
        road_index=road_index,
        strict_labels=(graph_route_roads or unique_matched_labels),
    )
    total_points_with_gps = int(segment[["latitude", "longitude"]].dropna().shape[0]) if {"latitude", "longitude"}.issubset(segment.columns) else 0
    road_match_ratio = (len(matched_labels) / total_points_with_gps) if total_points_with_gps > 0 else 0.0

    routes_complete.append(
        {
            "start_timestamp": _to_iso(start_timestamp),
            "end_timestamp": _to_iso(end_timestamp),
            "duration_hours": float(duration_hours) if pd.notna(duration_hours) else None,
            "distance_km": _total_distance_km(segment),
            "total_events": int(len(segment)),
            "unique_event_types": int(segment["eventType"].nunique(dropna=True)),
            "total_gps_points": int(segment[["latitude", "longitude"]].dropna().shape[0])
            if {"latitude", "longitude"}.issubset(segment.columns)
            else 0,
            "matched_roads": unique_matched_labels,
            "inferred_roads": inferred_roads,
            "predicted_route_points": graph_route_points,
            "predicted_route_roads": graph_route_roads,
            "road_match_ratio": float(road_match_ratio),
            "start_index": int(start_index),
            "end_index": int(end_index),
            "components": [
                {
                    "order_timestamp": _to_iso(row.get("orden_timestamp")),
                    "gps_timestamp": _to_iso(row.get("gpsTimestamp_readable")),
                    "server_timestamp": _to_iso(row.get("serverTimestamp_readable")),
                    "event_type": int(row["eventType"]) if pd.notna(row.get("eventType")) else None,
                    "event_key": row.get("Key"),
                    "event_description": row.get("Description"),
                    "latitude": float(row["latitude"]) if pd.notna(row.get("latitude")) else None,
                    "longitude": float(row["longitude"]) if pd.notna(row.get("longitude")) else None,
                    "matched_road_label": row.get("matched_road_label"),
                    "matched_road_distance_m": float(row["matched_road_distance_m"])
                    if pd.notna(row.get("matched_road_distance_m"))
                    else None,
                }
                for _, row in segment.iterrows()
            ],
        }
    )


def _scan_routes(filtered: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    routes_complete: list[dict[str, Any]] = []
    routes_incomplete: list[dict[str, Any]] = []
    start_index: int | None = None
    road_index = _load_road_index()

    for index, row in filtered.iterrows():
        event_type = row["eventType"]

        if event_type == EVENT_START:
            # Keep the first open start event. Additional 519 events inside the same
            # open segment are treated as intermediate records, not as a segment reset.
            if start_index is None:
                start_index = int(index)
            continue

        if event_type != EVENT_END:
            continue

        if start_index is None:
            _append_incomplete(
                routes_incomplete=routes_incomplete,
                issue_type="fin_sin_inicio",
                detail="Se encontró 520 sin un 519 abierto",
                start_index=None,
                end_index=int(index),
                start_timestamp=None,
                end_timestamp=row["orden_timestamp"],
            )
            continue

        segment = filtered.iloc[start_index : index + 1].copy()
        _append_complete(
            routes_complete=routes_complete,
            segment=segment,
            start_index=start_index,
            end_index=int(index),
            road_index=road_index,
        )
        start_index = None

    if start_index is not None:
        _append_incomplete(
            routes_incomplete=routes_incomplete,
            issue_type="inicio_sin_fin",
            detail="La serie terminó y no se encontró 520",
            start_index=int(start_index),
            end_index=None,
            start_timestamp=filtered.loc[start_index, "orden_timestamp"],
            end_timestamp=None,
        )

    return routes_complete, routes_incomplete


def _prioritize_complete_routes(routes_complete: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort routes so the first one is the best match against existing roads."""
    if not routes_complete:
        return []

    def _route_priority(route: dict[str, Any]) -> tuple[float, int, int, int, int, int]:
        match_ratio = float(route.get("road_match_ratio") or 0.0)
        matched_roads = route.get("matched_roads") or []
        matched_count = len(matched_roads) if isinstance(matched_roads, list) else 0
        total_gps_points = int(route.get("total_gps_points") or 0)
        total_events = int(route.get("total_events") or 0)
        distance_km = float(route.get("distance_km") or 0.0)

        # Higher is better for all these dimensions.
        return (
            match_ratio,
            matched_count,
            total_gps_points,
            total_events,
            int(distance_km * 1000),
            -int(route.get("start_index") or 0),
        )

    ordered = sorted(routes_complete, key=_route_priority, reverse=True)

    for index, route in enumerate(ordered):
        route["is_primary"] = index == 0

    return ordered


def _build_all_records(filtered: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for idx, row in filtered.iterrows():
        records.append(
            {
                "record_index": int(idx),
                "order_timestamp": _to_iso(row.get("orden_timestamp")),
                "gps_timestamp": _to_iso(row.get("gpsTimestamp_readable")),
                "server_timestamp": _to_iso(row.get("serverTimestamp_readable")),
                "event_type": int(row["eventType"]) if pd.notna(row.get("eventType")) else None,
                "olc": row.get("olc"),
                "event_key": row.get("Key"),
                "event_description": row.get("Description"),
                "latitude": float(row["latitude"]) if pd.notna(row.get("latitude")) else None,
                "longitude": float(row["longitude"]) if pd.notna(row.get("longitude")) else None,
                "matched_road_label": row.get("matched_road_label"),
                "matched_road_name": row.get("matched_road_name"),
                "matched_route_name": row.get("matched_route_name"),
                "matched_road_no": row.get("matched_road_no"),
                "matched_road_distance_m": float(row["matched_road_distance_m"])
                if pd.notna(row.get("matched_road_distance_m"))
                else None,
            }
        )
    return records


def _enrich_routes_with_graph_prediction(
    routes_complete: list[dict[str, Any]],
    filtered: pd.DataFrame,
    road_index: list[dict[str, Any]],
) -> None:
    if not routes_complete or filtered.empty or not road_index:
        return

    max_graph_routes = _read_int_env("ROUTE_GRAPH_MAX_COMPLETE_ROUTES", 2)
    max_graph_routes = max(0, max_graph_routes)
    if max_graph_routes == 0:
        return

    routes_to_enrich = routes_complete[:max_graph_routes]
    for route in routes_to_enrich:
        start_index = int(route.get("start_index") or -1)
        end_index = int(route.get("end_index") or -1)
        if start_index < 0 or end_index < start_index:
            continue

        segment = filtered.iloc[start_index : end_index + 1].copy()
        if segment.empty:
            continue

        graph_route_points, graph_route_roads = _predict_graph_route_for_segment(
            segment=segment,
            road_index=road_index,
        )

        route["predicted_route_points"] = graph_route_points
        route["predicted_route_roads"] = graph_route_roads

        matched_roads = route.get("matched_roads") or []
        inferred_roads = _infer_route_roads(
            segment=segment,
            road_index=road_index,
            strict_labels=(graph_route_roads or matched_roads),
        )
        route["inferred_roads"] = inferred_roads


def detect_routes_by_asset_client(
    asset_id: int,
    client_id: int,
    merged: pd.DataFrame | None = None,
) -> dict[str, Any]:
    filtered = _prepare_asset_subset(asset_id=asset_id, client_id=client_id, merged=merged)
    filtered_with_roads = _attach_road_matches(filtered)
    routes_complete, routes_incomplete = _scan_routes(filtered_with_roads)
    routes_complete = _prioritize_complete_routes(routes_complete)
    _enrich_routes_with_graph_prediction(
        routes_complete=routes_complete,
        filtered=filtered_with_roads,
        road_index=_load_road_index(),
    )
    all_records = _build_all_records(filtered_with_roads)

    return {
        "asset_id": asset_id,
        "client_id": client_id,
        "event_start": EVENT_START,
        "event_end": EVENT_END,
        "total_records_analyzed": len(filtered_with_roads),
        "complete_routes_count": len(routes_complete),
        "incomplete_routes_count": len(routes_incomplete),
        "complete_routes": routes_complete,
        "incomplete_routes": routes_incomplete,
        "all_records_count": len(all_records),
        "all_records": all_records,
    }
