from __future__ import annotations

import math
import os
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

    return pd.to_datetime(numeric_series, unit=unit, errors="coerce", utc=True)


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

        lon_value = pd.to_numeric(coords[0], errors="coerce")
        lat_value = pd.to_numeric(coords[1], errors="coerce")
        if pd.isna(lat_value) or pd.isna(lon_value):
            continue

        points.append((float(lat_value), float(lon_value)))

    return points


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
            }
        )

    _ROAD_INDEX_CACHE_KEY = current_cache_key
    _ROAD_INDEX_CACHE = road_index
    return road_index


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

        current_distance = _distance_to_polyline_m(latitude, longitude, road["polyline"])
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

    if max_distance_m <= 0:
        max_distance_m = 300.0

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

        if road is None:
            continue

        output.at[row_index, "matched_road_label"] = road["road_label"]
        output.at[row_index, "matched_road_name"] = road["road_name"]
        output.at[row_index, "matched_route_name"] = road["route_name"]
        output.at[row_index, "matched_road_no"] = road["road_no"]
        output.at[row_index, "matched_road_distance_m"] = distance_m

    return output


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


def detect_routes_by_asset_client(
    asset_id: int,
    client_id: int,
    merged: pd.DataFrame | None = None,
) -> dict[str, Any]:
    filtered = _prepare_asset_subset(asset_id=asset_id, client_id=client_id, merged=merged)
    filtered_with_roads = _attach_road_matches(filtered)
    routes_complete, routes_incomplete = _scan_routes(filtered_with_roads)
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