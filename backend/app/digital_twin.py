from __future__ import annotations

from typing import Any

import pandas as pd

from .eventtype_joiner import load_and_merge_pos_eventtype


def _infer_timestamp_unit(series: pd.Series) -> str:
    numeric_series = pd.to_numeric(series, errors="coerce").dropna()
    if numeric_series.empty:
        return "ms"

    median_value = numeric_series.abs().median()
    if median_value >= 1e17:
        return "ns"
    if median_value >= 1e14:
        return "us"
    if median_value >= 1e11:
        return "ms"
    return "s"


def _timestamp_to_iso(value: Any, unit: str) -> str | None:
    if pd.isna(value):
        return None

    parsed_timestamp = pd.to_datetime(pd.to_numeric([value], errors="coerce"), unit=unit, errors="coerce", utc=True)[0]
    if pd.isna(parsed_timestamp):
        return None
    return parsed_timestamp.isoformat()


def _prepare_sort_key(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    prepared["gpsTimestamp"] = pd.to_numeric(prepared.get("gpsTimestamp"), errors="coerce")
    prepared["serverTimestamp"] = pd.to_numeric(prepared.get("serverTimestamp"), errors="coerce")
    prepared["sort_timestamp"] = prepared["serverTimestamp"].fillna(prepared["gpsTimestamp"])
    return prepared


def _row_to_asset_state(row: pd.Series, gps_unit: str, server_unit: str) -> dict[str, Any]:
    return {
        "asset_id": int(row["assetId"]) if pd.notna(row.get("assetId")) else None,
        "client_id": int(row["clientId"]) if pd.notna(row.get("clientId")) else None,
        "latitude": float(row["latitude"]) if pd.notna(row.get("latitude")) else None,
        "longitude": float(row["longitude"]) if pd.notna(row.get("longitude")) else None,
        "event_type": int(row["eventType"]) if pd.notna(row.get("eventType")) else None,
        "event_key": row.get("Key"),
        "event_description": row.get("Description"),
        "gps_timestamp": _timestamp_to_iso(row.get("gpsTimestamp"), gps_unit),
        "server_timestamp": _timestamp_to_iso(row.get("serverTimestamp"), server_unit),
    }


def _row_to_recent_event(row: pd.Series, gps_unit: str, server_unit: str) -> dict[str, Any]:
    return {
        "event_type": int(row["eventType"]) if pd.notna(row.get("eventType")) else None,
        "event_key": row.get("Key"),
        "event_description": row.get("Description"),
        "gps_timestamp": _timestamp_to_iso(row.get("gpsTimestamp"), gps_unit),
        "server_timestamp": _timestamp_to_iso(row.get("serverTimestamp"), server_unit),
        "latitude": float(row["latitude"]) if pd.notna(row.get("latitude")) else None,
        "longitude": float(row["longitude"]) if pd.notna(row.get("longitude")) else None,
    }


def _filter_asset_rows(dataframe: pd.DataFrame, asset_id: int, client_id: int | None) -> pd.DataFrame:
    if "assetId" not in dataframe.columns:
        raise KeyError("Column 'assetId' not found in merged data")

    filtered = dataframe[pd.to_numeric(dataframe["assetId"], errors="coerce") == asset_id].copy()
    if client_id is not None and "clientId" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["clientId"], errors="coerce") == client_id].copy()

    if filtered.empty:
        raise ValueError(f"No data found for asset_id={asset_id} and client_id={client_id}")

    return filtered


def build_asset_states(dataframe: pd.DataFrame, sample_assets: int = 50) -> list[dict[str, Any]]:
    prepared = _prepare_sort_key(dataframe)
    if prepared.empty:
        return []

    gps_unit = _infer_timestamp_unit(prepared["gpsTimestamp"])
    server_unit = _infer_timestamp_unit(prepared["serverTimestamp"])

    latest_per_asset = (
        prepared.sort_values("sort_timestamp", ascending=False)
        .drop_duplicates(subset=["assetId"], keep="first")
        .head(sample_assets)
    )

    states: list[dict[str, Any]] = []
    for _, row in latest_per_asset.iterrows():
        states.append(_row_to_asset_state(row, gps_unit, server_unit))

    return states


def get_fleet_twin_summary(sample_assets: int = 50) -> dict[str, Any]:
    merged = load_and_merge_pos_eventtype()
    states = build_asset_states(merged, sample_assets=sample_assets)

    return {
        "total_records": len(merged),
        "total_assets": int(merged["assetId"].nunique()) if "assetId" in merged.columns else 0,
        "sample_assets": states,
    }


def get_asset_twin_state(asset_id: int, client_id: int | None = None, recent_events: int = 20) -> dict[str, Any]:
    merged = load_and_merge_pos_eventtype()
    filtered = _filter_asset_rows(merged, asset_id=asset_id, client_id=client_id)

    prepared = _prepare_sort_key(filtered)
    prepared = prepared.sort_values("sort_timestamp", ascending=False).reset_index(drop=True)

    gps_unit = _infer_timestamp_unit(prepared["gpsTimestamp"])
    server_unit = _infer_timestamp_unit(prepared["serverTimestamp"])

    latest_row = prepared.iloc[0]
    current_state = _row_to_asset_state(latest_row, gps_unit, server_unit)

    safe_recent_events = max(1, min(recent_events, 200))
    recent_rows = prepared.head(safe_recent_events)

    recent_events_data: list[dict[str, Any]] = []
    for _, row in recent_rows.iterrows():
        recent_events_data.append(_row_to_recent_event(row, gps_unit, server_unit))

    return {
        "current_state": current_state,
        "recent_events": recent_events_data,
    }
