#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from app.analytics_store import get_asset_client_rows, get_default_duckdb_path
from app.route_detection import detect_routes_by_asset_client


def _normalize_label(label: str | None) -> str:
    if label is None:
        return ""
    return str(label).strip()


def _build_road_sequence(components: list[dict]) -> list[str]:
    labels = [_normalize_label(c.get("matched_road_label")) for c in components]
    sequence: list[str] = []
    for label in labels:
        if not label:
            continue
        if not sequence or sequence[-1] != label:
            sequence.append(label)
    return sequence


def _build_route_sample(route: dict, asset_id: int, client_id: int, route_index: int) -> dict:
    components = route.get("components") or []
    road_sequence = _build_road_sequence(components)
    change_indices = [
        idx
        for idx in range(1, len(components))
        if _normalize_label(components[idx].get("matched_road_label"))
        != _normalize_label(components[idx - 1].get("matched_road_label"))
    ]
    polyline = [
        [comp.get("latitude"), comp.get("longitude")]
        for comp in components
        if comp.get("latitude") is not None and comp.get("longitude") is not None
    ]

    return {
        "asset_id": asset_id,
        "client_id": client_id,
        "route_index": route_index,
        "start_timestamp": route.get("start_timestamp"),
        "end_timestamp": route.get("end_timestamp"),
        "duration_hours": route.get("duration_hours"),
        "distance_km": route.get("distance_km"),
        "total_events": route.get("total_events"),
        "total_gps_points": route.get("total_gps_points"),
        "matched_roads": route.get("matched_roads"),
        "inferred_roads": route.get("inferred_roads"),
        "road_match_ratio": route.get("road_match_ratio"),
        "road_sequence": road_sequence,
        "road_change_indices": change_indices,
        "polyline": polyline,
        "components": components,
    }


def _write_jsonl(samples: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for sample in samples:
            handle.write(json.dumps(sample, ensure_ascii=False) + "\n")


def _write_csv(samples: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "asset_id",
        "client_id",
        "route_index",
        "start_timestamp",
        "end_timestamp",
        "duration_hours",
        "distance_km",
        "total_events",
        "total_gps_points",
        "matched_roads",
        "inferred_roads",
        "road_match_ratio",
        "road_sequence",
        "road_change_indices",
        "polyline",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for sample in samples:
            writer.writerow(
                {
                    "asset_id": sample["asset_id"],
                    "client_id": sample["client_id"],
                    "route_index": sample["route_index"],
                    "start_timestamp": sample["start_timestamp"],
                    "end_timestamp": sample["end_timestamp"],
                    "duration_hours": sample["duration_hours"],
                    "distance_km": sample["distance_km"],
                    "total_events": sample["total_events"],
                    "total_gps_points": sample["total_gps_points"],
                    "matched_roads": json.dumps(sample["matched_roads"], ensure_ascii=False),
                    "inferred_roads": json.dumps(sample["inferred_roads"], ensure_ascii=False),
                    "road_match_ratio": sample["road_match_ratio"],
                    "road_sequence": json.dumps(sample["road_sequence"], ensure_ascii=False),
                    "road_change_indices": json.dumps(sample["road_change_indices"]),
                    "polyline": json.dumps(sample["polyline"]),
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract route geometry training dataset from fleet_analytics.duckdb"
    )
    parser.add_argument("--asset-id", type=int, required=True, help="AssetId to extract routes for")
    parser.add_argument("--client-id", type=int, required=True, help="ClientId to extract routes for")
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(get_default_duckdb_path()),
        help="DuckDB snapshot path",
    )
    parser.add_argument(
        "--output-jsonl",
        type=str,
        default=str(ROOT / "backend" / "data" / "route_geometry_training.jsonl"),
        help="Output JSONL path",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=str(ROOT / "backend" / "data" / "route_geometry_training.csv"),
        help="Output CSV path",
    )

    args = parser.parse_args()
    db_path = Path(args.db_path)
    output_jsonl = Path(args.output_jsonl)
    output_csv = Path(args.output_csv)

    merged = get_asset_client_rows(asset_id=args.asset_id, client_id=args.client_id, db_path=db_path)
    routes = detect_routes_by_asset_client(asset_id=args.asset_id, client_id=args.client_id, merged=merged)
    complete_routes = routes.get("complete_routes") or []

    if not complete_routes:
        raise SystemExit(f"No complete routes found for asset_id={args.asset_id}, client_id={args.client_id}")

    samples = [
        _build_route_sample(route=route, asset_id=args.asset_id, client_id=args.client_id, route_index=index)
        for index, route in enumerate(complete_routes)
    ]

    _write_jsonl(samples=samples, output_path=output_jsonl)
    _write_csv(samples=samples, output_path=output_csv)

    print(f"Wrote {len(samples)} route samples")
    print(f"  JSONL: {output_jsonl}")
    print(f"  CSV: {output_csv}")


if __name__ == "__main__":
    main()
