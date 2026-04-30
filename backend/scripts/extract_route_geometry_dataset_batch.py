#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

import duckdb
from app.analytics_store import get_asset_client_rows, get_default_duckdb_path
from app.route_detection import detect_routes_by_asset_client


def get_top_asset_client_combos(
    db_path: Path,
    min_route_events: int,
    limit: int | None = None,
) -> list[tuple[int, int, int, int]]:
    connection = duckdb.connect(str(db_path))
    try:
        query = """
        SELECT try_cast(assetId AS BIGINT) AS assetId,
               try_cast(clientId AS BIGINT) AS clientId,
               SUM(CASE WHEN eventType = 519 THEN 1 ELSE 0 END) AS start_count,
               SUM(CASE WHEN eventType = 520 THEN 1 ELSE 0 END) AS end_count
        FROM merged_pos_eventtype
        GROUP BY assetId, clientId
        HAVING start_count >= ? AND end_count >= ?
        ORDER BY start_count DESC
        """
        if limit is not None:
            query += " LIMIT ?"
            rows = connection.execute(query, [min_route_events, min_route_events, limit]).fetchall()
        else:
            rows = connection.execute(query, [min_route_events, min_route_events]).fetchall()
    finally:
        connection.close()

    return [(int(row[0]), int(row[1]), int(row[2]), int(row[3])) for row in rows]


def build_route_sample(route: dict, asset_id: int, client_id: int, route_index: int) -> dict:
    components = route.get("components") or []
    labels = [str(c.get("matched_road_label") or "").strip() for c in components]
    road_sequence: list[str] = []
    for label in labels:
        if not label:
            continue
        if not road_sequence or road_sequence[-1] != label:
            road_sequence.append(label)
    change_indices = [
        idx
        for idx in range(1, len(components))
        if str(components[idx].get("matched_road_label") or "").strip()
        != str(components[idx - 1].get("matched_road_label") or "").strip()
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
    }


def write_jsonl(samples: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for sample in samples:
            handle.write(json.dumps(sample, ensure_ascii=False) + "\n")


def write_csv(samples: list[dict], output_path: Path, write_header: bool) -> None:
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
    with output_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
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
        description="Extract route geometry training data from many asset/client combos"
    )
    parser.add_argument("--db-path", type=str, default=str(get_default_duckdb_path()), help="DuckDB snapshot path")
    parser.add_argument("--output-jsonl", type=str, default=str(ROOT / "backend" / "data" / "route_geometry_training_batch.jsonl"), help="Output JSONL path")
    parser.add_argument("--output-csv", type=str, default=str(ROOT / "backend" / "data" / "route_geometry_training_batch.csv"), help="Output CSV path")
    parser.add_argument("--min-route-events", type=int, default=5, help="Minimum count of eventType 519/520 per combo")
    parser.add_argument("--top-combos", type=int, default=20, help="Number of asset/client combos to process")
    parser.add_argument("--asset-id", type=int, help="If provided, only process this assetId")
    parser.add_argument("--client-id", type=int, help="If provided, only process this clientId")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output files instead of appending")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    output_jsonl = Path(args.output_jsonl)
    output_csv = Path(args.output_csv)

    if args.overwrite:
        if output_jsonl.exists():
            output_jsonl.unlink()
        if output_csv.exists():
            output_csv.unlink()

    combos: list[tuple[int, int]] = []
    if args.asset_id is not None and args.client_id is not None:
        combos = [(args.asset_id, args.client_id)]
    else:
        top_combos = get_top_asset_client_combos(
            db_path=db_path,
            min_route_events=args.min_route_events,
            limit=args.top_combos,
        )
        combos = [(asset_id, client_id) for asset_id, client_id, _, _ in top_combos]

    total_samples = 0
    write_header = True
    for asset_id, client_id in combos:
        print(f"Processing assetId={asset_id} clientId={client_id}")
        merged = get_asset_client_rows(asset_id=asset_id, client_id=client_id, db_path=db_path)
        routes = detect_routes_by_asset_client(asset_id=asset_id, client_id=client_id, merged=merged)
        complete_routes = routes.get("complete_routes") or []
        if not complete_routes:
            print(f"  no complete routes found for {asset_id}/{client_id}")
            continue

        samples = [
            build_route_sample(route=route, asset_id=asset_id, client_id=client_id, route_index=index)
            for index, route in enumerate(complete_routes)
        ]

        write_jsonl(samples=samples, output_path=output_jsonl)
        write_csv(samples=samples, output_path=output_csv, write_header=write_header)
        write_header = False
        total_samples += len(samples)
        print(f"  wrote {len(samples)} samples")

    print(f"Done: wrote {total_samples} route samples")
    print(f"  jsonl -> {output_jsonl}")
    print(f"  csv -> {output_csv}")


if __name__ == "__main__":
    main()
