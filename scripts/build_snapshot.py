#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import importlib
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"

# Allow importing backend modules regardless of current working directory.
sys.path.insert(0, str(BACKEND_DIR))


def _load_analytics_store_api() -> tuple:
    module_name = "app" + ".analytics_store"
    module = importlib.import_module(module_name)
    return (
        module.build_snapshot,
        module.get_default_duckdb_path,
        module.get_default_eventtype_csv,
        module.get_default_pos_glob,
    )


def _resolve_files_from_glob(pattern: str) -> list[Path]:
    return sorted(Path(path) for path in glob.glob(pattern, recursive=True))


def _latest_mtime(paths: list[Path]) -> float:
    if not paths:
        return 0.0
    return max(path.stat().st_mtime for path in paths)


def _should_skip_snapshot_build(
    db_path: Path,
    source_files: list[Path],
    eventtype_csv: Path,
) -> bool:
    if not db_path.exists():
        return False

    if not source_files or not eventtype_csv.exists():
        return False

    sources_latest = max(_latest_mtime(source_files), eventtype_csv.stat().st_mtime)
    return db_path.stat().st_mtime >= sources_latest


def _convert_csv_to_parquet(
    csv_files: list[Path],
    parquet_dir: Path,
    force: bool,
) -> list[Path]:
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_files: list[Path] = []

    with duckdb.connect(":memory:") as connection:
        for csv_file in csv_files:
            parquet_file = parquet_dir / f"{csv_file.stem}.parquet"
            parquet_files.append(parquet_file)

            if not force and parquet_file.exists() and parquet_file.stat().st_mtime >= csv_file.stat().st_mtime:
                continue

            csv_sql_path = str(csv_file).replace("'", "''")
            parquet_sql_path = str(parquet_file).replace("'", "''")
            connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_csv_auto('{csv_sql_path}', union_by_name = true, filename = true)
                )
                TO '{parquet_sql_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )

    return parquet_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a local DuckDB snapshot from POS files (CSV/Parquet) and EventType catalog, "
            "with optional CSV to Parquet conversion."
        )
    )
    parser.add_argument("--pos-glob", default=None, help="Input POS glob pattern.")
    parser.add_argument("--eventtype-csv", default=None, help="EventType CSV path.")
    parser.add_argument("--db-path", default=None, help="Output DuckDB path.")
    parser.add_argument(
        "--source-format",
        choices=["auto", "csv", "parquet"],
        default="auto",
        help="POS source format before optional conversion.",
    )
    parser.add_argument(
        "--to-parquet",
        action="store_true",
        help="Convert CSV inputs into Parquet before building DuckDB snapshot.",
    )
    parser.add_argument(
        "--parquet-dir",
        default=str(BACKEND_DIR / "data" / "pos_parquet"),
        help="Directory where converted Parquet files will be written.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force conversion and snapshot rebuild even if outputs look fresh.",
    )
    parser.add_argument(
        "--skip-if-fresh",
        action="store_true",
        help="Skip snapshot build when DuckDB file is newer than input files.",
    )
    return parser.parse_args()


def _resolve_source_format(pos_glob: str, source_format: str) -> str:
    if source_format != "auto":
        return source_format

    lowered = pos_glob.lower()
    if lowered.endswith(".parquet") or lowered.endswith("*.parquet"):
        return "parquet"
    return "csv"


def _validate_inputs(source_files: list[Path], eventtype_csv: Path) -> int:
    if not source_files:
        return 1
    if not eventtype_csv.exists():
        return 2
    return 0


def _prepare_sources(
    args: argparse.Namespace,
    pos_glob: str,
    source_files: list[Path],
    source_format: str,
) -> tuple[str, str, list[Path]]:
    if not args.to_parquet:
        return pos_glob, source_format, source_files

    parquet_dir = Path(args.parquet_dir)
    parquet_files = _convert_csv_to_parquet(csv_files=source_files, parquet_dir=parquet_dir, force=args.force)
    if not parquet_files:
        raise ValueError("No Parquet files were generated")

    return str(parquet_dir / "*.parquet"), "parquet", parquet_files


def main() -> int:
    load_dotenv(BACKEND_DIR / ".env")
    args = parse_args()
    (
        build_snapshot,
        get_default_duckdb_path,
        get_default_eventtype_csv,
        get_default_pos_glob,
    ) = _load_analytics_store_api()

    pos_glob = args.pos_glob or get_default_pos_glob()
    eventtype_csv = Path(args.eventtype_csv or get_default_eventtype_csv())
    db_path = Path(args.db_path or get_default_duckdb_path())

    source_format = _resolve_source_format(pos_glob=pos_glob, source_format=args.source_format)

    source_files = _resolve_files_from_glob(pos_glob)
    validation_code = _validate_inputs(source_files=source_files, eventtype_csv=eventtype_csv)
    if validation_code == 1:
        print(f"No files found for pattern: {pos_glob}", file=sys.stderr)
        return 1
    if validation_code == 2:
        print(f"EventType file not found: {eventtype_csv}", file=sys.stderr)
        return 1

    if args.to_parquet and source_format != "csv":
        print("--to-parquet is only valid when source format is CSV", file=sys.stderr)
        return 1

    try:
        pos_glob, source_format, source_files = _prepare_sources(
            args=args,
            pos_glob=pos_glob,
            source_files=source_files,
            source_format=source_format,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.skip_if_fresh and not args.force:
        if _should_skip_snapshot_build(db_path=db_path, source_files=source_files, eventtype_csv=eventtype_csv):
            print(f"Snapshot is up to date: {db_path}")
            return 0

    summary = build_snapshot(
        db_path=str(db_path),
        pos_glob=pos_glob,
        eventtype_csv=str(eventtype_csv),
        pos_format=source_format,
    )

    print("Snapshot built successfully")
    print(f"db_path: {summary['db_path']}")
    print(f"pos_rows: {summary['pos_rows']}")
    print(f"eventtype_rows: {summary['eventtype_rows']}")
    print(f"merged_rows: {summary['merged_rows']}")
    print(f"distinct_assets: {summary['distinct_assets']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
