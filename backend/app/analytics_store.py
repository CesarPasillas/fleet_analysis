from __future__ import annotations

import os
from glob import glob
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

POSSIBLE_EVENTTYPE_COLUMNS = [
    "eventType",
    "Event_Type",
    "event_type",
    "eventtype",
    "id",
    "Id",
    "ID",
]

SUPPORTED_POS_FORMATS = {"auto", "csv", "parquet"}
DEFAULT_POS_GLOB_PATTERN = "pos*.csv"


class SnapshotUnavailableError(RuntimeError):
    pass


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_default_duckdb_path(project_root: Path | None = None) -> Path:
    root = project_root or get_project_root()
    return root / "backend" / "data" / "fleet_analytics.duckdb"


def get_default_pos_glob(project_root: Path | None = None) -> str:
    root = project_root or get_project_root()
    env_pos_glob = os.getenv("DATA_POS_GLOB", "").strip()
    if env_pos_glob:
        candidate = Path(env_pos_glob)
        return str(candidate if candidate.is_absolute() else root / candidate)

    env_pos_dir = os.getenv("DATA_POS_DIR", "").strip()
    if env_pos_dir:
        candidate_dir = Path(env_pos_dir)
        resolved_dir = candidate_dir if candidate_dir.is_absolute() else root / candidate_dir
        pattern = os.getenv("DATA_POS_GLOB_PATTERN", DEFAULT_POS_GLOB_PATTERN).strip() or DEFAULT_POS_GLOB_PATTERN
        return str(resolved_dir / pattern)

    return str(root / "data" / "pos" / DEFAULT_POS_GLOB_PATTERN)


def get_default_eventtype_csv(project_root: Path | None = None) -> str:
    root = project_root or get_project_root()
    env_eventtype_csv = os.getenv("DATA_EVENTTYPE_CSV", "").strip()
    if env_eventtype_csv:
        candidate = Path(env_eventtype_csv)
        return str(candidate if candidate.is_absolute() else root / candidate)

    return str(root / "data" / "EventType.csv")


def get_default_carreteras_glob(project_root: Path | None = None) -> str:
    root = project_root or get_project_root()
    env_carreteras_glob = os.getenv("DATA_CARRETERAS_GLOB", "").strip()
    if env_carreteras_glob:
        candidate = Path(env_carreteras_glob)
        return str(candidate if candidate.is_absolute() else root / candidate)

    return str(root / "data" / "carreteras*.csv")


def _detect_eventtype_column(columns: list[str]) -> str:
    for column in POSSIBLE_EVENTTYPE_COLUMNS:
        if column in columns:
            return column
    raise KeyError(
        "No event type key column found in EventType CSV. "
        f"Expected one of {POSSIBLE_EVENTTYPE_COLUMNS}, got: {columns}"
    )


def _read_table_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _resolve_pos_format(source_pos_glob: str, pos_format: str) -> str:
    normalized = (pos_format or "auto").strip().lower()
    if normalized not in SUPPORTED_POS_FORMATS:
        raise ValueError(
            f"Unsupported pos_format '{pos_format}'. Use one of: {sorted(SUPPORTED_POS_FORMATS)}"
        )

    if normalized != "auto":
        return normalized

    lowered = source_pos_glob.lower()
    if lowered.endswith(".parquet") or lowered.endswith("*.parquet") or lowered.endswith(".pq"):
        return "parquet"
    return "csv"


def _create_pos_raw_table(connection: duckdb.DuckDBPyConnection, source_pos_glob: str, resolved_format: str) -> None:
    if resolved_format == "parquet":
        connection.execute(
            """
            CREATE TABLE pos_raw AS
            SELECT *
            FROM read_parquet(?)
            """,
            [source_pos_glob],
        )
        return

    connection.execute(
        """
        CREATE TABLE pos_raw AS
        SELECT *
        FROM read_csv_auto(?, union_by_name = true, filename = true)
        """,
        [source_pos_glob],
    )


def build_snapshot(
    db_path: str | Path | None = None,
    pos_glob: str | None = None,
    eventtype_csv: str | None = None,
    carreteras_glob: str | None = None,
    pos_format: str = "auto",
) -> dict[str, int | str]:
    target_db_path = Path(db_path) if db_path else get_default_duckdb_path()
    target_db_path.parent.mkdir(parents=True, exist_ok=True)

    source_pos_glob = pos_glob or get_default_pos_glob()
    source_eventtype_csv = eventtype_csv or get_default_eventtype_csv()
    source_carreteras_glob = carreteras_glob or get_default_carreteras_glob()
    resolved_pos_format = _resolve_pos_format(source_pos_glob=source_pos_glob, pos_format=pos_format)

    if not Path(source_eventtype_csv).exists():
        raise FileNotFoundError(f"EventType CSV not found: {source_eventtype_csv}")

    pos_parent = Path(source_pos_glob).parent
    if not pos_parent.exists():
        raise FileNotFoundError(f"POS directory not found: {pos_parent}")

    with duckdb.connect(str(target_db_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS pos_raw")
        connection.execute("DROP TABLE IF EXISTS eventtype_catalog")
        connection.execute("DROP TABLE IF EXISTS merged_pos_eventtype")
        connection.execute("DROP TABLE IF EXISTS carreteras_raw")

        _create_pos_raw_table(
            connection=connection,
            source_pos_glob=source_pos_glob,
            resolved_format=resolved_pos_format,
        )

        eventtype_dataframe = pd.read_csv(source_eventtype_csv, skipinitialspace=True)
        eventtype_dataframe.columns = [str(column).strip() for column in eventtype_dataframe.columns]
        eventtype_key = _detect_eventtype_column(eventtype_dataframe.columns.tolist())

        if eventtype_key != "eventType":
            eventtype_dataframe = eventtype_dataframe.rename(columns={eventtype_key: "eventType"})

        eventtype_dataframe["eventType"] = pd.to_numeric(eventtype_dataframe["eventType"], errors="coerce")

        connection.register("eventtype_source", eventtype_dataframe)
        connection.execute(
            """
            CREATE TABLE eventtype_catalog AS
            SELECT
                try_cast(eventType AS BIGINT) AS eventType,
                * EXCLUDE (eventType)
            FROM eventtype_source
            """
        )

        connection.execute(
            """
            CREATE TABLE merged_pos_eventtype AS
            WITH pos_clean AS (
                SELECT
                    try_cast(eventType AS BIGINT) AS eventType,
                    * EXCLUDE (eventType)
                FROM pos_raw
            )
            SELECT p.*, c.*
            FROM pos_clean p
            LEFT JOIN eventtype_catalog c USING (eventType)
            """
        )

        carreteras_files = sorted(glob(source_carreteras_glob))
        carreteras_rows = 0
        if carreteras_files:
            connection.execute(
                """
                CREATE TABLE carreteras_raw AS
                SELECT *
                FROM read_csv_auto(?, union_by_name = true, filename = true)
                """,
                [source_carreteras_glob],
            )
            carreteras_rows = _read_table_count(connection, "carreteras_raw")

        pos_rows = _read_table_count(connection, "pos_raw")
        eventtype_rows = _read_table_count(connection, "eventtype_catalog")
        merged_rows = _read_table_count(connection, "merged_pos_eventtype")
        distinct_assets = int(
            connection.execute(
                "SELECT COUNT(DISTINCT try_cast(assetId AS BIGINT)) FROM merged_pos_eventtype"
            ).fetchone()[0]
            or 0
        )

    return {
        "db_path": str(target_db_path),
        "pos_rows": pos_rows,
        "eventtype_rows": eventtype_rows,
        "merged_rows": merged_rows,
        "distinct_assets": distinct_assets,
        "carreteras_rows": carreteras_rows,
    }


def get_snapshot_status(db_path: str | Path | None = None) -> dict[str, int | str | bool | None]:
    target_db_path = Path(db_path) if db_path else get_default_duckdb_path()
    if not target_db_path.exists():
        return {
            "db_path": str(target_db_path),
            "exists": False,
            "last_updated_iso": None,
            "pos_rows": None,
            "eventtype_rows": None,
            "merged_rows": None,
            "distinct_assets": None,
            "carreteras_rows": None,
        }

    last_updated_iso = datetime.fromtimestamp(target_db_path.stat().st_mtime, UTC).isoformat()

    with duckdb.connect(str(target_db_path), read_only=True) as connection:
        try:
            pos_rows = _read_table_count(connection, "pos_raw")
            eventtype_rows = _read_table_count(connection, "eventtype_catalog")
            merged_rows = _read_table_count(connection, "merged_pos_eventtype")
            carreteras_rows = _read_table_count(connection, "carreteras_raw")
            distinct_assets = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT try_cast(assetId AS BIGINT)) FROM merged_pos_eventtype"
                ).fetchone()[0]
                or 0
            )
        except duckdb.Error:
            return {
                "db_path": str(target_db_path),
                "exists": True,
                "last_updated_iso": last_updated_iso,
                "pos_rows": None,
                "eventtype_rows": None,
                "merged_rows": None,
                "distinct_assets": None,
                "carreteras_rows": None,
            }

    return {
        "db_path": str(target_db_path),
        "exists": True,
        "last_updated_iso": last_updated_iso,
        "pos_rows": pos_rows,
        "eventtype_rows": eventtype_rows,
        "merged_rows": merged_rows,
        "distinct_assets": distinct_assets,
        "carreteras_rows": carreteras_rows,
    }


def _open_snapshot_read_only(db_path: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    target_db_path = Path(db_path) if db_path else get_default_duckdb_path()
    if not target_db_path.exists():
        raise SnapshotUnavailableError(f"Snapshot database not found: {target_db_path}")

    connection = duckdb.connect(str(target_db_path), read_only=True)
    try:
        connection.execute("SELECT 1 FROM merged_pos_eventtype LIMIT 1")
    except duckdb.Error as exc:
        connection.close()
        raise SnapshotUnavailableError("Table 'merged_pos_eventtype' not found in snapshot") from exc

    return connection


def query_merged_rows(
    limit: int,
    offset: int,
    asset_id: int | None = None,
    client_id: int | None = None,
    event_type: int | None = None,
    db_path: str | Path | None = None,
) -> dict[str, object]:
    where_parts: list[str] = []
    where_params: list[object] = []

    if asset_id is not None:
        where_parts.append("try_cast(assetId AS BIGINT) = ?")
        where_params.append(asset_id)
    if client_id is not None:
        where_parts.append("try_cast(clientId AS BIGINT) = ?")
        where_params.append(client_id)
    if event_type is not None:
        where_parts.append("try_cast(eventType AS BIGINT) = ?")
        where_params.append(event_type)

    where_sql = ""
    if where_parts:
        where_sql = " WHERE " + " AND ".join(where_parts)

    with _open_snapshot_read_only(db_path=db_path) as connection:
        total_rows = int(
            connection.execute(
                f"SELECT COUNT(*) FROM merged_pos_eventtype{where_sql}",
                where_params,
            ).fetchone()[0]
            or 0
        )

        page_dataframe = connection.execute(
            f"SELECT * FROM merged_pos_eventtype{where_sql} LIMIT ? OFFSET ?",
            [*where_params, limit, offset],
        ).fetchdf()

    return {
        "total_rows": total_rows,
        "columns": page_dataframe.columns.tolist(),
        "rows": page_dataframe.to_dict(orient="records"),
    }


def get_asset_client_rows(
    asset_id: int,
    client_id: int,
    db_path: str | Path | None = None,
) -> pd.DataFrame:
    with _open_snapshot_read_only(db_path=db_path) as connection:
        dataframe = connection.execute(
            """
            SELECT *
            FROM merged_pos_eventtype
            WHERE try_cast(assetId AS BIGINT) = ?
              AND try_cast(clientId AS BIGINT) = ?
            """,
            [asset_id, client_id],
        ).fetchdf()

    if dataframe.empty:
        raise ValueError(f"No hay registros para assetId={asset_id}, clientId={client_id}")

    return dataframe


def get_carreteras_geometries(
    db_path: str | Path | None = None,
) -> pd.DataFrame:
    with _open_snapshot_read_only(db_path=db_path) as connection:
        try:
            dataframe = connection.execute(
                """
                SELECT
                    coalesce(name, '') AS name,
                    coalesce(p_route, '') AS p_route,
                    coalesce(p_no, '') AS p_no,
                    geometry
                FROM carreteras_raw
                WHERE geometry IS NOT NULL
                """
            ).fetchdf()
        except duckdb.Error as exc:
            raise SnapshotUnavailableError("Table 'carreteras_raw' not found in snapshot") from exc

    return dataframe


def query_carreteras_rows(
    limit: int,
    offset: int,
    route_query: str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, object]:
    where_parts: list[str] = []
    where_params: list[object] = []

    if route_query and route_query.strip():
        query_text = route_query.strip()
        where_parts.append("(lower(coalesce(name, '')) LIKE lower(?) OR lower(coalesce(p_route, '')) LIKE lower(?))")
        where_params.extend([f"%{query_text}%", f"%{query_text}%"])

    where_sql = ""
    if where_parts:
        where_sql = " WHERE " + " AND ".join(where_parts)

    with _open_snapshot_read_only(db_path=db_path) as connection:
        try:
            total_rows = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM carreteras_raw{where_sql}",
                    where_params,
                ).fetchone()[0]
                or 0
            )

            page_dataframe = connection.execute(
                f"SELECT * FROM carreteras_raw{where_sql} LIMIT ? OFFSET ?",
                [*where_params, limit, offset],
            ).fetchdf()
        except duckdb.Error as exc:
            raise SnapshotUnavailableError("Table 'carreteras_raw' not found in snapshot") from exc

    return {
        "total_rows": total_rows,
        "columns": page_dataframe.columns.tolist(),
        "rows": page_dataframe.to_dict(orient="records"),
    }


def get_snapshot_files_info(db_path: str | Path | None = None) -> dict[str, object]:
    with _open_snapshot_read_only(db_path=db_path) as connection:
        table_info = connection.execute("PRAGMA table_info('pos_raw')").fetchall()
        column_names = {str(row[1]) for row in table_info}

        if "filename" in column_names:
            file_rows = connection.execute(
                """
                SELECT DISTINCT filename
                FROM pos_raw
                WHERE filename IS NOT NULL
                ORDER BY filename
                """
            ).fetchall()
            pos_files = [str(row[0]) for row in file_rows if row and row[0]]
        else:
            pos_files = []

    if pos_files:
        first_parent = Path(pos_files[0]).parent
        same_parent = all(Path(path).parent == first_parent for path in pos_files)
        pos_directory = str(first_parent) if same_parent else ""
    else:
        pos_glob = get_default_pos_glob()
        pos_directory = str(Path(pos_glob).parent)

    eventtype_file = get_default_eventtype_csv()

    return {
        "pos_directory": pos_directory,
        "eventtype_file": eventtype_file,
        "total_pos_files": len(pos_files),
        "pos_files": [Path(path).name for path in pos_files],
    }
