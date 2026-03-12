from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_eventtype_csv_path(project_root: Path | None = None) -> Path:
    root = project_root or get_project_root()
    env_eventtype_csv = os.getenv("DATA_EVENTTYPE_CSV", "").strip()
    if env_eventtype_csv:
        candidate = Path(env_eventtype_csv)
        csv_path = candidate if candidate.is_absolute() else root / candidate
    else:
        csv_path = root / "data" / "EventType.csv"

    if not csv_path.exists() or not csv_path.is_file():
        raise FileNotFoundError(f"EventType file not found: {csv_path}")

    return csv_path


def read_eventtype_csv(project_root: Path | None = None, strip_columns: bool = True) -> pd.DataFrame:
    csv_path = get_eventtype_csv_path(project_root)
    dataframe = pd.read_csv(csv_path)

    if strip_columns:
        dataframe.columns = [column.strip() for column in dataframe.columns]

    return dataframe
