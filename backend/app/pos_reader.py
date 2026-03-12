from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

POS_COLUMNS_TO_DROP = {
    "AVRGFUELECONOMY",
    "INSTFUELECONOMY",
    "TOTALFUELHIRES",
    "TOTALFUEL",
}


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_pos_dir(project_root: Path) -> Path:
    env_pos_dir = os.getenv("DATA_POS_DIR", "").strip()
    if env_pos_dir:
        candidate = Path(env_pos_dir)
        return candidate if candidate.is_absolute() else project_root / candidate
    return project_root / "data" / "pos"


def _resolve_pos_glob_pattern() -> str:
    return os.getenv("DATA_POS_GLOB_PATTERN", "pos*.csv").strip() or "pos*.csv"


def get_pos_directory(project_root: Path | None = None) -> Path:
    root = project_root or get_project_root()
    return _resolve_pos_dir(root)


def list_pos_csv_files(project_root: Path | None = None) -> list[Path]:
    pos_dir = get_pos_directory(project_root)
    pos_glob_pattern = _resolve_pos_glob_pattern()

    if not pos_dir.exists() or not pos_dir.is_dir():
        raise FileNotFoundError(f"POS directory not found: {pos_dir}")

    files = sorted(pos_dir.glob(pos_glob_pattern))
    if not files:
        raise FileNotFoundError(
            f"No files matching '{pos_glob_pattern}' found in: {pos_dir}"
        )

    return files


def read_pos_csv_files(
    project_root: Path | None = None,
    add_source_file: bool = False,
) -> pd.DataFrame:
    files = list_pos_csv_files(project_root)
    frames: list[pd.DataFrame] = []

    for file_path in files:
        frame = pd.read_csv(file_path)
        frame = frame.drop(
            columns=[column for column in frame.columns if column.upper() in POS_COLUMNS_TO_DROP],
            errors="ignore",
        )
        if add_source_file:
            frame["source_file"] = file_path.name
        frames.append(frame)

    return pd.concat(frames, ignore_index=True)
