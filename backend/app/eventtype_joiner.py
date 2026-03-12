from __future__ import annotations

from pathlib import Path

import pandas as pd

from .eventtype_reader import read_eventtype_csv
from .pos_reader import read_pos_csv_files

POSSIBLE_EVENTTYPE_COLUMNS = [
    "eventType",
    "Event_Type",
    "event_type",
    "eventtype",
    "id",
    "Id",
    "ID",
]


def _detect_eventtype_column(dataframe: pd.DataFrame) -> str:
    for column in POSSIBLE_EVENTTYPE_COLUMNS:
        if column in dataframe.columns:
            return column
    raise KeyError(
        "No event type key column found. "
        f"Expected one of: {POSSIBLE_EVENTTYPE_COLUMNS}. "
        f"Available columns: {dataframe.columns.tolist()}"
    )


def normalize_eventtype_catalog(dataframe: pd.DataFrame) -> pd.DataFrame:
    catalog = dataframe.copy()
    key_column = _detect_eventtype_column(catalog)

    if key_column != "eventType":
        catalog = catalog.rename(columns={key_column: "eventType"})

    catalog["eventType"] = pd.to_numeric(catalog["eventType"], errors="coerce")
    catalog = catalog.drop_duplicates(subset=["eventType"])

    return catalog


def merge_pos_with_eventtype(
    pos_dataframe: pd.DataFrame,
    eventtype_dataframe: pd.DataFrame,
    how: str = "left",
) -> pd.DataFrame:
    if "eventType" not in pos_dataframe.columns:
        raise KeyError("Column 'eventType' not found in POS dataframe")

    pos = pos_dataframe.copy()
    pos["eventType"] = pd.to_numeric(pos["eventType"], errors="coerce")

    eventtype_catalog = normalize_eventtype_catalog(eventtype_dataframe)

    return pos.merge(
        eventtype_catalog,
        on="eventType",
        how=how,
        validate="many_to_one",
    )


def load_and_merge_pos_eventtype(project_root: Path | None = None) -> pd.DataFrame:
    pos_dataframe = read_pos_csv_files(project_root=project_root)
    eventtype_dataframe = read_eventtype_csv(project_root=project_root)

    return merge_pos_with_eventtype(pos_dataframe, eventtype_dataframe)
