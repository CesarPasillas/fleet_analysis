from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from app.analytics_store import (
    SnapshotUnavailableError,
    build_snapshot,
    get_asset_client_rows,
    query_carreteras_rows,
    get_snapshot_files_info,
    get_snapshot_status,
    query_merged_rows,
)
from app.digital_twin import get_asset_twin_state, get_fleet_twin_summary
from app.eventtype_joiner import load_and_merge_pos_eventtype
from app.eventtype_reader import get_eventtype_csv_path
from app.ml.model import RouteMLService
from app.pos_reader import get_pos_directory, list_pos_csv_files
from app.route_detection import detect_routes_by_asset_client
from app.schemas import (
    BackendFilesResponse,
    CarreterasRowsResponse,
    MergeEventTypeRowsResponse,
    MergeEventTypeSummaryResponse,
    PredictionResponse,
    RouteDetectionResponse,
    RouteFeatures,
    SnapshotBuildRequest,
    SnapshotBuildResponse,
    SnapshotStatusResponse,
    TwinAssetStateResponse,
    TwinFleetSummaryResponse,
    TrainRequest,
    TrainResponse,
)

app = FastAPI(title="Route Analysis ML API", version="0.1.0")
ml_service = RouteMLService()

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_TRAINING_DATA_PATH = BACKEND_DIR / "data" / "routes_training.csv"
DEFAULT_CORS_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]

load_dotenv(BACKEND_DIR / ".env")


def get_cors_origins() -> list[str]:
    cors_origins_raw = os.getenv("CORS_ORIGINS", "").strip()
    if not cors_origins_raw:
        return DEFAULT_CORS_ORIGINS

    origins = [origin.strip() for origin in cors_origins_raw.split(",") if origin.strip()]
    return origins or DEFAULT_CORS_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/ml/train",
    responses={
        400: {"description": "Invalid training request or dataset format"},
        404: {"description": "Training dataset file not found"},
        500: {"description": "Unexpected training error"},
    },
)
def train_model(payload: TrainRequest) -> TrainResponse:
    csv_path = payload.csv_path or os.getenv("DEFAULT_TRAINING_DATA") or str(DEFAULT_TRAINING_DATA_PATH)
    try:
        samples, accuracy = ml_service.train(csv_path=csv_path, epochs=payload.epochs)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Training failed: {exc}") from exc

    return TrainResponse(
        message="Model trained successfully",
        samples=samples,
        accuracy=accuracy,
    )


@app.post(
    "/ml/predict",
    responses={
        404: {"description": "Model artifacts not found"},
        500: {"description": "Unexpected prediction error"},
    },
)
def predict_route(payload: RouteFeatures) -> PredictionResponse:
    try:
        route_id, confidence = ml_service.predict(payload)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {exc}") from exc

    return PredictionResponse(predicted_route=route_id, confidence=confidence)


@app.post(
    "/data/merge-eventtype/summary",
    responses={
        404: {"description": "POS or EventType file/directory not found"},
        500: {"description": "Unexpected merge error"},
    },
)
def merge_eventtype_summary(sample_rows: int = 10) -> MergeEventTypeSummaryResponse:
    try:
        merged = load_and_merge_pos_eventtype()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Merge failed: {exc}") from exc

    safe_sample_rows = max(1, min(sample_rows, 100))
    sample = merged.head(safe_sample_rows).to_dict(orient="records")

    null_event_type_rows = int(merged["eventType"].isna().sum()) if "eventType" in merged.columns else 0
    null_key_rows = int(merged["Key"].isna().sum()) if "Key" in merged.columns else 0
    distinct_event_types = int(merged["eventType"].nunique(dropna=True)) if "eventType" in merged.columns else 0

    return MergeEventTypeSummaryResponse(
        total_rows=len(merged),
        total_columns=len(merged.columns),
        null_event_type_rows=null_event_type_rows,
        null_key_rows=null_key_rows,
        distinct_event_types=distinct_event_types,
        columns=merged.columns.tolist(),
        sample=sample,
    )


@app.get(
    "/data/merge-eventtype/rows",
    responses={
        404: {"description": "POS or EventType file/directory not found"},
        500: {"description": "Unexpected merge error"},
    },
)
def merge_eventtype_rows(
    limit: int = 100,
    offset: int = 0,
    asset_id: int | None = None,
    client_id: int | None = None,
    event_type: int | None = None,
) -> MergeEventTypeRowsResponse:
    safe_limit = max(1, min(limit, 1000))
    safe_offset = max(0, offset)

    try:
        snapshot_result = query_merged_rows(
            limit=safe_limit,
            offset=safe_offset,
            asset_id=asset_id,
            client_id=client_id,
            event_type=event_type,
        )

        return MergeEventTypeRowsResponse(
            total_rows=int(snapshot_result["total_rows"]),
            offset=safe_offset,
            limit=safe_limit,
            columns=list(snapshot_result["columns"]),
            rows=list(snapshot_result["rows"]),
        )
    except SnapshotUnavailableError:
        pass
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DuckDB query failed: {exc}") from exc

    try:
        merged = load_and_merge_pos_eventtype()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Merge failed: {exc}") from exc

    filtered = merged
    if asset_id is not None and "assetId" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["assetId"], errors="coerce") == asset_id]

    if client_id is not None and "clientId" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["clientId"], errors="coerce") == client_id]

    if event_type is not None and "eventType" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["eventType"], errors="coerce") == event_type]

    page = filtered.iloc[safe_offset : safe_offset + safe_limit]

    return MergeEventTypeRowsResponse(
        total_rows=len(filtered),
        offset=safe_offset,
        limit=safe_limit,
        columns=filtered.columns.tolist(),
        rows=page.to_dict(orient="records"),
    )


@app.get(
    "/data/carreteras/rows",
    responses={
        404: {"description": "Carreteras snapshot not found"},
        500: {"description": "Unexpected carreteras query error"},
    },
)
def carreteras_rows(
    limit: int = 1000,
    offset: int = 0,
    route_query: str | None = None,
) -> CarreterasRowsResponse:
    safe_limit = max(1, min(limit, 5000))
    safe_offset = max(0, offset)

    try:
        snapshot_result = query_carreteras_rows(
            limit=safe_limit,
            offset=safe_offset,
            route_query=route_query,
        )
    except SnapshotUnavailableError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Carreteras query failed: {exc}") from exc

    return CarreterasRowsResponse(
        total_rows=int(snapshot_result["total_rows"]),
        offset=safe_offset,
        limit=safe_limit,
        columns=list(snapshot_result["columns"]),
        rows=list(snapshot_result["rows"]),
    )


@app.get(
    "/data/files",
    responses={
        404: {"description": "POS or EventType file/directory not found"},
        500: {"description": "Unexpected file listing error"},
    },
)
def data_files() -> BackendFilesResponse:
    try:
        snapshot_files = get_snapshot_files_info()
        return BackendFilesResponse(**snapshot_files)
    except SnapshotUnavailableError:
        pass
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not query snapshot files: {exc}") from exc

    try:
        pos_files = list_pos_csv_files()
        pos_directory = str(get_pos_directory())
        eventtype_file = str(get_eventtype_csv_path())
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not list backend files: {exc}") from exc

    return BackendFilesResponse(
        pos_directory=pos_directory,
        eventtype_file=eventtype_file,
        total_pos_files=len(pos_files),
        pos_files=[file_path.name for file_path in pos_files],
    )


@app.post(
    "/data/snapshot/build",
    responses={
        404: {"description": "POS or EventType file/directory not found"},
        500: {"description": "Unexpected snapshot build error"},
    },
)
def data_snapshot_build(payload: SnapshotBuildRequest) -> SnapshotBuildResponse:
    try:
        summary = build_snapshot(
            db_path=payload.db_path,
            pos_glob=payload.pos_glob,
            eventtype_csv=payload.eventtype_csv,
            carreteras_glob=payload.carreteras_glob,
            pos_format=payload.pos_format,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Snapshot build failed: {exc}") from exc

    return SnapshotBuildResponse(**summary)


@app.get(
    "/data/snapshot/status",
    responses={
        500: {"description": "Unexpected snapshot status error"},
    },
)
def data_snapshot_status(db_path: str | None = None) -> SnapshotStatusResponse:
    try:
        status = get_snapshot_status(db_path=db_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Snapshot status failed: {exc}") from exc

    return SnapshotStatusResponse(**status)


@app.get(
    "/twin/fleet/summary",
    responses={
        404: {"description": "POS or EventType file/directory not found"},
        500: {"description": "Unexpected digital twin error"},
    },
)
def twin_fleet_summary(sample_assets: int = 25) -> TwinFleetSummaryResponse:
    try:
        summary = get_fleet_twin_summary(sample_assets=max(1, min(sample_assets, 200)))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Digital twin summary failed: {exc}") from exc

    return TwinFleetSummaryResponse(**summary)


@app.get(
    "/twin/assets/{asset_id}",
    responses={
        404: {"description": "Asset data not found"},
        500: {"description": "Unexpected digital twin error"},
    },
)
def twin_asset_state(asset_id: int, client_id: int | None = None, recent_events: int = 20) -> TwinAssetStateResponse:
    try:
        state = get_asset_twin_state(
            asset_id=asset_id,
            client_id=client_id,
            recent_events=max(1, min(recent_events, 200)),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Digital twin asset lookup failed: {exc}") from exc

    return TwinAssetStateResponse(**state)


@app.get(
    "/routes/detect",
    responses={
        404: {"description": "Asset/client data not found"},
        500: {"description": "Unexpected route detection error"},
    },
)
def detect_routes(asset_id: int, client_id: int) -> RouteDetectionResponse:
    try:
        merged_subset = get_asset_client_rows(asset_id=asset_id, client_id=client_id)
        result = detect_routes_by_asset_client(asset_id=asset_id, client_id=client_id, merged=merged_subset)
    except SnapshotUnavailableError:
        result = detect_routes_by_asset_client(asset_id=asset_id, client_id=client_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Route detection failed: {exc}") from exc

    return RouteDetectionResponse(**result)
