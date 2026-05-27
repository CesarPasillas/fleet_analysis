from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from functools import partial
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
from app.signal_loss_detection import detect_signal_loss
from app.signal_loss_anomalies import classify_signal_loss_anomalies
from app.stop_detection import detect_and_classify_stops
from app.digital_twin import get_asset_twin_state, get_fleet_twin_summary
from app.eventtype_joiner import load_and_merge_pos_eventtype
from app.eventtype_reader import get_eventtype_csv_path
from app.ml.model import RouteMLService
from app.pos_reader import get_pos_directory, list_pos_csv_files
from app.route_detection import detect_routes_by_asset_client
from app.route_cache import get_cached_routes, store_cached_routes, clear_route_cache, coverage_cache_info
from app.route_summary import get_fleet_summary, get_client_summary, get_fleet_heatmap, get_client_assets, get_signal_loss_coverage
from app.schemas import (
    BackendFilesResponse,
    CarreterasRowsResponse,
    MergeEventTypeRowsResponse,
    MergeEventTypeSummaryResponse,
    PredictionResponse,
    RouteDetectionResponse,
    RouteFeatures,
    SignalLossAnomaliesResponse,
    SignalLossResponse,
    StopsResponse,
    SnapshotBuildRequest,
    SnapshotBuildResponse,
    SnapshotStatusResponse,
    TwinAssetStateResponse,
    TwinFleetSummaryResponse,
    TrainRequest,
    TrainResponse,
)

logger = logging.getLogger(__name__)

ml_service = RouteMLService()

# In-memory TTL cache for /routes/detect responses.
# Key: (asset_id, client_id)  Value: (timestamp_stored, result_dict)
_ROUTE_DETECT_CACHE: dict[tuple[int, int], tuple[float, dict]] = {}
_ROUTE_CACHE_TTL_SECONDS = int(os.getenv("ROUTE_CACHE_TTL_SECONDS", "300"))  # 5 min default

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_TRAINING_DATA_PATH = BACKEND_DIR / "data" / "route_geometry_training_batch.csv"
DEFAULT_CORS_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]

load_dotenv(BACKEND_DIR / ".env")


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Auto-train the ML model on startup if artifacts are missing."""
    if not ml_service.model_path.exists() or not ml_service.metadata_path.exists():
        if DEFAULT_TRAINING_DATA_PATH.exists():
            logger.info("ML artifacts missing — auto-training from %s", DEFAULT_TRAINING_DATA_PATH)
            try:
                samples, accuracy = ml_service.train(csv_path=str(DEFAULT_TRAINING_DATA_PATH))
                logger.info("Auto-training complete: %d samples, accuracy=%.4f", samples, accuracy)
            except Exception as exc:
                logger.warning("Auto-training failed: %s", exc)
        else:
            logger.warning(
                "ML artifacts missing and training data not found at %s. "
                "Call POST /ml/train to train the model.",
                DEFAULT_TRAINING_DATA_PATH,
            )
    yield


app = FastAPI(title="Route Analysis ML API", version="0.1.0", lifespan=lifespan)


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
    event_type_2: int | None = None,
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
            event_type_2=event_type_2,
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

    selected_event_types = sorted({value for value in [event_type, event_type_2] if value is not None})
    if selected_event_types and "eventType" in filtered.columns:
        numeric_event_type = pd.to_numeric(filtered["eventType"], errors="coerce")
        filtered = filtered[numeric_event_type.isin(selected_event_types)]

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

    # Invalidate all route caches — underlying data changed
    _ROUTE_DETECT_CACHE.clear()
    clear_route_cache()

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
def detect_routes(
    asset_id: int,
    client_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
) -> RouteDetectionResponse:
    cache_key = (asset_id, client_id, date_from, date_to)

    # 1. In-memory cache (fastest, 5-min TTL)
    cached = _ROUTE_DETECT_CACHE.get(cache_key)
    if cached is not None:
        stored_at, result = cached
        if time.monotonic() - stored_at < _ROUTE_CACHE_TTL_SECONDS:
            logger.info("Memory cache HIT asset_id=%d client_id=%d", asset_id, client_id)
            return RouteDetectionResponse(**result)
        del _ROUTE_DETECT_CACHE[cache_key]

    # 2. Persistent disk cache (survives restarts, invalidated on new snapshot)
    result = get_cached_routes(asset_id, client_id, date_from, date_to)
    if result is not None:
        logger.info("Disk cache HIT asset_id=%d client_id=%d", asset_id, client_id)
        _ROUTE_DETECT_CACHE[cache_key] = (time.monotonic(), result)
        return RouteDetectionResponse(**result)

    # 3. Full calculation
    try:
        merged_subset = get_asset_client_rows(asset_id=asset_id, client_id=client_id, date_from=date_from, date_to=date_to)
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

    _ROUTE_DETECT_CACHE[cache_key] = (time.monotonic(), result)
    store_cached_routes(asset_id, client_id, date_from, date_to, result)
    return RouteDetectionResponse(**result)


@app.get(
    "/routes/stops",
    responses={
        404: {"description": "No se encontraron registros para este asset/client"},
        500: {"description": "Error en detección de paradas"},
    },
)
async def stops(
    asset_id: int,
    client_id: int,
    min_stop_minutes: float = 5.0,
    use_geo: bool = False,
    date_from: str | None = None,
    date_to: str | None = None,
) -> StopsResponse:
    try:
        merged_subset = get_asset_client_rows(asset_id=asset_id, client_id=client_id, date_from=date_from, date_to=date_to)
    except SnapshotUnavailableError:
        try:
            merged = load_and_merge_pos_eventtype()
            asset_mask = pd.to_numeric(merged.get("assetId", pd.Series(dtype="object")), errors="coerce") == asset_id
            client_mask = pd.to_numeric(merged.get("clientId", pd.Series(dtype="object")), errors="coerce") == client_id
            merged_subset = merged[asset_mask & client_mask].copy()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Error cargando datos: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error accediendo snapshot: {exc}") from exc

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            partial(detect_and_classify_stops, merged_subset, min_stop_minutes=min_stop_minutes, use_geo=use_geo),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error en detección de paradas: {exc}") from exc

    return StopsResponse(
        asset_id=asset_id,
        client_id=client_id,
        min_stop_minutes=min_stop_minutes,
        use_geo=use_geo,
        **result,
    )


@app.get(
    "/routes/signal-loss/anomalies",
    responses={
        404: {"description": "No se encontraron registros para este asset/client"},
        500: {"description": "Error inesperado en clasificación de anomalías"},
    },
)
def signal_loss_anomalies(
    asset_id: int,
    client_id: int,
    max_speed_kmh: float = 120.0,
    date_from: str | None = None,
    date_to: str | None = None,
) -> SignalLossAnomaliesResponse:
    try:
        merged_subset = get_asset_client_rows(asset_id=asset_id, client_id=client_id, date_from=date_from, date_to=date_to)
    except SnapshotUnavailableError:
        try:
            merged = load_and_merge_pos_eventtype()
            asset_mask = pd.to_numeric(merged.get("assetId", pd.Series(dtype="object")), errors="coerce") == asset_id
            client_mask = pd.to_numeric(merged.get("clientId", pd.Series(dtype="object")), errors="coerce") == client_id
            merged_subset = merged[asset_mask & client_mask].copy()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Error cargando datos: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error accediendo snapshot: {exc}") from exc

    try:
        result = classify_signal_loss_anomalies(merged_subset, max_speed_kmh=max_speed_kmh)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error en clasificación de anomalías: {exc}") from exc

    return SignalLossAnomaliesResponse(
        asset_id=asset_id,
        client_id=client_id,
        total_records=result["total_records"],
        signal_loss_segments_count=result["signal_loss_segments_count"],
        total_signal_loss_minutes=result["total_signal_loss_minutes"],
        max_speed_kmh_threshold=max_speed_kmh,
        anomalies=result["anomalies"],
        error=result.get("error"),
    )


@app.get(
    "/routes/signal-loss",
    responses={
        404: {"description": "No se encontraron registros para este asset/client"},
        500: {"description": "Error inesperado en detección de pérdida de señal"},
    },
)
def signal_loss(
    asset_id: int,
    client_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
) -> SignalLossResponse:
    try:
        merged_subset = get_asset_client_rows(asset_id=asset_id, client_id=client_id, date_from=date_from, date_to=date_to)
    except SnapshotUnavailableError:
        try:
            from app.eventtype_joiner import load_and_merge_pos_eventtype
            import pandas as pd

            merged = load_and_merge_pos_eventtype()
            asset_mask = pd.to_numeric(merged.get("assetId", pd.Series(dtype="object")), errors="coerce") == asset_id
            client_mask = pd.to_numeric(merged.get("clientId", pd.Series(dtype="object")), errors="coerce") == client_id
            merged_subset = merged[asset_mask & client_mask].copy()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Error cargando datos: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error accediendo snapshot: {exc}") from exc

    try:
        result = detect_signal_loss(merged_subset)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error en detección de señal: {exc}") from exc

    return SignalLossResponse(asset_id=asset_id, client_id=client_id, **result)


@app.get("/routes/fleet-summary")
def fleet_summary(
    limit: int = 20,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Top rutas de toda la flota agregadas desde el CSV de entrenamiento."""
    return get_fleet_summary(limit=limit, date_from=date_from, date_to=date_to)


@app.get("/routes/client-assets")
def client_assets(client_id: int, limit: int = 200) -> dict:
    """Lista de assets con viajes para un clientId, ordenados por frecuencia."""
    assets, total = get_client_assets(client_id, limit=limit)
    return {"client_id": client_id, "assets": assets, "total_shown": len(assets), "total_assets": total}


@app.get("/routes/client-summary")
def client_summary(client_id: int, limit: int = 20, asset_ids: str | None = None) -> dict:
    """Top rutas para un clientId específico, con filtro opcional por asset_ids (CSV separado por comas)."""
    parsed_assets = [int(x) for x in asset_ids.split(",") if x.strip().isdigit()] if asset_ids else None
    return get_client_summary(client_id=client_id, limit=limit, asset_ids=parsed_assets)


@app.get("/routes/fleet-heatmap")
def fleet_heatmap(limit_routes: int = 12, limit_clients: int = 10) -> dict:
    """Matriz cliente × ruta con frecuencia de viajes para heatmap."""
    return get_fleet_heatmap(limit_routes=limit_routes, limit_clients=limit_clients)


@app.get("/routes/signal-loss/coverage-summary/cache-status")
def coverage_cache_status(client_id: int) -> dict:
    """Devuelve metadatos del caché sin ejecutar el análisis."""
    return coverage_cache_info(client_id)


@app.get("/routes/signal-loss/coverage-summary")
def signal_loss_coverage_summary(
    client_id: int,
    limit_assets: int = 20,
    grid_size_deg: float = 0.01,
    force_refresh: bool = False,
) -> dict:
    """
    Devuelve zonas sin cobertura GPS del cliente.
    Si existe caché válido se devuelve instantáneamente; usa force_refresh=true para recalcular.
    """
    return get_signal_loss_coverage(
        client_id=client_id,
        limit_assets=limit_assets,
        grid_size_deg=grid_size_deg,
        force_refresh=force_refresh,
    )


@app.delete(
    "/routes/detect/cache",
    responses={200: {"description": "Cache cleared"}},
)
def clear_route_detect_cache(asset_id: int | None = None, client_id: int | None = None) -> dict:
    """Invalidate cached route detection results (memory + disk).

    If asset_id and client_id are both provided, only that asset is removed.
    Otherwise the entire cache is cleared.
    """
    if asset_id is not None and client_id is not None:
        mem_removed = sum(
            1 for k in list(_ROUTE_DETECT_CACHE)
            if k[0] == asset_id and k[1] == client_id and _ROUTE_DETECT_CACHE.pop(k, None) is not None
        )
        disk_removed = clear_route_cache(asset_id, client_id)
    else:
        mem_removed = len(_ROUTE_DETECT_CACHE)
        _ROUTE_DETECT_CACHE.clear()
        disk_removed = clear_route_cache()
    return {"memory_cleared": mem_removed, "disk_cleared": disk_removed}
