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
from app.global_coverage import start_global_coverage_job, get_status as global_cov_status, get_cached_result as global_cov_cached
from app.road_coverage_match import compute_road_coverage_match
from app.historical_stops import get_historical_stop_analysis
from app.batch_stop_analysis import start_batch_job, get_status as batch_stop_status, get_cached_result as batch_stop_cached
from app.signal_loss_detection import detect_signal_loss
from app.signal_loss_anomalies import classify_signal_loss_anomalies
from app.stop_detection import detect_and_classify_stops
from app.asset_stop_profile import build_asset_stop_profile, build_profile_from_stops, flag_atypical_stops
from app.digital_twin import get_asset_twin_state, get_fleet_twin_summary
from app.eventtype_joiner import load_and_merge_pos_eventtype
from app.eventtype_reader import get_eventtype_csv_path
from app.ml.model import RouteMLService
from app.route_learning import build_route_learning_model, predict_destination
from app.route_stop_patterns import build_route_stop_patterns, flag_atypical_trip, assign_stops_to_routes
from app.route_duration_patterns import build_route_duration_patterns, flag_atypical_duration
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


@app.get(
    "/routes/predict/learned-routes",
    responses={
        404: {"description": "Asset/client data not found"},
        500: {"description": "Unexpected error building the route learning model"},
    },
)
def learned_routes(
    asset_id: int,
    client_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
    origin_radius_km: float = 8.0,
    dest_radius_km: float = 8.0,
) -> dict:
    """
    Fase de aprendizaje de rutas históricas del Caso 3: agrupa los recorridos de este
    asset+cliente por origen/destino y calcula, para cada origen, la distribución de
    destinos históricos y el punto de control (proxy de caseta) que los precede.
    """
    detection = _get_or_detect_routes(asset_id, client_id, date_from, date_to)
    routes = detection.get("complete_routes") or []
    model = build_route_learning_model(routes, origin_radius_km=origin_radius_km, dest_radius_km=dest_radius_km)
    return {"asset_id": asset_id, "client_id": client_id, **model}


@app.get(
    "/routes/predict/destination",
    responses={
        404: {"description": "Asset/client data not found"},
        500: {"description": "Unexpected error predicting destination"},
    },
)
def predict_destination_endpoint(
    asset_id: int,
    client_id: int,
    origin_lat: float,
    origin_lon: float,
    passed_roads: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    origin_radius_km: float = 8.0,
    dest_radius_km: float = 8.0,
) -> dict:
    """
    Predicción de destino por aprendizaje de rutas históricas: dado el origen del
    viaje en curso y las carreteras ya recorridas (passed_roads, separadas por '|'),
    devuelve la distribución de destinos históricos aprendida para ese origen.
    """
    detection = _get_or_detect_routes(asset_id, client_id, date_from, date_to)
    routes = detection.get("complete_routes") or []
    model = build_route_learning_model(routes, origin_radius_km=origin_radius_km, dest_radius_km=dest_radius_km)
    roads = [r.strip() for r in passed_roads.split("|")] if passed_roads else []
    prediction = predict_destination(model, origin_lat=origin_lat, origin_lon=origin_lon, passed_roads=roads)
    return {"asset_id": asset_id, "client_id": client_id, **prediction}


@app.get(
    "/routes/predict/duration-patterns",
    responses={
        404: {"description": "Asset/client data not found"},
        500: {"description": "Unexpected error building the duration pattern"},
    },
)
def duration_patterns(
    asset_id: int,
    client_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
    origin_radius_km: float = 8.0,
    dest_radius_km: float = 8.0,
    min_trips_for_pattern: int = 3,
) -> dict:
    """
    Caso 5 (demoras anómalas): aprende, por cada ruta habitual del asset (misma
    agrupación origen/destino que el Caso 3/4), el tiempo normal de recorrido
    (media, mediana, desviación estándar, percentiles) tras filtrar recorridos
    atípicos previos (IQR). Unidad de análisis: assetId + clientId + ruta (sin
    driverId ni segmentación por día/hora — ver route_duration_patterns.py).
    """
    detection = _get_or_detect_routes(asset_id, client_id, date_from, date_to)
    routes = detection.get("complete_routes") or []
    model = build_route_duration_patterns(
        routes, origin_radius_km=origin_radius_km, dest_radius_km=dest_radius_km,
        min_trips_for_pattern=min_trips_for_pattern,
    )
    return {"asset_id": asset_id, "client_id": client_id, "driver_id": None, **model}


@app.get(
    "/routes/predict/duration-anomalies",
    responses={
        404: {"description": "Asset/client data not found"},
        500: {"description": "Unexpected error detecting duration anomalies"},
    },
)
def duration_anomalies(
    asset_id: int,
    client_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
    origin_radius_km: float = 8.0,
    dest_radius_km: float = 8.0,
    min_trips_for_pattern: int = 3,
) -> dict:
    """
    Caso 5: evalúa cada viaje histórico de cada ruta habitual contra el tiempo
    normal de esa ruta y devuelve los viajes con demora anómala (% sobre el
    promedio, severidad).
    """
    detection = _get_or_detect_routes(asset_id, client_id, date_from, date_to)
    routes = detection.get("complete_routes") or []
    model = build_route_duration_patterns(
        routes, origin_radius_km=origin_radius_km, dest_radius_km=dest_radius_km,
        min_trips_for_pattern=min_trips_for_pattern,
    )

    alerts: list[dict] = []
    for pattern in model["route_patterns"]:
        for route_idx in pattern["route_indices"]:
            route = routes[route_idx]
            flag = flag_atypical_duration(pattern, route)
            if flag is None or not flag["is_delayed"]:
                continue
            alerts.append({
                "route_index": route_idx,
                "origin_cluster_id": pattern["origin_cluster_id"],
                **{k: pattern.get(k) for k in ("origin_place_n", "origin_place_name", "destination_place_n", "destination_place_name")},
                "destination_cluster_id": pattern["destination_cluster_id"],
                "start_timestamp": route.get("start_timestamp"),
                "end_timestamp": route.get("end_timestamp"),
                **flag,
            })

    alerts.sort(key=lambda a: (-{"alta": 2, "media": 1}[a["severity"]], -(a["z_score"] or 0)))
    return {
        "asset_id": asset_id, "client_id": client_id, "driver_id": None,
        "total_route_groups": model["total_route_groups"],
        "total_trips_evaluated": sum(p["total_trips"] for p in model["route_patterns"]),
        "delayed_trip_count": len(alerts),
        "route_patterns": model["route_patterns"],
        "alerts": alerts,
    }


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
    result = _get_or_detect_routes(asset_id, client_id, date_from, date_to)
    return RouteDetectionResponse(**result)


def _get_or_detect_routes(
    asset_id: int,
    client_id: int,
    date_from: str | None,
    date_to: str | None,
) -> dict:
    """Detección de rutas por asset+cliente con caché en memoria (5 min) y disco."""
    cache_key = (asset_id, client_id, date_from, date_to)

    # 1. In-memory cache (fastest, 5-min TTL)
    cached = _ROUTE_DETECT_CACHE.get(cache_key)
    if cached is not None:
        stored_at, result = cached
        if time.monotonic() - stored_at < _ROUTE_CACHE_TTL_SECONDS:
            logger.info("Memory cache HIT asset_id=%d client_id=%d", asset_id, client_id)
            return result
        del _ROUTE_DETECT_CACHE[cache_key]

    # 2. Persistent disk cache (survives restarts, invalidated on new snapshot)
    result = get_cached_routes(asset_id, client_id, date_from, date_to)
    if result is not None:
        logger.info("Disk cache HIT asset_id=%d client_id=%d", asset_id, client_id)
        _ROUTE_DETECT_CACHE[cache_key] = (time.monotonic(), result)
        return result

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
    return result


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


@app.post("/routes/signal-loss/global-coverage/compute")
def global_coverage_compute() -> dict:
    """Lanza el job de cobertura global en background. Si ya hay uno corriendo, lo indica."""
    return start_global_coverage_job()


@app.get("/routes/signal-loss/global-coverage/status")
def global_coverage_status_endpoint() -> dict:
    """Devuelve el estado del job de cobertura global (progreso, mensaje, done)."""
    return global_cov_status()


@app.get("/routes/signal-loss/global-coverage")
def global_coverage_result() -> dict:
    """Devuelve el resultado cacheado del análisis global, o {} si no hay."""
    cached = global_cov_cached()
    if cached is None:
        return {"available": False}
    return {"available": True, **cached}


@app.post("/routes/stops/batch-analysis/compute")
def batch_stops_compute(force: bool = False) -> dict:
    return start_batch_job(force=force)


@app.get("/routes/stops/batch-analysis/status")
def batch_stops_status() -> dict:
    return batch_stop_status()


@app.get("/routes/stops/batch-analysis")
def batch_stops_result() -> dict:
    cached = batch_stop_cached()
    if cached is None:
        return {"available": False}
    return {"available": True, **cached}


@app.get("/routes/stops/historical-analysis")
async def historical_stops_analysis(top_locations: int = 30) -> dict:
    """Análisis histórico de paradas: distribución por categoría y top ubicaciones."""
    import asyncio
    from functools import partial
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(get_historical_stop_analysis, top_locations))


@app.get(
    "/routes/stops/asset-profile",
    responses={
        404: {"description": "No se encontraron registros para este asset/client"},
        500: {"description": "Error construyendo el perfil de paradas"},
    },
)
async def stops_asset_profile(
    asset_id: int,
    client_id: int,
    min_stop_minutes: float = 5.0,
    use_geo: bool = True,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Perfil histórico de paradas normales (ubicaciones, categorías, duraciones) para un asset+cliente."""
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
        profile = await loop.run_in_executor(
            None,
            partial(build_asset_stop_profile, merged_subset, min_stop_minutes=min_stop_minutes, use_geo=use_geo),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error construyendo perfil de paradas: {exc}") from exc

    return {"asset_id": asset_id, "client_id": client_id, **profile}


@app.get(
    "/routes/stops/asset-anomalies",
    responses={
        404: {"description": "No se encontraron registros para este asset/client"},
        500: {"description": "Error detectando paradas atípicas"},
    },
)
async def stops_asset_anomalies(
    asset_id: int,
    client_id: int,
    min_stop_minutes: float = 5.0,
    use_geo: bool = True,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """
    Marca las paradas del asset+cliente que se apartan de su propio patrón histórico
    (ubicación, tipo o duración no habituales). Precursor del Caso 4 — aún sin
    segmentación por conductor ni por ruta.
    """
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

    def _compute() -> dict:
        classified = detect_and_classify_stops(merged_subset, min_stop_minutes=min_stop_minutes, use_geo=use_geo)
        if classified.get("error") or not classified["stops"]:
            return {"total_stops": 0, "atypical_count": 0, "stops": [], "error": classified.get("error")}
        profile = build_profile_from_stops(classified["stops"])
        flagged = flag_atypical_stops(profile, classified["stops"])
        return {
            "total_stops": len(flagged),
            "atypical_count": sum(1 for s in flagged if s["is_atypical"]),
            "stops": flagged,
            "error": None,
        }

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _compute)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error detectando paradas atípicas: {exc}") from exc

    return {"asset_id": asset_id, "client_id": client_id, **result}


def _load_asset_client_subset(asset_id: int, client_id: int, date_from: str | None, date_to: str | None) -> pd.DataFrame:
    try:
        return get_asset_client_rows(asset_id=asset_id, client_id=client_id, date_from=date_from, date_to=date_to)
    except SnapshotUnavailableError:
        try:
            merged = load_and_merge_pos_eventtype()
            asset_mask = pd.to_numeric(merged.get("assetId", pd.Series(dtype="object")), errors="coerce") == asset_id
            client_mask = pd.to_numeric(merged.get("clientId", pd.Series(dtype="object")), errors="coerce") == client_id
            return merged[asset_mask & client_mask].copy()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Error cargando datos: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error accediendo snapshot: {exc}") from exc


@app.get(
    "/routes/stops/route-patterns",
    responses={
        404: {"description": "No se encontraron registros para este asset/client"},
        500: {"description": "Error construyendo el patrón de paradas por ruta"},
    },
)
async def stops_route_patterns(
    asset_id: int,
    client_id: int,
    min_stop_minutes: float = 5.0,
    use_geo: bool = True,
    date_from: str | None = None,
    date_to: str | None = None,
    origin_radius_km: float = 8.0,
    dest_radius_km: float = 8.0,
    min_trips_for_pattern: int = 3,
) -> dict:
    """
    Caso 4 (paradas atípicas): aprende, por cada ruta habitual del asset (agrupada por
    origen/destino, ver route_learning.py), el número típico de paradas, sus ubicaciones/
    categorías/duraciones (reutiliza asset_stop_profile.py) y la secuencia típica de
    categorías. NOTA DE ALCANCE: el dataset no tiene driverId — la unidad de análisis
    es assetId + ruta, no assetId + conductor + ruta como en la metodología original.
    """
    merged_subset = _load_asset_client_subset(asset_id, client_id, date_from, date_to)
    detection = _get_or_detect_routes(asset_id, client_id, date_from, date_to)
    routes = detection.get("complete_routes") or []

    def _compute() -> dict:
        classified = detect_and_classify_stops(merged_subset, min_stop_minutes=min_stop_minutes, use_geo=use_geo)
        stops = classified.get("stops") or []
        return build_route_stop_patterns(
            routes, stops,
            origin_radius_km=origin_radius_km, dest_radius_km=dest_radius_km,
            min_trips_for_pattern=min_trips_for_pattern,
        )

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _compute)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error construyendo patrón de paradas por ruta: {exc}") from exc

    return {"asset_id": asset_id, "client_id": client_id, "driver_id": None, **result}


@app.get(
    "/routes/stops/route-anomalies",
    responses={
        404: {"description": "No se encontraron registros para este asset/client"},
        500: {"description": "Error detectando viajes atípicos"},
    },
)
async def stops_route_anomalies(
    asset_id: int,
    client_id: int,
    min_stop_minutes: float = 5.0,
    use_geo: bool = True,
    date_from: str | None = None,
    date_to: str | None = None,
    origin_radius_km: float = 8.0,
    dest_radius_km: float = 8.0,
    min_trips_for_pattern: int = 3,
) -> dict:
    """
    Caso 4: evalúa cada viaje histórico de cada ruta habitual contra el patrón normal
    de esa ruta y devuelve los viajes atípicos (cantidad, ubicación, tipo, duración o
    secuencia de paradas fuera de lo común), con severidad. Ver nota de alcance en
    /routes/stops/route-patterns (sin driverId en el dataset).
    """
    merged_subset = _load_asset_client_subset(asset_id, client_id, date_from, date_to)
    detection = _get_or_detect_routes(asset_id, client_id, date_from, date_to)
    routes = detection.get("complete_routes") or []

    def _compute() -> dict:
        classified = detect_and_classify_stops(merged_subset, min_stop_minutes=min_stop_minutes, use_geo=use_geo)
        stops = classified.get("stops") or []
        model = build_route_stop_patterns(
            routes, stops,
            origin_radius_km=origin_radius_km, dest_radius_km=dest_radius_km,
            min_trips_for_pattern=min_trips_for_pattern,
        )
        stop_assignment = assign_stops_to_routes(routes, stops)

        alerts: list[dict] = []
        for pattern in model["route_patterns"]:
            for route_idx in pattern["route_indices"]:
                trip_stops = stop_assignment.get(route_idx, [])
                trip = flag_atypical_trip(pattern, trip_stops)
                if not trip["is_atypical"]:
                    continue
                route = routes[route_idx]
                alerts.append({
                    "route_index": route_idx,
                    "origin_cluster_id": pattern["origin_cluster_id"],
                    **{k: pattern.get(k) for k in ("origin_place_n", "origin_place_name", "destination_place_n", "destination_place_name")},
                    "destination_cluster_id": pattern["destination_cluster_id"],
                    "start_timestamp": route.get("start_timestamp"),
                    "end_timestamp": route.get("end_timestamp"),
                    "reasons": trip["reasons"],
                    "severity": trip["severity"],
                    "trip_stop_count": trip["trip_stop_count"],
                    "expected_stop_count_range": trip["expected_stop_count_range"],
                    "sequence_actual": trip["sequence_actual"],
                    "sequence_typical": trip["sequence_typical"],
                    "atypical_stops": [s for s in trip["stops"] if s["is_atypical"]],
                })

        alerts.sort(key=lambda a: (-{"alta": 2, "media": 1, "baja": 0}[a["severity"]], a["start_timestamp"] or ""))
        return {
            "total_route_groups": model["total_route_groups"],
            "total_trips_evaluated": sum(p["total_trips"] for p in model["route_patterns"]),
            "atypical_trip_count": len(alerts),
            "route_patterns": model["route_patterns"],
            "alerts": alerts,
        }

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _compute)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error detectando viajes atípicos: {exc}") from exc

    return {"asset_id": asset_id, "client_id": client_id, "driver_id": None, **result}


@app.get("/routes/signal-loss/road-coverage-match")
def road_coverage_match() -> dict:
    """Cruza los hotspots de cobertura global con el catálogo de carreteras."""
    return compute_road_coverage_match()


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
