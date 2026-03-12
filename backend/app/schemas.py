from typing import Any

from pydantic import BaseModel, Field


class RouteFeatures(BaseModel):
    distance_km: float = Field(..., ge=0)
    traffic_level: float = Field(..., ge=0, le=10)
    weather_index: float = Field(..., ge=0, le=10)
    hour_of_day: int = Field(..., ge=0, le=23)
    day_of_week: int = Field(..., ge=0, le=6)
    vehicle_load_kg: float = Field(..., ge=0)


class PredictionResponse(BaseModel):
    predicted_route: str
    confidence: float


class TrainRequest(BaseModel):
    csv_path: str | None = None
    epochs: int = Field(default=50, ge=5, le=500)


class TrainResponse(BaseModel):
    message: str
    samples: int
    accuracy: float


class MergeEventTypeSummaryResponse(BaseModel):
    total_rows: int
    total_columns: int
    null_event_type_rows: int
    null_key_rows: int
    distinct_event_types: int
    columns: list[str]
    sample: list[dict[str, Any]]


class TwinAssetState(BaseModel):
    asset_id: int | None = None
    client_id: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    event_type: int | None = None
    event_key: str | None = None
    event_description: str | None = None
    gps_timestamp: str | None = None
    server_timestamp: str | None = None


class TwinRecentEvent(BaseModel):
    event_type: int | None = None
    event_key: str | None = None
    event_description: str | None = None
    gps_timestamp: str | None = None
    server_timestamp: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class TwinAssetStateResponse(BaseModel):
    current_state: TwinAssetState
    recent_events: list[TwinRecentEvent]


class TwinFleetSummaryResponse(BaseModel):
    total_records: int
    total_assets: int
    sample_assets: list[TwinAssetState]


class MergeEventTypeRowsResponse(BaseModel):
    total_rows: int
    offset: int
    limit: int
    columns: list[str]
    rows: list[dict[str, Any]]


class CarreterasRowsResponse(BaseModel):
    total_rows: int
    offset: int
    limit: int
    columns: list[str]
    rows: list[dict[str, Any]]


class BackendFilesResponse(BaseModel):
    pos_directory: str
    eventtype_file: str
    total_pos_files: int
    pos_files: list[str]


class SnapshotBuildRequest(BaseModel):
    db_path: str | None = None
    pos_glob: str | None = None
    eventtype_csv: str | None = None
    carreteras_glob: str | None = None
    pos_format: str = Field(default="auto")


class SnapshotBuildResponse(BaseModel):
    db_path: str
    pos_rows: int
    eventtype_rows: int
    merged_rows: int
    distinct_assets: int
    carreteras_rows: int | None = None


class SnapshotStatusResponse(BaseModel):
    db_path: str
    exists: bool
    last_updated_iso: str | None = None
    pos_rows: int | None = None
    eventtype_rows: int | None = None
    merged_rows: int | None = None
    distinct_assets: int | None = None
    carreteras_rows: int | None = None


class CompleteRoute(BaseModel):
    start_timestamp: str | None = None
    end_timestamp: str | None = None
    duration_hours: float | None = None
    distance_km: float
    total_events: int
    unique_event_types: int
    total_gps_points: int
    matched_roads: list[str] = []
    road_match_ratio: float | None = None
    start_index: int
    end_index: int
    components: list[dict[str, Any]]


class IncompleteRoute(BaseModel):
    type: str
    start_index: int | None = None
    end_index: int | None = None
    start_timestamp: str | None = None
    end_timestamp: str | None = None
    detail: str


class AssetRecord(BaseModel):
    record_index: int | None = None
    order_timestamp: str | None = None
    gps_timestamp: str | None = None
    server_timestamp: str | None = None
    event_type: int | None = None
    olc: str | None = None
    event_key: str | None = None
    event_description: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    matched_road_label: str | None = None
    matched_road_name: str | None = None
    matched_route_name: str | None = None
    matched_road_no: str | None = None
    matched_road_distance_m: float | None = None


class RouteDetectionResponse(BaseModel):
    asset_id: int
    client_id: int
    event_start: int
    event_end: int
    total_records_analyzed: int
    complete_routes_count: int
    incomplete_routes_count: int
    complete_routes: list[CompleteRoute]
    incomplete_routes: list[IncompleteRoute]
    all_records_count: int
    all_records: list[AssetRecord]
