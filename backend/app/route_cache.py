from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from app.analytics_store import get_default_duckdb_path

_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "route_cache"


def _cache_path(
    asset_id: int,
    client_id: int,
    date_from: str | None,
    date_to: str | None,
) -> Path:
    suffix = f"{asset_id}_{client_id}_{date_from or ''}_{date_to or ''}"
    return _CACHE_DIR / f"{suffix}.json.gz"


def _is_stale(cache_file: Path) -> bool:
    db_path = get_default_duckdb_path()
    if not db_path.exists():
        return True
    return db_path.stat().st_mtime > cache_file.stat().st_mtime


def get_cached_routes(
    asset_id: int,
    client_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any] | None:
    path = _cache_path(asset_id, client_id, date_from, date_to)
    if not path.exists():
        return None
    if _is_stale(path):
        path.unlink(missing_ok=True)
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        path.unlink(missing_ok=True)
        return None


def store_cached_routes(
    asset_id: int,
    client_id: int,
    date_from: str | None = None,
    date_to: str | None = None,
    result: dict[str, Any] | None = None,
) -> None:
    if result is None:
        return
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(asset_id, client_id, date_from, date_to)
    try:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            json.dump(result, fh)
    except Exception:
        path.unlink(missing_ok=True)


def clear_route_cache(
    asset_id: int | None = None,
    client_id: int | None = None,
) -> int:
    if not _CACHE_DIR.exists():
        return 0
    pattern = f"{asset_id}_{client_id}_*.json.gz" if (asset_id is not None and client_id is not None) else "*.json.gz"
    files = list(_CACHE_DIR.glob(pattern))
    for f in files:
        f.unlink(missing_ok=True)
    return len(files)


# ── Coverage-summary cache ─────────────────────────────────────────────────────

def _coverage_cache_path(client_id: int) -> Path:
    return _CACHE_DIR / f"coverage_{client_id}.json.gz"


def get_cached_coverage(client_id: int) -> dict[str, Any] | None:
    """Return persisted coverage result or None if absent / stale."""
    path = _coverage_cache_path(client_id)
    if not path.exists():
        return None
    if _is_stale(path):
        path.unlink(missing_ok=True)
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
            data["from_cache"] = True
            return data
    except Exception:
        path.unlink(missing_ok=True)
        return None


def store_cached_coverage(client_id: int, result: dict[str, Any]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _coverage_cache_path(client_id)
    try:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            json.dump(result, fh)
    except Exception:
        path.unlink(missing_ok=True)


def coverage_cache_info(client_id: int) -> dict[str, Any]:
    """Return metadata about a cached coverage result without loading it."""
    path = _coverage_cache_path(client_id)
    if not path.exists():
        return {"exists": False, "stale": False, "cached_at": None, "size_kb": None}
    stale = _is_stale(path)
    cached_at = None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
            cached_at = data.get("cached_at")
    except Exception:
        pass
    return {
        "exists": True,
        "stale": stale,
        "cached_at": cached_at,
        "size_kb": round(path.stat().st_size / 1024, 1),
    }
