from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock
from typing import Any

import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from app.schemas import RouteFeatures


class RouteMLService:
    def __init__(self) -> None:
        self.backend_dir = Path(__file__).resolve().parents[2]
        self.model_path = self._resolve_path(
            os.getenv("MODEL_PATH", "app/ml/artifacts/route_model.keras")
        )
        self.metadata_path = self._resolve_path(
            os.getenv("MODEL_METADATA_PATH", "app/ml/artifacts/metadata.json")
        )
        self.feature_order = [
            "start_lat",
            "start_lon",
            "distance_km",
            "hour_of_day",
            "day_of_week",
        ]
        self._cache_lock = Lock()
        self._model_cache: tf.keras.Model | None = None
        self._metadata_cache: dict[str, Any] | None = None
        self._artifact_cache_key: tuple[int, int] | None = None

    def _resolve_path(self, configured_path: str) -> Path:
        path = Path(configured_path)
        if path.is_absolute():
            return path
        return self.backend_dir / path

    def _current_artifact_cache_key(self) -> tuple[int, int]:
        return (
            self.model_path.stat().st_mtime_ns,
            self.metadata_path.stat().st_mtime_ns,
        )

    def _load_artifacts(self) -> tuple[tf.keras.Model, dict[str, Any]]:
        if not self.model_path.exists() or not self.metadata_path.exists():
            raise FileNotFoundError("Model artifacts not found. Train the model first.")

        current_cache_key = self._current_artifact_cache_key()
        with self._cache_lock:
            cache_is_fresh = (
                self._model_cache is not None
                and self._metadata_cache is not None
                and self._artifact_cache_key == current_cache_key
            )
            if not cache_is_fresh:
                self._model_cache = tf.keras.models.load_model(self.model_path)
                self._metadata_cache = json.loads(self.metadata_path.read_text(encoding="utf-8"))
                self._artifact_cache_key = current_cache_key

            return self._model_cache, self._metadata_cache

    def _normalize_route_label(self, sequence: object) -> str:
        if sequence is None or (isinstance(sequence, float) and pd.isna(sequence)):
            return ""

        if isinstance(sequence, str):
            try:
                decoded = json.loads(sequence)
                if isinstance(decoded, list):
                    return " | ".join(str(item).strip() for item in decoded if item is not None)
            except json.JSONDecodeError:
                pass
            return sequence.strip()

        if isinstance(sequence, list):
            return " | ".join(str(item).strip() for item in sequence if item is not None)

        return str(sequence).strip()

    def _ensure_training_features(self, data: pd.DataFrame) -> pd.DataFrame:
        if "start_lat" not in data.columns or "start_lon" not in data.columns:
            if "polyline" in data.columns:
                def _first_point(raw):
                    try:
                        pts = raw if isinstance(raw, list) else json.loads(raw)
                        return float(pts[0][0]), float(pts[0][1])
                    except Exception:
                        return np.nan, np.nan

                coords = data["polyline"].apply(_first_point)
                data["start_lat"] = coords.apply(lambda c: c[0])
                data["start_lon"] = coords.apply(lambda c: c[1])
            else:
                data["start_lat"] = 0.0
                data["start_lon"] = 0.0

        data["start_lat"] = data["start_lat"].astype(float)
        data["start_lon"] = data["start_lon"].astype(float)

        if "hour_of_day" not in data.columns or "day_of_week" not in data.columns:
            if "start_timestamp" in data.columns:
                parsed = pd.to_datetime(data["start_timestamp"], errors="coerce", utc=True)
                if "hour_of_day" not in data.columns:
                    data["hour_of_day"] = parsed.dt.hour.fillna(9).astype(int)
                if "day_of_week" not in data.columns:
                    data["day_of_week"] = parsed.dt.dayofweek.fillna(1).astype(int)
            else:
                if "hour_of_day" not in data.columns:
                    data["hour_of_day"] = 9
                if "day_of_week" not in data.columns:
                    data["day_of_week"] = 1

        if "distance_km" not in data.columns:
            data["distance_km"] = 0.0
        data["distance_km"] = data["distance_km"].astype(float).clip(lower=0.0)

        return data

    def _build_model(self, input_dim: int, output_dim: int) -> tf.keras.Model:
        model = tf.keras.Sequential(
            [
                tf.keras.layers.Input(shape=(input_dim,)),
                tf.keras.layers.Dense(64, activation="relu"),
                tf.keras.layers.Dense(32, activation="relu"),
                tf.keras.layers.Dense(output_dim, activation="softmax"),
            ]
        )
        model.compile(
            optimizer="adam",
            loss="sparse_categorical_crossentropy",
            metrics=["accuracy"],
        )
        return model

    def train(self, csv_path: str, epochs: int = 50) -> tuple[int, float]:
        data = pd.read_csv(csv_path)

        data = self._ensure_training_features(data)

        if "route_id" in data.columns:
            labels = data["route_id"].astype(str)
        elif "route_sequence" in data.columns:
            labels = data["route_sequence"].apply(self._normalize_route_label)
        elif "inferred_roads" in data.columns:
            labels = data["inferred_roads"].apply(self._normalize_route_label)
        else:
            raise ValueError(
                "Missing required label column: route_id or route_sequence or inferred_roads"
            )

        labels = labels.astype(str)
        valid_mask = labels.notna() & labels.str.strip().astype(bool)
        if not valid_mask.any():
            raise ValueError(
                "Training labels contain no valid route labels. Provide route_id, route_sequence, or inferred_roads values."
            )

        if valid_mask.sum() != len(labels):
            data = data.loc[valid_mask].reset_index(drop=True)
            labels = labels.loc[valid_mask].reset_index(drop=True)

        if labels.isna().any() or labels.eq("").any():
            raise ValueError(
                "Training labels contain empty values after filtering. Check your dataset labels."
            )

        x = data[self.feature_order].astype(float).values
        classes = sorted(labels.unique().tolist())
        class_to_idx = {route: idx for idx, route in enumerate(classes)}
        y = labels.map(class_to_idx).values

        if len(classes) < 2:
            raise ValueError("Training dataset must contain at least two route classes")

        stratify_labels = y if pd.Series(y).value_counts().min() >= 2 else None
        x_train, x_test, y_train, y_test = train_test_split(
            x,
            y,
            test_size=0.2,
            random_state=42,
            stratify=stratify_labels,
        )

        scaler = StandardScaler()
        x_train_scaled = scaler.fit_transform(x_train)
        x_test_scaled = scaler.transform(x_test)

        model = self._build_model(input_dim=x_train_scaled.shape[1], output_dim=len(classes))
        model.fit(
            x_train_scaled,
            y_train,
            epochs=epochs,
            validation_split=0.2,
            verbose=0,
        )

        _, accuracy = model.evaluate(x_test_scaled, y_test, verbose=0)

        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)

        model.save(self.model_path)

        metadata = {
            "feature_order": self.feature_order,
            "classes": classes,
            "scaler": {
                "mean": scaler.mean_.tolist(),
                "scale": scaler.scale_.tolist(),
            },
        }
        self.metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        with self._cache_lock:
            self._model_cache = model
            self._metadata_cache = metadata
            self._artifact_cache_key = self._current_artifact_cache_key()

        return len(data), float(accuracy)

    def predict(self, features: RouteFeatures) -> tuple[str, float]:
        model, metadata = self._load_artifacts()

        feature_order = metadata["feature_order"]
        classes = metadata["classes"]
        mean = np.array(metadata["scaler"]["mean"], dtype=float)
        scale = np.array(metadata["scaler"]["scale"], dtype=float)

        values = np.array([[getattr(features, feature_name) for feature_name in feature_order]], dtype=float)
        values_scaled = (values - mean) / scale

        probabilities = model.predict(values_scaled, verbose=0)[0]
        predicted_index = int(np.argmax(probabilities))

        return classes[predicted_index], float(probabilities[predicted_index])
