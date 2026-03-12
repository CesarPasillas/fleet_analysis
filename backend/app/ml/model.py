from __future__ import annotations

import json
import os
from pathlib import Path

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
            "distance_km",
            "traffic_level",
            "weather_index",
            "hour_of_day",
            "day_of_week",
            "vehicle_load_kg",
        ]

    def _resolve_path(self, configured_path: str) -> Path:
        path = Path(configured_path)
        if path.is_absolute():
            return path
        return self.backend_dir / path

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

        required_columns = self.feature_order + ["route_id"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        x = data[self.feature_order].astype(float).values
        labels = data["route_id"].astype(str)
        classes = sorted(labels.unique().tolist())
        class_to_idx = {route: idx for idx, route in enumerate(classes)}
        y = labels.map(class_to_idx).values

        x_train, x_test, y_train, y_test = train_test_split(
            x,
            y,
            test_size=0.2,
            random_state=42,
            stratify=y,
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

        return len(data), float(accuracy)

    def predict(self, features: RouteFeatures) -> tuple[str, float]:
        if not self.model_path.exists() or not self.metadata_path.exists():
            raise FileNotFoundError("Model artifacts not found. Train the model first.")

        model = tf.keras.models.load_model(self.model_path)
        metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))

        feature_order = metadata["feature_order"]
        classes = metadata["classes"]
        mean = np.array(metadata["scaler"]["mean"], dtype=float)
        scale = np.array(metadata["scaler"]["scale"], dtype=float)

        values = np.array([[getattr(features, feature_name) for feature_name in feature_order]], dtype=float)
        values_scaled = (values - mean) / scale

        probabilities = model.predict(values_scaled, verbose=0)[0]
        predicted_index = int(np.argmax(probabilities))

        return classes[predicted_index], float(probabilities[predicted_index])
