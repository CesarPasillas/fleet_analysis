# Sistema de análisis de rutas con ML

Proyecto MVP con:
- **Backend:** Python + FastAPI + TensorFlow + Pandas/Numpy
- **Frontend:** React (Vite)

## Arranque rápido (un solo comando)

```bash
make install
make run-all
```

Comandos útiles:

```bash
make run-backend
make run-frontend
```

Si `make` no funciona en macOS por licencia de Xcode, usa scripts:

```bash
./scripts/run_backend.sh
./scripts/run_frontend.sh
# o ambos
./scripts/run_all.sh
# detener servicios
./scripts/stop_all.sh
```

Puertos por defecto:
- Backend: `http://127.0.0.1:8000`
- Frontend: `http://127.0.0.1:5173`

## Estructura

- `backend/`: API y modelo de ML
- `frontend/`: interfaz para entrenar y predecir rutas

## 1) Backend (Python)

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Configura fuentes de datos por variables de entorno (opcional):

```bash
DATA_POS_DIR=data/pos
DATA_POS_GLOB_PATTERN=pos*.csv
# Opcional para snapshot DuckDB (prioridad alta):
# DATA_POS_GLOB=data/pos/pos*.csv
DATA_EVENTTYPE_CSV=data/EventType.csv
```

Perfiles sugeridos en el repo:

- `backend/.env`: configuración local lista para esta estructura.
- `backend/.env.readonly.example`: plantilla para entorno con datos externos en solo lectura.

Endpoints:
- `GET /health`
- `POST /ml/train`
- `POST /ml/predict`

Ejemplo entrenamiento:

```bash
curl -X POST http://localhost:8000/ml/train \
  -H "Content-Type: application/json" \
  -d '{"epochs": 50}'
```

Ejemplo predicción:

```bash
curl -X POST http://localhost:8000/ml/predict \
  -H "Content-Type: application/json" \
  -d '{
    "distance_km": 15,
    "traffic_level": 5,
    "weather_index": 3,
    "hour_of_day": 9,
    "day_of_week": 1,
    "vehicle_load_kg": 1000
  }'
```

## 2) Frontend (React)

```bash
cd frontend
npm install
npm run dev
```

Abrir en navegador: `http://localhost:5173`

## Dataset de ejemplo

Se incluye en `backend/data/routes_training.csv`.
Puedes reemplazarlo con tus datos reales manteniendo columnas:

- `distance_km`
- `traffic_level`
- `weather_index`
- `hour_of_day`
- `day_of_week`
- `vehicle_load_kg`
- `route_id` (etiqueta objetivo)

## Próximos pasos sugeridos

- Conectar a base de datos real de telemetría/rutas.
- Agregar endpoint para reentrenamiento programado.
- Añadir autenticación y control de acceso.
- Evaluar modelos secuenciales (LSTM/Transformer) para predicción temporal de rutas.

## Flujo recomendado para gemelo digital (solo lectura)

Para trabajar sin tocar producción, este proyecto ahora soporta un snapshot analítico local con DuckDB.

Flujo sugerido:

1. Recibir archivos CSV/Parquet o extraer desde una fuente read-only.
2. Construir snapshot local en DuckDB con endpoint de ingesta.
3. Ejecutar análisis/predicciones sobre snapshot (no sobre producción).
4. Publicar resultados en reportes/API separada sin write-back a origen.

Instalar dependencias backend:

```bash
cd backend
pip install -r requirements.txt
```

Construir snapshot:

```bash
curl -X POST http://localhost:8000/data/snapshot/build \
  -H "Content-Type: application/json" \
  -d '{}'
```

Por defecto usa archivos POS CSV (`data/pos/pos*.csv`).
Si tus archivos ya son Parquet, envía `pos_format: "parquet"` y un patrón `pos_glob` de Parquet.

Si no quieres pasar `pos_glob` en cada request, define `DATA_POS_GLOB` en `backend/.env`.
También puedes separar directorio + patrón con `DATA_POS_DIR` y `DATA_POS_GLOB_PATTERN`.

Ejemplo con Parquet:

```bash
curl -X POST http://localhost:8000/data/snapshot/build \
  -H "Content-Type: application/json" \
  -d '{
    "pos_glob": "/ruta/a/pos/*.parquet",
    "eventtype_csv": "/ruta/a/EventType.csv",
    "pos_format": "parquet"
  }'
```

Consultar estado del snapshot:

```bash
curl "http://localhost:8000/data/snapshot/status"
```

Opcionalmente puedes enviar rutas personalizadas:

```json
{
  "db_path": "/ruta/a/fleet_analytics.duckdb",
  "pos_glob": "/ruta/a/pos/pos*.csv",
  "eventtype_csv": "/ruta/a/EventType.csv",
  "pos_format": "auto"
}
```

## Script multiplataforma para pre-cargar snapshot (macOS y Windows)

Para evitar procesar todo al iniciar la app, usa este script una vez (o programado):

`python scripts/build_snapshot.py`

Funciona en macOS y Windows porque está en Python.

Wrappers incluidos:

- macOS/Linux: `./scripts/build_snapshot.sh --to-parquet --skip-if-fresh`
- Windows (PowerShell): `./scripts/build_snapshot.ps1 --to-parquet --skip-if-fresh`

Ejemplos:

1. Construir snapshot desde CSV (usa rutas de `.env` por defecto):

```bash
python scripts/build_snapshot.py
```

2. Convertir CSV a Parquet y luego construir DuckDB:

```bash
python scripts/build_snapshot.py --to-parquet
```

3. Saltar reconstrucción si no hay cambios:

```bash
python scripts/build_snapshot.py --to-parquet --skip-if-fresh
```

4. Usar fuentes/rutas custom:

```bash
python scripts/build_snapshot.py \
  --pos-glob "/ruta/a/pos/*.csv" \
  --eventtype-csv "/ruta/a/EventType.csv" \
  --db-path "/ruta/a/fleet_analytics.duckdb" \
  --to-parquet
```
