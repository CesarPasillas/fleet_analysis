# Guia de entrenamiento local del modelo ML

Este documento explica como entrenar el modelo de rutas de este sistema en un ambiente local, de forma reproducible para cualquier persona del equipo.

## 1. Objetivo

Entrenar el clasificador de rutas y generar los artefactos que usa el endpoint de prediccion:

- Modelo Keras: `backend/app/ml/artifacts/route_model.keras`
- Metadata (clases + scaler): `backend/app/ml/artifacts/metadata.json`

## 2. Prerrequisitos

- macOS, Linux o Windows (PowerShell)
- Python 3.10+ (recomendado 3.12)
- Dependencias de `backend/requirements.txt`
- Dataset CSV con columnas requeridas

Dependencias principales:

- `tensorflow==2.16.2`
- `scikit-learn==1.5.2`
- `pandas==2.2.3`
- `numpy==1.26.4`
- `fastapi==0.115.0`
- `uvicorn[standard]==0.30.6`

## 3. Estructura de datos requerida

El entrenamiento requiere estas columnas en el CSV:

- `distance_km`
- `traffic_level`
- `weather_index`
- `hour_of_day`
- `day_of_week`
- `vehicle_load_kg`
- `route_id` (etiqueta objetivo)

Dataset ejemplo incluido en el repo:

- `backend/data/routes_training.csv`

## 4. Preparar entorno local

Desde la raiz del proyecto:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

Opcional (con Makefile):

```bash
make backend-install
```

## 5. Levantar backend

Con script recomendado:

```bash
./scripts/run_backend.sh
```

Alternativa directa:

```bash
source .venv/bin/activate
python -m uvicorn app.main:app --app-dir backend --host 127.0.0.1 --port 8000 --reload
```

Verificar salud:

```bash
curl http://127.0.0.1:8000/health
```

Respuesta esperada:

```json
{"status":"ok"}
```

## 6. Ejecutar entrenamiento por API

Endpoint:

- `POST /ml/train`

### 6.1 Entrenar con dataset por defecto

Si no mandas `csv_path`, el backend toma (en este orden):

1. `csv_path` del request (si viene)
2. variable de entorno `DEFAULT_TRAINING_DATA`
3. `backend/data/routes_training.csv`

Comando:

```bash
curl -X POST http://127.0.0.1:8000/ml/train \
  -H "Content-Type: application/json" \
  -d '{"epochs": 50}'
```

### 6.2 Entrenar con dataset custom

```bash
curl -X POST http://127.0.0.1:8000/ml/train \
  -H "Content-Type: application/json" \
  -d '{
    "csv_path": "/ruta/absoluta/a/mi_dataset.csv",
    "epochs": 80
  }'
```

Notas importantes:

- `epochs` permitido: minimo 5, maximo 500
- Si faltan columnas, el endpoint responde 400
- Si el archivo no existe, responde 404

## 7. Validar artefactos generados

Despues de entrenar, valida que existan:

```bash
ls -lh backend/app/ml/artifacts/
```

Debe haber al menos:

- `route_model.keras`
- `metadata.json`

Tambien puedes validar con una prediccion:

```bash
curl -X POST http://127.0.0.1:8000/ml/predict \
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

## 8. Como funciona el entrenamiento internamente

El servicio `RouteMLService.train()` realiza este pipeline:

1. Carga CSV con pandas
2. Valida columnas obligatorias
3. Construye `X` con features y `y` desde `route_id`
4. Convierte clases de `route_id` a indices numericos
5. Split train/test (80/20, `random_state=42`, `stratify=y`)
6. Estandariza features con `StandardScaler`
7. Entrena MLP en Keras:
   - Input
   - Dense(64, relu)
   - Dense(32, relu)
   - Dense(K, softmax)
8. Evalua accuracy sobre test set
9. Guarda modelo y metadata (orden de features, clases y scaler)

## 9. Configuracion de rutas de artefactos

Puedes cambiar donde se guardan/cargan artefactos con variables de entorno:

- `MODEL_PATH` (default: `app/ml/artifacts/route_model.keras`)
- `MODEL_METADATA_PATH` (default: `app/ml/artifacts/metadata.json`)

Si no son rutas absolutas, se resuelven relativas a `backend/`.

## 10. Auto-entrenamiento al iniciar el backend

Al iniciar FastAPI, si no existen artefactos y existe dataset de entrenamiento por defecto, el sistema intenta entrenar automaticamente.

Recomendacion en ambientes productivos:

- Entrenar explicitamente en un paso controlado (CI/CD o tarea de MLOps)
- Versionar artefactos fuera del runtime local

## 11. Troubleshooting

### Error 400: Missing required columns

Causa: el CSV no tiene todas las columnas obligatorias.

Solucion: corrige encabezados y tipos numericos de features.

### Error 404 en /ml/train

Causa: el `csv_path` no existe.

Solucion: usa ruta absoluta valida o coloca el archivo en `backend/data/`.

### Proceso termina con codigo 137 o muy lento

Causa probable: limite de memoria/CPU durante entrenamiento (TensorFlow).

Soluciones:

- Cerrar procesos pesados
- Bajar temporalmente `epochs` (ej. 20)
- Ejecutar sin `--reload`
- Entrenar en equipo con mas RAM

## 12. Checklist rapido para otra persona

1. Clonar repo
2. Crear y activar `.venv`
3. Instalar `backend/requirements.txt`
4. Verificar dataset con columnas requeridas
5. Levantar backend
6. Ejecutar `POST /ml/train`
7. Confirmar artefactos en `backend/app/ml/artifacts/`
8. Probar `POST /ml/predict`
