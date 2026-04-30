#!/usr/bin/env python3
"""Script para lanzar el entrenamiento del modelo ML en el backend usando el endpoint /ml/train."""

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Call the backend /ml/train endpoint to train the route ML model."
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Backend host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        default=8000,
        type=int,
        help="Backend port (default: 8000)",
    )
    parser.add_argument(
        "--csv-path",
        default="data/route_geometry_training_batch.csv",
        help="Relative path to the training CSV file from the backend folder.",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=50,
        help="Number of training epochs (default: 50)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    url = f"http://{args.host}:{args.port}/ml/train"
    payload = {
        "csv_path": args.csv_path,
        "epochs": args.epochs,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
    }
    request = Request(url, data=body, headers=headers, method="POST")

    try:
        with urlopen(request, timeout=120) as response:
            response_text = response.read().decode("utf-8")
            print("Entrenamiento iniciado correctamente.")
            print(response_text)
            return 0
    except HTTPError as exc:
        print(f"Error HTTP: {exc.code} - {exc.reason}", file=sys.stderr)
        try:
            error_body = exc.read().decode("utf-8")
            print(error_body, file=sys.stderr)
        except Exception:
            pass
        return 1
    except URLError as exc:
        print(f"No se pudo conectar al backend: {exc.reason}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error inesperado: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
