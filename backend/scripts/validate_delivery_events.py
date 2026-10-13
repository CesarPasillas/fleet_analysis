#!/usr/bin/env python3
"""
Validación de rutas/escalas contra los eventos de entrega del propio GPS.

No hay inicios/fines oficiales de las rutas. Algunos assets reportan eventos de entrega
(DELIVERY_STARTED, ORDER_DELIVERED, ...). Si son entregas reales, deben ocurrir con el camión
detenido, es decir, dentro de las escalas que detecta detect_route_turnarounds.py, y no durante
una ruta (en movimiento). Este script cruza cada evento con la salida de ese script:

1. Estado del camión en cada evento: en escala (y de qué tipo), en movimiento o fuera del periodo.
2. Línea base: si los eventos cayeran al azar en el tiempo, la fracción en escalas sería la fracción
   del tiempo que el camión pasa detenido. La razón observado / esperado mide si los eventos se
   concentran en las paradas (> 1) o no (≈ 1).
3. Qué escalas tienen entregas, por tipo de escala y por rol del sitio.

Uso: primero detect_route_turnarounds.py --client-id C --asset-id ... --out-dir D, luego
     validate_delivery_events.py --client-id C --out-dir D
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from app.analytics_store import get_default_duckdb_path  # noqa: E402

LOCAL_TZ = "America/Mexico_City"
STILL_WINDOW_S = 5 * 60        # ventana ± alrededor del evento
STILL_RADIUS_M = 150.0         # se movió menos que esto en la ventana = detenido (mismo radio que las paradas)
RANDOM_SAMPLES = 3000          # momentos al azar por asset para la línea base
EARTH_RADIUS_M = 6371000.0
DELIVERY_KEYS = ["DELIVERY_STARTED", "ORDER_DELIVERED", "DELIVERY_COMPLETED", "DELIVERY_FAILED", "ORDER_EN_ROUTE"]


def load_events(con: duckdb.DuckDBPyConnection, client_id: int, assets: list[int]) -> pd.DataFrame:
    ev = con.execute(
        f"""
        SELECT assetId, Key AS evento, gpsTimestamp AS ts, latitude AS lat, longitude AS lon
        FROM merged_pos_eventtype
        WHERE clientId = ? AND assetId IN (SELECT unnest(?)) AND Key IN ({", ".join("?" * len(DELIVERY_KEYS))})
        ORDER BY assetId, gpsTimestamp
        """,
        [client_id, assets, *DELIVERY_KEYS],
    ).fetchdf()
    ev["t"] = pd.to_datetime(ev.ts, unit="ms", utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)
    return ev


def haversine_m(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(a))


def load_track(con: duckdb.DuckDBPyConnection, client_id: int, asset_id: int) -> pd.DataFrame:
    return con.execute(
        """
        SELECT gpsTimestamp AS ts, latitude AS lat, longitude AS lon FROM merged_pos_eventtype
        WHERE assetId = ? AND clientId = ? AND latitude IS NOT NULL AND latitude <> 0 AND longitude <> 0
        ORDER BY gpsTimestamp
        """, [asset_id, client_id]).fetchdf()


def still_at(track: pd.DataFrame, times_ms: np.ndarray) -> np.ndarray:
    """1 = detenido, 0 = en movimiento, NaN = sin puntos GPS suficientes alrededor del momento."""
    ts, lat, lon = track.ts.to_numpy(), track.lat.to_numpy(), track.lon.to_numpy()
    lo = np.searchsorted(ts, times_ms - STILL_WINDOW_S * 1000)
    hi = np.searchsorted(ts, times_ms + STILL_WINDOW_S * 1000, side="right")
    out = np.full(len(times_ms), np.nan)
    for k, (a, b) in enumerate(zip(lo, hi)):
        if b - a < 2:
            continue
        la, lo_ = np.median(lat[a:b]), np.median(lon[a:b])
        out[k] = float(haversine_m(la, lo_, lat[a:b], lon[a:b]).max() <= STILL_RADIUS_M)
    return out


def locate(ev: pd.DataFrame, routes: pd.DataFrame, escalas: pd.DataFrame) -> pd.DataFrame:
    """Estado del camión en cada evento según los intervalos de rutas (movimiento) y escalas (detenido)."""
    out = []
    for asset_id, e in ev.groupby("assetId"):
        r = routes[routes.assetId == asset_id]
        s = escalas[escalas.assetId == asset_id]
        r_i, r_f = pd.to_datetime(r.inicio).to_numpy(), pd.to_datetime(r.fin).to_numpy()
        s_i, s_f = pd.to_datetime(s.llegada).to_numpy(), pd.to_datetime(s.salida).to_numpy()
        for row in e.itertuples():
            t = np.datetime64(row.t)
            in_s = np.where((s_i <= t) & (t <= s_f))[0]
            in_r = np.where((r_i <= t) & (t <= r_f))[0]
            if len(in_s):
                esc = s.iloc[in_s[0]]
                estado = "detenido en base" if esc.en_base else "detenido en escala"
                out.append((row.Index, estado, esc.tipo_escala, esc.rol_sitio, esc.viaje_n, esc.ruta_antes))
            elif len(in_r):
                estado = "parada corta en ruta" if row.detenido == 1 else "en ruta, en movimiento" if row.detenido == 0 else "en ruta, sin GPS"
                out.append((row.Index, estado, "", "", r.iloc[in_r[0]].viaje_n, r.iloc[in_r[0]].ruta_n))
            else:
                out.append((row.Index, "fuera de rutas", "", "", pd.NA, pd.NA))
    loc = pd.DataFrame(out, columns=["idx", "estado", "tipo_escala", "rol_sitio", "viaje_n", "ruta"]).set_index("idx")
    return ev.join(loc)


def time_share(routes: pd.DataFrame, escalas: pd.DataFrame) -> pd.DataFrame:
    """Horas en movimiento, en escalas y en bases por asset (línea base para eventos al azar)."""
    mov = routes.groupby("assetId").duracion_h.sum().rename("h_movimiento")
    esc = escalas[~escalas.en_base].groupby("assetId").duracion_h.sum().rename("h_escala")
    base = escalas[escalas.en_base].groupby("assetId").duracion_h.sum().rename("h_base")
    t = pd.concat([mov, esc, base], axis=1).fillna(0)
    total = t.sum(axis=1)
    for c in ("movimiento", "escala", "base"):
        t[f"frac_{c}"] = t[f"h_{c}"] / total
    return t


def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--db", default=str(get_default_duckdb_path()))
    parser.add_argument("--client-id", type=int, required=True)
    parser.add_argument("--out-dir", required=True, help="carpeta con la salida de detect_route_turnarounds.py")
    args = parser.parse_args()
    out = Path(args.out_dir)

    routes = pd.read_csv(out / "rutas_inicio_fin.csv")
    escalas = pd.read_csv(out / "escalas.csv")
    escalas["rol_sitio"] = escalas.rol_sitio.fillna("")
    assets = sorted(routes.assetId.unique().tolist())
    span = routes.groupby("assetId").agg(t0=("inicio", "min"), t1=("fin", "max"))
    rng = np.random.default_rng(0)
    baseline = {}
    with duckdb.connect(args.db, read_only=True) as con:
        ev = load_events(con, args.client_id, assets)
        # Solo el periodo cubierto por las rutas de cada asset.
        ev = ev.join(span, on="assetId")
        ev = ev[(ev.t >= pd.to_datetime(ev.t0)) & (ev.t <= pd.to_datetime(ev.t1))].drop(columns=["t0", "t1"])
        ev["detenido"] = np.nan
        for asset_id in assets:
            track = load_track(con, args.client_id, asset_id)
            m = ev.assetId == asset_id
            ev.loc[m, "detenido"] = still_at(track, ev.loc[m, "ts"].to_numpy())
            t0 = pd.Timestamp(span.loc[asset_id, "t0"]).tz_localize(LOCAL_TZ).value // 10**6
            t1 = pd.Timestamp(span.loc[asset_id, "t1"]).tz_localize(LOCAL_TZ).value // 10**6
            rand = still_at(track, np.sort(rng.integers(t0, t1, RANDOM_SAMPLES)))
            baseline[asset_id] = (np.nanmean(rand), int(np.isfinite(rand).sum()))
    ev = locate(ev, routes, escalas)
    ev.drop(columns=["ts"]).to_csv(out / "eventos_entrega_clasificados.csv", index=False)

    share = time_share(routes, escalas)
    by_key = (ev.groupby(["evento", "estado"]).size().unstack(fill_value=0))
    by_key["total"] = by_key.sum(axis=1)
    for c in [c for c in by_key.columns if c != "total"]:
        by_key[c] = (100 * by_key[c] / by_key.total).round(1)
    by_key = by_key.reset_index()

    # Observado vs esperado por asset: fracción de eventos con el camión detenido fuera de base.
    rows = []
    for asset_id, e in ev[ev.evento.isin(["DELIVERY_STARTED", "ORDER_DELIVERED"])].groupby("assetId"):
        sh = share.loc[asset_id]
        n = len(e)
        obs_mov = (e.estado == "en ruta, en movimiento").mean()
        obs_esc = (e.estado == "detenido en escala").mean()
        obs_base = (e.estado == "detenido en base").mean()
        still = e.detenido.dropna()
        rows.append({
            "assetId": asset_id, "eventos": n,
            "% detenido (GPS)": round(100 * still.mean(), 1) if len(still) else None,
            "% detenido al azar": round(100 * baseline[asset_id][0], 1),
            "% en ruta en movimiento": round(100 * obs_mov, 1), "% tiempo en ruta": round(100 * sh.frac_movimiento, 1),
            "% en escala": round(100 * obs_esc, 1), "% tiempo en escala": round(100 * sh.frac_escala, 1),
            "% en base": round(100 * obs_base, 1), "% tiempo en base": round(100 * sh.frac_base, 1),
            "razón escala obs/esp": round(obs_esc / sh.frac_escala, 2) if sh.frac_escala else None,
        })
    per_asset = pd.DataFrame(rows)

    trip_esc = escalas[escalas.viaje_n.notna() & ~escalas.en_base].copy()
    hits = ev[ev.evento.isin(["DELIVERY_STARTED", "ORDER_DELIVERED"]) & (ev.estado == "detenido en escala")]
    hit_keys = set(zip(hits.assetId, hits.ruta))
    trip_esc["con_entrega"] = [(a, r) in hit_keys for a, r in zip(trip_esc.assetId, trip_esc.ruta_antes)]
    by_type = (trip_esc.groupby("tipo_escala").agg(escalas=("con_entrega", "size"), con_entrega=("con_entrega", "sum"))
               .assign(pct=lambda d: (100 * d.con_entrega / d.escalas).round(1)).reset_index()
               .sort_values("escalas", ascending=False))

    tot = ev[ev.evento.isin(["DELIVERY_STARTED", "ORDER_DELIVERED"])]
    obs_mov = (tot.estado == "en ruta, en movimiento").mean()
    exp_mov = (share.h_movimiento.sum() / share[["h_movimiento", "h_escala", "h_base"]].sum().sum())
    text = f"""# Validación con eventos de entrega

Generado: {datetime.now():%Y-%m-%d %H:%M} · script `backend/scripts/validate_delivery_events.py`
Rutas y escalas: `rutas_inicio_fin.csv`, `escalas.csv` (detect_route_turnarounds.py) · eventos: `eventos_entrega_clasificados.csv`

Cliente {args.client_id} · assets: {', '.join(map(str, assets))} · eventos en el periodo: {len(ev):,}

## ¿Las entregas ocurren con el camión detenido?

Si los eventos fueran reales, casi ninguno debería caer **en movimiento**. Si cayeran al azar en el tiempo,
el % en movimiento sería parecido al % del tiempo que el camión se mueve.

- Prueba directa con el GPS (no depende de cómo se cortaron las rutas): en ±{STILL_WINDOW_S // 60} min alrededor del evento, ¿el
  camión se movió menos de {STILL_RADIUS_M:.0f} m? Eventos DELIVERY_STARTED + ORDER_DELIVERED con camión **detenido: {100 * tot.detenido.mean():.1f}%**
  (con GPS suficiente: {int(tot.detenido.notna().sum()):,} de {len(tot):,}). En {RANDOM_SAMPLES:,} momentos al azar por asset: **{100 * np.mean([b[0] for b in baseline.values()]):.1f}%** detenido.
- Con las rutas detectadas: {100 * obs_mov:.1f}% de los eventos caen dentro de una ruta con el camión en movimiento; el camión
  pasa en ruta el {100 * exp_mov:.1f}% del tiempo (las rutas incluyen paradas cortas de menos de 2 h).

Por tipo de evento (% de cada evento por estado del camión):

{md_table(by_key)}

## Por asset (DELIVERY_STARTED + ORDER_DELIVERED)

`razón escala obs/esp` > 1: los eventos se concentran en las escalas más de lo que daría el azar.

{md_table(per_asset)}

## Escalas dentro de viajes con al menos una entrega

{md_table(by_type)}

## Lectura

- Si el % detenido de los eventos es parecido al % detenido al azar y la razón obs/esp ≈ 1, los eventos
  de entrega no siguen las paradas del camión y **no sirven como verdad de terreno**.
- Si el % detenido es mucho mayor que al azar y la razón > 1, los eventos confirman las escalas detectadas y
  sirven para medir qué tan bien se clasifican.
"""
    (out / "validacion_entregas.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
