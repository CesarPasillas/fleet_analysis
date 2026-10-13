#!/usr/bin/env python3
"""
Detección de inicio/fin de rutas con corte en el punto de retorno.

Para cada asset de data/asset_id presente en el snapshot DuckDB (sección 2):

1. Limpia la trayectoria GPS (coordenadas nulas/0 y saltos imposibles).
2. Detecta paradas (vehículo dentro de un radio pequeño durante un tiempo mínimo).
   Las paradas largas y los huecos de datos son cortes "duros" entre bloques.
3. Dentro de cada bloque busca el punto donde los puntos empiezan a regresar:
   la trayectoria vuelve a pasar cerca de puntos ya recorridos, en sentido
   inverso, durante varios km. Ese punto (ápice) termina la ruta de ida y
   empieza la de regreso. Si hay una parada cerca del ápice, la ida termina al
   llegar a la parada y el regreso empieza al salir de ella.
4. Empata cada punto con la red de carreteras (carreteras_raw) para confirmar
   el retorno (el kilometraje sobre la misma carretera cambia de sentido) y para
   reportar las carreteras principales de cada ruta.

Salida: CSV con una fila por ruta, CSV de sitios (inicios/fines agrupados) y un
resumen en Markdown.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

import duckdb
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from scipy.stats import spearmanr
from sklearn.cluster import DBSCAN

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from app.analytics_store import get_default_duckdb_path  # noqa: E402
from app.route_detection import _parse_linestring_wkt  # noqa: E402

EARTH_RADIUS_M = 6371000.0
LOCAL_TZ = "America/Mexico_City"

# Parámetros (ver resumen .md)
MAX_SPEED_KMH = 180.0          # saltos más rápidos que esto se descartan como ruido GPS
STOP_RADIUS_M = 150.0          # radio para considerar al vehículo detenido
MIN_STOP_S = 5 * 60            # parada mínima
LONG_STOP_S = 2 * 3600         # parada larga = corte duro entre rutas
MAX_GAP_S = 3 * 3600           # hueco de datos = corte duro
RESAMPLE_M = 250.0             # remuestreo por distancia recorrida
RETRACE_RADIUS_M = 300.0       # cercanía para considerar que se repasa un punto previo
MIN_RETRACE_M = 3000.0         # longitud mínima del repaso en sentido inverso
MIN_LEG_M = 2000.0             # longitud mínima de una ruta
APEX_STOP_WINDOW_M = 2000.0    # parada a esta distancia del ápice se toma como destino
ROAD_MAX_DIST_M = 300.0        # distancia máxima punto-carretera
ROAD_DENSIFY_M = 100.0
SITE_EPS_M = 1500.0            # radio para agrupar inicios/fines en sitios
LOCAL_MAX_KM = 3.0             # trayecto que nunca se aleja más de esto del inicio = movimiento local
MIN_REVERSE_OVERLAP = 0.5      # fracción mínima repasada al revés para un regreso tras parada larga
MAP_STEP_M = 300.0             # espaciado de los trazos en el mapa HTML
REVERSE_LOOKBACK = 2           # trayectos significativos anteriores donde buscar la ida de un regreso tras parada
BASE_MIN_SHARE = 0.10          # sitio con >= esta fracción de los inicios/fines del asset = base
                               # (0.15 dejaba fuera terminales de operación multi-base, p. ej. asset 2894)
BASE_MIN_VISITS = 3
PERNOCTA_MIN_NIGHTS = 3        # base propia con >= 3 estancias nocturnas ...
PERNOCTA_MIN_SHARE = 0.6       # ... que son >= 60% de sus estancias = pernocta (pensión)
HUB_MIN_ASSETS = 5             # sitio donde >= 5 assets distintos se quedan ...
HUB_MIN_DWELL_S = 4 * 3600     # ... >= 4 h = centro de flota
PRE_LEG_MAX_S = 24 * 3600      # carga → pernocta → salida a repartir dentro de este tiempo = mismo viaje
TRASLADO_MAX_KM = 30.0         # una ruta base → base de hasta esta distancia = traslado, no viaje
ESCALA_POI_MIN_S = 15 * 60     # paradas dentro de una escala que se consultan en OSM
NIGHT_HOUR = 2                 # escala que incluye las 02:00 ...
NIGHT_MIN_S = 3 * 3600         # ... y dura entre 3 h ...
NIGHT_MAX_S = 24 * 3600        # ... y 24 h = descanso nocturno (más larga: espera de carga, fin de semana)


# ---------------------------------------------------------------- geometría

def to_ecef(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    la, lo = np.radians(lat), np.radians(lon)
    return EARTH_RADIUS_M * np.column_stack((np.cos(la) * np.cos(lo), np.cos(la) * np.sin(lo), np.sin(la)))


def haversine_m(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(a))


# ---------------------------------------------------------------- carreteras

class RoadIndex:
    """Vértices de carreteras densificados en un KD-tree 3D, con kilometraje."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        rows = connection.execute(
            "SELECT coalesce(name, '') AS rname, coalesce(p_route, '') AS rroute, coalesce(CAST(p_no AS VARCHAR), '') AS rno, geometry "
            "FROM carreteras_raw WHERE geometry IS NOT NULL"
        ).fetchall()
        lats, lons, road_ids, chain = [], [], [], []
        self.labels: list[str] = []
        for name, route, no, wkt in rows:
            poly = _parse_linestring_wkt(wkt)
            if len(poly) < 2:
                continue
            road_id = len(self.labels)
            label = (name or route or no or "carretera_sin_nombre").strip()
            self.labels.append(f"{label} ({no})" if no and no != "nan" else label)
            p = np.array(poly)
            seg = haversine_m(p[:-1, 0], p[:-1, 1], p[1:, 0], p[1:, 1])
            cum = np.concatenate(([0.0], np.cumsum(seg)))
            for k in range(len(seg)):
                n = max(1, int(np.ceil(seg[k] / ROAD_DENSIFY_M)))
                t = np.arange(n) / n
                lats.append(p[k, 0] + t * (p[k + 1, 0] - p[k, 0]))
                lons.append(p[k, 1] + t * (p[k + 1, 1] - p[k, 1]))
                chain.append(cum[k] + t * seg[k])
                road_ids.append(np.full(n, road_id))
            lats.append(p[-1:, 0]); lons.append(p[-1:, 1]); chain.append(cum[-1:]); road_ids.append([road_id])
        self.road_id = np.concatenate(road_ids).astype(int)
        self.chainage = np.concatenate(chain)
        self.tree = cKDTree(to_ecef(np.concatenate(lats), np.concatenate(lons)))

    def match(self, lat: np.ndarray, lon: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        dist, idx = self.tree.query(to_ecef(lat, lon), distance_upper_bound=ROAD_MAX_DIST_M)
        ok = np.isfinite(dist)
        road = np.full(len(lat), -1)
        ch = np.full(len(lat), np.nan)
        road[ok] = self.road_id[idx[ok]]
        ch[ok] = self.chainage[idx[ok]]
        return road, ch


# ---------------------------------------------------------------- datos

def load_track(connection: duckdb.DuckDBPyConnection, asset_id: int, client_id: int) -> pd.DataFrame:
    df = connection.execute(
        """
        SELECT gpsTimestamp AS ts, latitude AS lat, longitude AS lon, Key AS ev
        FROM merged_pos_eventtype
        WHERE assetId = ? AND clientId = ?
          AND latitude IS NOT NULL AND longitude IS NOT NULL
          AND latitude <> 0 AND longitude <> 0 AND gpsTimestamp IS NOT NULL
        ORDER BY gpsTimestamp, serverTimestamp, latitude, longitude
        """,
        [asset_id, client_id],
    ).fetchdf()
    # Orden total (con desempate) para que el registro que se conserva en timestamps repetidos sea siempre el mismo.
    df = df.drop_duplicates("ts").reset_index(drop=True)
    return drop_jumps(df)


def drop_jumps(df: pd.DataFrame) -> pd.DataFrame:
    """Quita puntos que implican velocidades imposibles respecto al último punto válido."""
    if df.empty:
        return df
    ts, lat, lon = df.ts.to_numpy(), df.lat.to_numpy(), df.lon.to_numpy()
    keep = np.zeros(len(df), dtype=bool)
    keep[0] = True
    last = 0
    for i in range(1, len(df)):
        d = haversine_m(lat[last], lon[last], lat[i], lon[i])
        dt = max((ts[i] - ts[last]) / 1000.0, 1.0)
        if d < 1000 or d / dt * 3.6 <= MAX_SPEED_KMH:
            keep[i] = True
            last = i
    return df[keep].reset_index(drop=True)


def detect_stops(df: pd.DataFrame) -> list[tuple[int, int]]:
    """Paradas como (índice_inicio, índice_fin): puntos dentro de STOP_RADIUS_M del ancla por >= MIN_STOP_S."""
    ts, lat, lon = df.ts.to_numpy(), df.lat.to_numpy(), df.lon.to_numpy()
    stops, i, n = [], 0, len(df)
    while i < n:
        j = i
        while j + 1 < n and haversine_m(lat[i], lon[i], lat[j + 1], lon[j + 1]) <= STOP_RADIUS_M:
            j += 1
        if (ts[j] - ts[i]) / 1000.0 >= MIN_STOP_S:
            stops.append((i, j))
            i = j + 1
        else:
            i += 1
    return stops


# ---------------------------------------------------------------- retorno

class Sample(NamedTuple):
    s: np.ndarray       # distancia recorrida (m)
    xyz: np.ndarray     # posición interpolada (ECEF)
    idx: np.ndarray     # índice del punto GPS original que corresponde a cada muestra
    road: np.ndarray    # carretera empatada (-1 = ninguna)
    ch: np.ndarray      # kilometraje sobre esa carretera (m)


def resample(lat, lon, cum, roads: RoadIndex, a: int, b: int) -> Sample:
    """
    Posiciones interpoladas cada RESAMPLE_M a lo largo del recorrido [a, b]. En carretera
    los reportes llegan cada 1-4 min (varios km); interpolar evita comparar puntos dispersos.
    """
    seg = slice(a, b + 1)
    s = np.append(np.arange(cum[a], cum[b], RESAMPLE_M), cum[b])
    la = np.interp(s, cum[seg], lat[seg])
    lo = np.interp(s, cum[seg], lon[seg])
    idx = np.minimum(a + np.searchsorted(cum[seg], s), b)
    road, ch = roads.match(la, lo)
    return Sample(s, to_ecef(la, lo), idx, road, ch)


def find_turnaround(xyz: np.ndarray, s: np.ndarray) -> int | None:
    """
    Primer ápice de retorno en una secuencia remuestreada (xyz, distancia recorrida s).
    Un punto j "repasa" al punto previo i si están a < RETRACE_RADIUS_M y entre ambos se
    recorrió mucho más que su separación. Una racha de repasos con i decreciente
    (se recorre el camino previo al revés) de >= MIN_RETRACE_M indica retorno.
    """
    n = len(s)
    if n < 4:
        return None
    tree = cKDTree(xyz)
    match = np.full(n, -1)
    for j, cands in enumerate(tree.query_ball_point(xyz, RETRACE_RADIUS_M)):
        cands = [i for i in cands if i < j]
        if not cands:
            continue
        cands = np.array(cands)
        d = np.linalg.norm(xyz[cands] - xyz[j], axis=1)
        ok = (s[j] - s[cands]) > 3 * d + 1000.0
        if ok.any():
            match[j] = cands[ok][np.argmin(d[ok])]

    run_start, prev_j, misses = None, None, 0
    for j in range(n):
        if match[j] < 0:
            if run_start is not None:
                misses += 1
                if misses > 4:
                    run_start, prev_j, misses = None, None, 0
            continue
        if run_start is not None and match[j] <= match[prev_j] + 2:
            prev_j, misses = j, 0
        else:
            run_start, prev_j, misses = j, j, 0
        if s[prev_j] - s[run_start] >= MIN_RETRACE_M:
            i0 = match[run_start]
            span = np.arange(i0, run_start + 1)
            apex = span[np.argmax(np.linalg.norm(xyz[span] - xyz[i0], axis=1))]
            return int(apex)
    return None


def road_confirms(road: np.ndarray, ch: np.ndarray, s: np.ndarray, apex: int, labels: list[str]) -> str:
    """'carretera: X' si sobre la misma carretera el kilometraje avanza antes del ápice y retrocede después."""
    before = (s >= s[apex] - 5000) & (np.arange(len(s)) <= apex)
    after = (s <= s[apex] + 5000) & (np.arange(len(s)) >= apex)
    for rid in set(road[before]) & set(road[after]):
        if rid < 0:
            continue
        b, a = before & (road == rid), after & (road == rid)
        if b.sum() < 3 or a.sum() < 3:
            continue
        trend_b = ch[b][-1] - ch[b][0]
        trend_a = ch[a][-1] - ch[a][0]
        if abs(trend_b) >= 1000 and abs(trend_a) >= 1000 and np.sign(trend_b) != np.sign(trend_a):
            return f"carretera: {labels[rid]}"
    return "geométrico"


# ---------------------------------------------------------------- rutas

def analyze_asset(df: pd.DataFrame, roads: RoadIndex, asset_id: int, client_id: int) -> list[dict]:
    if len(df) < 10:
        return [], []
    ts, lat, lon = df.ts.to_numpy(), df.lat.to_numpy(), df.lon.to_numpy()
    step = np.concatenate(([0.0], haversine_m(lat[:-1], lon[:-1], lat[1:], lon[1:])))
    cum = np.cumsum(step)
    stops = detect_stops(df)

    # Cortes duros: paradas largas y huecos de datos. Bloque = tramo de movimiento entre cortes.
    cuts: list[tuple[int, int, str]] = []
    for a, b in stops:
        dur = (ts[b] - ts[a]) / 1000.0
        if dur >= LONG_STOP_S:
            cuts.append((a, b, f"parada larga {dur / 3600:.1f} h"))
    for i in np.where(np.diff(ts) / 1000.0 >= MAX_GAP_S)[0]:
        if not any(a <= i < b for a, b, _ in cuts):
            cuts.append((int(i), int(i) + 1, f"hueco de datos {(ts[i + 1] - ts[i]) / 3.6e6:.1f} h"))
    cuts.sort()

    blocks, cursor, reason = [], 0, "inicio de datos"
    for a, b, why in cuts:
        if a > cursor:
            blocks.append((cursor, a, reason, why))
        cursor, reason = max(cursor, b), why
    if cursor < len(df) - 1:
        blocks.append((cursor, len(df) - 1, reason, "fin de datos"))

    short_stops = [(a, b) for a, b in stops if (ts[b] - ts[a]) / 1000.0 < LONG_STOP_S]

    def route(i, j, start_reason, end_reason, leg_type, method, dwell):
        return make_route(df, cum, resample(lat, lon, cum, roads, i, j), roads.labels,
                          i, j, start_reason, end_reason, leg_type, method, dwell)

    routes: list[dict] = []
    for a, b, start_reason, end_reason in blocks:
        if cum[b] - cum[a] < MIN_LEG_M:
            continue
        # Partir el bloque en el primer retorno; repetir con lo que queda.
        seg_start, seg_reason, leg_type, pending_dwell = a, start_reason, None, None
        while True:
            smp = resample(lat, lon, cum, roads, seg_start, b)
            apex_r = find_turnaround(smp.xyz, smp.s)
            if apex_r is None or smp.s[apex_r] - cum[seg_start] < MIN_LEG_M or cum[b] - smp.s[apex_r] < MIN_LEG_M:
                routes.append(route(seg_start, b, seg_reason, end_reason, leg_type or "trayecto", "", pending_dwell))
                break
            apex = int(smp.idx[apex_r])
            method = road_confirms(smp.road, smp.ch, smp.s, apex_r, roads.labels)
            end_i, next_start, dwell = apex, apex, None
            near = [(sa, sb) for sa, sb in short_stops
                    if sa > seg_start and sb < b and abs(cum[sa] - cum[apex]) <= APEX_STOP_WINDOW_M]
            if near:
                sa, sb = min(near, key=lambda st: abs(cum[st[0]] - cum[apex]))
                end_i, next_start, dwell = sa, sb, (ts[sb] - ts[sa]) / 60000.0
            routes.append(route(seg_start, end_i, seg_reason, "retorno detectado", leg_type or "ida", method,
                                pending_dwell))
            seg_start, seg_reason, leg_type, pending_dwell = next_start, "retorno detectado", "regreso", dwell
            routes[-1]["parada_destino_min"] = round(dwell, 1) if dwell else ""

    for k, r in enumerate(routes, 1):
        r.update(assetId=asset_id, clientId=client_id, ruta_n=k)
        if r["tipo"] == "trayecto" and r["max_alejamiento_km"] < LOCAL_MAX_KM:
            r["tipo"] = "local"
        if r["tipo"] == "regreso":
            r["ruta_ida_n"] = k - 1

    # Regreso después de una parada larga en el destino: un trayecto que repasa al revés
    # uno de los REVERSE_LOOKBACK trayectos significativos anteriores (se ignoran los
    # movimientos locales; un tramo corto en el destino puede quedar entre ida y regreso).
    recent: list[dict] = []
    for cur in routes:
        if cur["tipo"] == "local":
            continue
        if cur["tipo"] == "trayecto":
            for prev in reversed(recent):
                if prev["tipo"] != "trayecto":
                    break
                frac = reversed_overlap(lat, lon, cum, roads, prev, cur)
                if frac is not None:
                    cur["tipo"], prev["tipo"] = "regreso", "ida"
                    cur["ruta_ida_n"] = prev["ruta_n"]
                    cur["retorno_metodo"] = f"tras parada: repasa {frac:.0%} de la ruta {prev['ruta_n']} al revés"
                    break
        recent = (recent + [cur])[-REVERSE_LOOKBACK:]

    # Un regreso que termina cerca del inicio de su ida = vuelta completa.
    by_n = {r["ruta_n"]: r for r in routes}
    for cur in routes:
        if cur["tipo"] == "regreso":
            ida = by_n[cur["ruta_ida_n"]]
            d = haversine_m(ida["inicio_lat"], ida["inicio_lon"], cur["fin_lat"], cur["fin_lon"])
            cur["regresa_a_origen"] = bool(d <= 5000)
            cur["dist_fin_a_origen_ida_km"] = round(d / 1000, 1)
    return routes, collect_escalas(df, routes)


def engine_off_pct(ts: np.ndarray, ev: np.ndarray) -> float | None:
    """% del tiempo con motor apagado (de IGNITION_OFF al siguiente IGNITION_ON); None si no hay eventos de motor."""
    if len(ts) < 2 or not any(k in ("IGNITION_OFF", "IGNITION_ON") for k in ev):
        return None
    off_since, off = None, 0.0
    for t, k in zip(ts, ev):
        if k == "IGNITION_OFF" and off_since is None:
            off_since = t
        elif k == "IGNITION_ON" and off_since is not None:
            off, off_since = off + t - off_since, None
    if off_since is not None:
        off += ts[-1] - off_since
    return round(100 * off / (ts[-1] - ts[0]))


def collect_escalas(df: pd.DataFrame, routes: list[dict]) -> list[dict]:
    """
    Escala = lo que pasa entre el fin de una ruta y el inicio de la siguiente. Puede contener
    varias paradas (p. ej. espera en la carretera y luego 4 h en el cliente, con un movimiento
    corto en medio), así que se guardan sus paradas internas para consultarlas en OSM.
    """
    ts, lat, lon = df.ts.to_numpy(), df.lat.to_numpy(), df.lon.to_numpy()
    ev = df.ev.to_numpy() if "ev" in df else np.full(len(df), None)
    escalas = []
    for prev, nxt in zip(routes, routes[1:]):
        a, b = prev["_j"], nxt["_i"]
        if b <= a:
            continue  # retorno sin parada: la ruta siguiente empieza en el mismo punto
        sub = df.iloc[a:b + 1].reset_index(drop=True)
        paradas = []
        for sa, sb in detect_stops(sub):
            dur = (sub.ts[sb] - sub.ts[sa]) / 1000.0
            if dur < ESCALA_POI_MIN_S:
                continue
            paradas.append({
                "lat": float(np.median(sub.lat[sa:sb + 1])), "lon": float(np.median(sub.lon[sa:sb + 1])),
                "min": dur / 60.0, "apagado_pct": engine_off_pct(sub.ts.to_numpy()[sa:sb + 1], sub.ev.to_numpy()[sa:sb + 1]),
            })
        if not paradas:  # hueco de datos o parada sin puntos suficientes: se usa el punto de llegada
            paradas.append({"lat": float(lat[a]), "lon": float(lon[a]), "min": (ts[b] - ts[a]) / 60000.0,
                            "apagado_pct": engine_off_pct(ts[a:b + 1], ev[a:b + 1])})
        escalas.append({
            "ruta_antes": prev["ruta_n"], "ruta_despues": nxt["ruta_n"],
            "llegada": prev["fin"], "salida": nxt["inicio"],
            "duracion_h": round((ts[b] - ts[a]) / 3.6e6, 2),
            "lat": round(float(lat[a]), 6), "lon": round(float(lon[a]), 6),
            "motor_apagado_pct": engine_off_pct(ts[a:b + 1], ev[a:b + 1]),
            "hueco_datos": prev["fin_motivo"].startswith("hueco"),
            "_paradas": paradas,
        })
    return escalas


def reversed_overlap(lat, lon, cum, roads, prev: dict, cur: dict) -> float | None:
    """Fracción de `cur` que recorre el camino de `prev` en sentido inverso (None si no es un regreso)."""
    p = resample(lat, lon, cum, roads, prev["_i"], prev["_j"])
    c = resample(lat, lon, cum, roads, cur["_i"], cur["_j"])
    if len(p.s) < 8 or len(c.s) < 8:
        return None
    dist, near = cKDTree(p.xyz).query(c.xyz, distance_upper_bound=RETRACE_RADIUS_M)
    ok = np.isfinite(dist)
    frac = ok.mean()
    if frac < MIN_REVERSE_OVERLAP or ok.sum() < 8:
        return None
    rho = spearmanr(np.arange(len(c.s))[ok], near[ok]).statistic
    return float(frac) if rho <= -0.5 else None


def make_route(df, cum, smp: Sample, labels, i, j, start_reason, end_reason, leg_type, method, start_dwell) -> dict:
    ts = df.ts.to_numpy()
    lat, lon = df.lat.to_numpy(), df.lon.to_numpy()
    rc = Counter(int(r) for r in smp.road if r >= 0)
    main_roads = "; ".join(f"{labels[r]} ~{c * RESAMPLE_M / 1000:.0f} km" for r, c in rc.most_common(3))
    far = haversine_m(lat[i], lon[i], lat[i:j + 1], lon[i:j + 1])
    t0 = pd.Timestamp(ts[i], unit="ms", tz="UTC").tz_convert(LOCAL_TZ)
    t1 = pd.Timestamp(ts[j], unit="ms", tz="UTC").tz_convert(LOCAL_TZ)
    return {
        "tipo": leg_type,
        "inicio": t0.strftime("%Y-%m-%d %H:%M"),
        "inicio_lat": round(float(lat[i]), 6),
        "inicio_lon": round(float(lon[i]), 6),
        "fin": t1.strftime("%Y-%m-%d %H:%M"),
        "fin_lat": round(float(lat[j]), 6),
        "fin_lon": round(float(lon[j]), 6),
        "duracion_h": round((ts[j] - ts[i]) / 3.6e6, 2),
        "distancia_km": round((cum[j] - cum[i]) / 1000, 1),
        "linea_recta_km": round(float(far[-1]) / 1000, 1),
        "max_alejamiento_km": round(float(far.max()) / 1000, 1),
        "inicio_motivo": start_reason,
        "fin_motivo": end_reason,
        "retorno_metodo": method,
        "parada_origen_min": round(start_dwell, 1) if start_dwell else "",
        "parada_destino_min": "",
        "pct_en_carretera": round(100 * np.mean(smp.road >= 0), 1),
        "carreteras_principales": main_roads,
        "puntos_gps": int(j - i + 1),
        "regresa_a_origen": "",
        "dist_fin_a_origen_ida_km": "",
        "ruta_ida_n": "",
        "_i": int(i),
        "_j": int(j),
        "_path": thin_path(lat, lon, cum, i, j),
    }


def thin_path(lat, lon, cum, i: int, j: int) -> list[list[float]]:
    """Puntos GPS originales de la ruta, uno cada ~MAP_STEP_M recorridos (para el mapa)."""
    marks = np.floor(cum[i:j + 1] / MAP_STEP_M)
    keep = np.concatenate(([True], marks[1:] != marks[:-1]))
    keep[-1] = True
    return np.round(np.column_stack((lat[i:j + 1][keep], lon[i:j + 1][keep])), 5).tolist()


def assign_sites(routes: pd.DataFrame) -> pd.DataFrame:
    pts = np.vstack([routes[["inicio_lat", "inicio_lon"]].to_numpy(), routes[["fin_lat", "fin_lon"]].to_numpy()])
    labels = DBSCAN(eps=SITE_EPS_M / EARTH_RADIUS_M, min_samples=1, metric="haversine").fit_predict(np.radians(pts))
    order = {lab: k for k, (lab, _) in enumerate(Counter(labels).most_common(), 1)}
    names = np.array([f"S{order[l]:03d}" for l in labels])
    n = len(routes)
    routes["sitio_inicio"], routes["sitio_fin"] = names[:n], names[n:]
    sites = pd.DataFrame({"sitio": names, "lat": pts[:, 0], "lon": pts[:, 1]})
    sites = sites.groupby("sitio").agg(lat=("lat", "mean"), lon=("lon", "mean"), visitas=("lat", "size")).reset_index()
    return sites.sort_values("visitas", ascending=False).round({"lat": 5, "lon": 5})


# ---------------------------------------------------------------- viajes y escalas

def gmaps_url(lat, lon) -> str:
    """Enlace a Google Maps con un pin en el punto (para ver qué hay exactamente ahí)."""
    if pd.isna(lat) or pd.isna(lon):
        return ""
    return f"https://www.google.com/maps/search/?api=1&query={float(lat):.6f},{float(lon):.6f}"


def is_overnight(llegada: str, salida: str, duracion_h: float) -> bool:
    """La estancia incluye las NIGHT_HOUR:00 y dura entre NIGHT_MIN_S y NIGHT_MAX_S."""
    t0, t1 = pd.Timestamp(llegada), pd.Timestamp(salida)
    night = t0.normalize() + pd.Timedelta(hours=NIGHT_HOUR)
    if night < t0:
        night += pd.Timedelta(days=1)
    return bool(night <= t1 and NIGHT_MIN_S <= duracion_h * 3600 <= NIGHT_MAX_S)


def site_roles(routes: pd.DataFrame, escalas: pd.DataFrame) -> pd.DataFrame:
    """
    Rol de cada sitio para cada asset, inferido del comportamiento (sin catálogo):
    - base propia: sitio con >= BASE_MIN_SHARE de los inicios/fines del asset (y >= BASE_MIN_VISITS).
    - pernocta: la base propia con más estancias nocturnas, si son >= PERNOCTA_MIN_NIGHTS y
      >= PERNOCTA_MIN_SHARE de sus estancias (p. ej. la Pensión La Llave del asset 2728: 9 de 10).
      Las demás bases propias son "carga" (el camión pasa ahí el día; de ahí salen los viajes).
      Sin pernocta clara, las bases propias quedan como "base".
    - centro de flota: sitio donde >= HUB_MIN_ASSETS assets distintos se quedan >= HUB_MIN_DWELL_S;
      cuenta como base del asset si el asset se quedó ahí ese tiempo al menos una vez.
    """
    ends = pd.concat([routes[["assetId", "sitio_inicio"]].rename(columns={"sitio_inicio": "sitio"}),
                      routes[["assetId", "sitio_fin"]].rename(columns={"sitio_fin": "sitio"})])
    visits = ends.value_counts().rename("visitas").reset_index()
    visits["fraccion"] = visits.visitas / visits.groupby("assetId").visitas.transform("sum")
    dw = escalas.assign(nocturna=[is_overnight(a, b, h) for a, b, h in zip(escalas.llegada, escalas.salida, escalas.duracion_h)])
    stays = dw.groupby(["assetId", "sitio"]).agg(estancias=("nocturna", "size"), nocturnas=("nocturna", "sum"),
                                                 horas_mediana=("duracion_h", "median")).reset_index()
    long_stay = dw[dw.duracion_h * 3600 >= HUB_MIN_DWELL_S]
    hub_assets = long_stay.groupby("sitio").assetId.nunique().rename("assets_con_estancia_larga")
    t = (visits.merge(stays, on=["assetId", "sitio"], how="left")
         .merge(hub_assets, on="sitio", how="left").fillna({"estancias": 0, "nocturnas": 0, "assets_con_estancia_larga": 0}))
    own = (t.fraccion >= BASE_MIN_SHARE) & (t.visitas >= BASE_MIN_VISITS)
    stayed_long = t.set_index(["assetId", "sitio"]).index.isin(long_stay.set_index(["assetId", "sitio"]).index)
    t["rol"] = ""
    t.loc[own, "rol"] = "base"
    for asset_id, g in t[own].groupby("assetId"):
        cand = g[(g.nocturnas >= PERNOCTA_MIN_NIGHTS) & (g.nocturnas >= PERNOCTA_MIN_SHARE * g.estancias)]
        if len(cand):
            t.loc[g.index, "rol"] = "carga"
            t.loc[cand.nocturnas.idxmax(), "rol"] = "pernocta"
    t.loc[(t.rol == "") & (t.assets_con_estancia_larga >= HUB_MIN_ASSETS) & stayed_long, "rol"] = "centro de flota"
    t = t[t.rol != ""].copy()
    t["fraccion"] = t.fraccion.round(3)
    t["horas_mediana"] = t.horas_mediana.round(1)
    return t.sort_values(["assetId", "visitas"], ascending=[True, False]).reset_index(drop=True)


def build_trips(routes: pd.DataFrame, escalas: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Viaje = sale de una base y termina al llegar a una base (o al acabar los datos), con las rutas de en medio.
    Con pernocta identificada (ver site_roles):
    - carga → pernocta seguido, tras <= PRE_LEG_MAX_S, de una salida de la pernocta a un sitio que no es base
      = el camión cargó, durmió en la pensión y salió a repartir: esa ruta abre el viaje y la noche en la
      pensión es una escala ("pernocta en base") en vez de su fin (asset 2728, rutas 26–29).
    - Una sola ruta base → base de <= TRASLADO_MAX_KM es un traslado (p. ej. pensión → centro de carga), no un viaje.
    """
    sitio_de = {(a, n): s for a, n, s in zip(routes.assetId, routes.ruta_n, routes.sitio_fin)}
    escalas["sitio"] = [sitio_de.get((a, n)) for a, n in zip(escalas.assetId, escalas.ruta_antes)]
    roles = site_roles(routes, escalas)
    role_of = {(a, s): r for a, s, r in zip(roles.assetId, roles.sitio, roles.rol)}
    routes["viaje_n"] = pd.array([pd.NA] * len(routes), dtype="Int64")
    routes["traslado"] = False
    escalas["viaje_n"] = pd.array([pd.NA] * len(escalas), dtype="Int64")
    escalas["en_base"] = False
    escalas["rol_sitio"] = [role_of.get((a, s), "") for a, s in zip(escalas.assetId, escalas.sitio)]
    trips = []
    for asset_id, r in routes.groupby("assetId", sort=True):
        role = {s: rl for (a, s), rl in role_of.items() if a == asset_id}
        bases = set(role)
        has_pernocta = "pernocta" in role.values()
        rows = list(r.itertuples())
        cur: list = []

        def close(open_end: bool) -> None:
            t = r.loc[cur]
            first, last = t.iloc[0], t.iloc[-1]
            d = haversine_m(first.inicio_lat, first.inicio_lon, t.fin_lat.to_numpy(), t.fin_lon.to_numpy())
            far = t.iloc[int(np.argmax(d))]
            n = len(trips) + 1
            trips.append({
                "viaje_n": n, "assetId": asset_id, "clientId": first.clientId,
                "rutas": f"{first.ruta_n}–{last.ruta_n}" if len(t) > 1 else str(first.ruta_n),
                "tramos": len(t), "inicio": first.inicio, "fin": last.fin,
                "sitio_origen": first.sitio_inicio, "rol_origen": role.get(first.sitio_inicio, ""),
                "sitio_destino": far.sitio_fin, "sitio_fin": last.sitio_fin, "rol_fin": role.get(last.sitio_fin, ""),
                "destino_lat": far.fin_lat, "destino_lon": far.fin_lon,
                "max_alejamiento_km": round(float(d.max()) / 1000, 1),
                "km": round(t.distancia_km.sum(), 1),
                "horas_total": round((pd.Timestamp(last.fin) - pd.Timestamp(first.inicio)).total_seconds() / 3600, 1),
                "horas_manejo": round(t.duracion_h.sum(), 1),
                "regresa_a_base": last.sitio_fin in bases and not open_end,
                "origen_en_base": first.sitio_inicio in bases,
                "bases_asset": ", ".join(f"{s} ({rl})" for s, rl in sorted(role.items())),
                "base_clara": any(rl in ("base", "carga", "pernocta") for rl in role.values()),
            })
            routes.loc[cur, "viaje_n"] = n
            inside = (escalas.assetId == asset_id) & escalas.ruta_antes.isin(t.ruta_n) & escalas.ruta_despues.isin(t.ruta_n)
            escalas.loc[inside, "viaje_n"] = n
            cur.clear()

        def pre_leg(k: int) -> bool:
            row = rows[k]
            if not has_pernocta or k + 1 >= len(rows):
                return False
            nxt = rows[k + 1]
            wait = (pd.Timestamp(nxt.inicio) - pd.Timestamp(row.fin)).total_seconds()
            return (role.get(row.sitio_inicio) in ("carga", "centro de flota") and role.get(row.sitio_fin) == "pernocta"
                    and nxt.sitio_inicio == row.sitio_fin and nxt.sitio_fin not in bases and wait <= PRE_LEG_MAX_S)

        for k, row in enumerate(rows):
            if not cur and row.sitio_inicio in bases and row.sitio_fin in bases:
                if pre_leg(k):
                    cur.append(row.Index)
                    continue
                if row.tipo == "local" or row.distancia_km <= TRASLADO_MAX_KM:
                    routes.loc[row.Index, "traslado"] = True
                    continue
            cur.append(row.Index)
            if row.sitio_fin in bases:
                close(open_end=False)
        if cur:
            close(open_end=True)
        at_base = (escalas.assetId == asset_id) & escalas.sitio.isin(bases)
        escalas.loc[at_base & escalas.viaje_n.isna(), "en_base"] = True
    return pd.DataFrame(trips), escalas, roles


def classify_escala(e: dict, pois: list[tuple[str | None, str | None]]) -> str:
    """Tipo de escala por reglas simples (ver resumen). Es una lectura probable, no una etiqueta confirmada."""
    if e["en_base"]:
        return "base"
    if e["hueco_datos"]:
        return "hueco de datos"
    if e.get("rol_sitio") == "pernocta":
        return "pernocta en base"
    if is_overnight(e["llegada"], e["salida"], e["duracion_h"]):
        return "descanso nocturno"
    cats = {c for c, _ in pois if c}
    if "terminal_carga" in cats:
        return "carga/descarga"
    if cats & {"hotel", "tienda_conveniencia", "farmacia", "hospital"}:
        return "entrega probable"
    if "gasolinera" in cats:
        return "combustible/descanso"
    if "restaurante" in cats:
        return "alimentos"
    if "taller_mecanico" in cats:
        return "taller"
    return "sin identificar"


def enrich_escalas(escalas: pd.DataFrame, use_poi: bool) -> pd.DataFrame:
    """POIs de OSM (stop_detection) en cada parada interna de la escala, y tipo de escala."""
    if use_poi:
        from app.stop_detection import _classify_geo, _classify_via_nominatim
    poi_txt, place_txt, tipos, main_lat, main_lon = [], [], [], [], []
    todo = int((~escalas.en_base).sum()) if use_poi else 0
    done = 0
    for k, e in enumerate(escalas.to_dict("records")):
        pois: list[tuple[str | None, str | None]] = []
        places: list[str] = []
        query = use_poi and not e["en_base"] and not e["hueco_datos"]
        for p in e["_paradas"]:
            p["lat"], p["lon"] = round(p["lat"], 6), round(p["lon"], 6)
            p["label"] = ""
            if query:
                cat, name = _classify_geo(p["lat"], p["lon"])
                cat = None if cat == "ERROR" else cat
                pois.append((cat, name))
                p["label"] = f"{cat}: {name}" if cat else (_classify_via_nominatim(p["lat"], p["lon"])[1] or "sin POI")
                time.sleep(0.2)
            off = "" if p["apagado_pct"] is None else f", motor apagado {p['apagado_pct']}%"
            places.append(f"{p['label'] + ' ' if p['label'] else ''}({p['min'] / 60:.1f} h{off}) @ {p['lat']}, {p['lon']}")
        if query:
            done += 1
            if done % 10 == 0:
                print(f"  POIs: {done}/{todo} escalas", flush=True)
        longest = max(e["_paradas"], key=lambda p: p["min"])
        main_lat.append(longest["lat"]); main_lon.append(longest["lon"])
        poi_txt.append("; ".join(f"{c}: {n}" for c, n in pois if c))
        place_txt.append(" → ".join(places))
        tipos.append(classify_escala(e, pois))
    escalas["tipo_escala"] = tipos
    escalas["poi"] = poi_txt
    escalas["paradas_internas"] = place_txt
    escalas["parada_principal_lat"], escalas["parada_principal_lon"] = main_lat, main_lon
    escalas["gmaps_parada_principal"] = [gmaps_url(a, b) for a, b in zip(main_lat, main_lon)]
    escalas["gmaps_llegada"] = [gmaps_url(a, b) for a, b in zip(escalas.lat, escalas.lon)]
    return escalas


# ---------------------------------------------------------------- reporte

def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |")
    return "\n".join(lines)


def write_summary(path: Path, routes: pd.DataFrame, sites: pd.DataFrame, scope: str, idle: list[int],
                  trips: pd.DataFrame, escalas: pd.DataFrame, used_poi: bool) -> None:
    turn = routes[routes.fin_motivo == "retorno detectado"]
    regresos = routes[routes.tipo == "regreso"]
    per_asset = routes.groupby("assetId").agg(
        rutas=("ruta_n", "size"),
        ida=("tipo", lambda t: int((t == "ida").sum())),
        regreso=("tipo", lambda t: int((t == "regreso").sum())),
        trayecto=("tipo", lambda t: int((t == "trayecto").sum())),
        local=("tipo", lambda t: int((t == "local").sum())),
        km_total=("distancia_km", "sum"),
        horas_mov=("duracion_h", "sum"),
        km_mediana_ruta=("distancia_km", "median"),
    ).round(1).reset_index()
    od = (routes.groupby(["sitio_inicio", "sitio_fin"]).agg(
        rutas=("ruta_n", "size"), assets=("assetId", "nunique"),
        km_mediana=("distancia_km", "median"), horas_mediana=("duracion_h", "median"))
        .round(1).reset_index().sort_values("rutas", ascending=False))
    od = od[od.sitio_inicio != od.sitio_fin].head(15)
    after_stop = regresos[regresos.retorno_metodo.astype(str).str.startswith("tras parada")]
    method = turn.retorno_metodo.str.startswith("carretera").value_counts()

    examples = turn.sort_values("distancia_km", ascending=False).head(10)[
        ["assetId", "ruta_n", "tipo", "inicio", "fin", "distancia_km", "sitio_inicio", "sitio_fin", "retorno_metodo"]]
    long_trips = trips.sort_values("km", ascending=False).head(10)[
        ["assetId", "viaje_n", "rutas", "inicio", "fin", "sitio_origen", "sitio_destino", "sitio_fin", "km",
         "horas_total", "regresa_a_base", "escalas"]] if len(trips) else trips
    in_trip = escalas[escalas.viaje_n.notna()]
    escala_types = in_trip.tipo_escala.value_counts().rename_axis("tipo_escala").reset_index(name="escalas")

    text = f"""# Rutas: inicio, fin y punto de retorno

Generado: {datetime.now():%Y-%m-%d %H:%M} · script `backend/scripts/detect_route_turnarounds.py`
Detalle por ruta: `rutas_inicio_fin.csv` · viajes: `viajes.csv` · escalas: `escalas.csv` · sitios: `sitios.csv` · mapa: `mapa_rutas.html`

## Alcance de los datos

{scope}
- Con rutas: {routes.assetId.nunique()}. Sin rutas (sin movimiento ≥ {MIN_LEG_M / 1000:.0f} km en todo el periodo): {', '.join(map(str, idle)) or 'ninguno'}.
- Periodo: {routes.inicio.min()} a {routes.fin.max()} (hora {LOCAL_TZ}).

## Método

1. **Limpieza**: se quitan coordenadas nulas/0 y puntos que implican > {MAX_SPEED_KMH:.0f} km/h desde el último punto válido.
2. **Paradas**: el vehículo permanece en un radio de {STOP_RADIUS_M:.0f} m por ≥ {MIN_STOP_S // 60} min.
3. **Cortes duros** (separan bloques de movimiento): parada ≥ {LONG_STOP_S / 3600:.0f} h o hueco de datos ≥ {MAX_GAP_S / 3600:.0f} h.
4. **Retorno** (nueva ruta): dentro de cada bloque la trayectoria se remuestrea cada {RESAMPLE_M:.0f} m. Un punto
   "repasa" un punto previo si está a < {RETRACE_RADIUS_M:.0f} m y entre ambos se recorrió bastante más que esa separación.
   Si durante ≥ {MIN_RETRACE_M / 1000:.0f} km se repasa el camino previo **en sentido inverso**, hay retorno. El corte va en el
   ápice (el punto más lejano antes de empezar a regresar). Si hay una parada a ≤ {APEX_STOP_WINDOW_M / 1000:.0f} km del ápice, la ida
   termina al llegar a la parada y el regreso empieza al salir (`parada_destino_min`). Se repite en lo que queda
   del bloque, así que un bloque puede tener varias vueltas.
5. **Carreteras**: cada punto se empata con la vértice más cercana de `carreteras_raw` (≤ {ROAD_MAX_DIST_M:.0f} m). Si sobre la misma
   carretera el kilometraje avanza antes del ápice y retrocede después, el retorno queda confirmado por carretera
   (`retorno_metodo = carretera: …`); si no (calles urbanas fuera del catálogo), queda `geométrico`.
6. **Sitios**: inicios y fines a ≤ {SITE_EPS_M / 1000:.1f} km se agrupan (DBSCAN) en sitios `S001…`, ordenados por visitas.
7. Rutas de menos de {MIN_LEG_M / 1000:.0f} km se descartan (maniobras en patio).

8. **Regreso tras parada larga**: si el vehículo llega al destino, se queda ≥ {LONG_STOP_S / 3600:.0f} h y después regresa, el
   corte ya lo hizo la parada. Ese trayecto se marca como regreso cuando ≥ {MIN_REVERSE_OVERLAP:.0%} de sus puntos repasan el
   trayecto significativo anterior (o el previo a ese, si en medio hubo un tramo corto en el destino) en sentido
   inverso (correlación de Spearman ≤ −0.5 entre el avance de uno y del otro).

9. **Bases y viajes**: el rol de cada sitio se infiere del comportamiento de cada asset (`bases.csv`):
   **base** = sitio con ≥ {BASE_MIN_SHARE:.0%} de sus inicios/fines (y ≥ {BASE_MIN_VISITS} visitas); entre sus bases, la que tiene
   ≥ {PERNOCTA_MIN_NIGHTS} estancias nocturnas y ≥ {PERNOCTA_MIN_SHARE:.0%} de sus estancias nocturnas es **pernocta** (pensión) y las demás son
   **carga**; **centro de flota** = sitio donde ≥ {HUB_MIN_ASSETS} assets distintos se quedan ≥ {HUB_MIN_DWELL_S // 3600} h (cuenta si el asset también
   se quedó ahí). Un viaje sale de una base y termina al llegar a una base. Excepción: carga → pernocta y, en
   ≤ {PRE_LEG_MAX_S // 3600} h, salida de la pernocta a un sitio que no es base = cargó, durmió en la pensión y salió a repartir:
   es un solo viaje y la noche en la pensión es una escala ("pernocta en base"). Una ruta base → base de
   ≤ {TRASLADO_MAX_KM:.0f} km es un **traslado** (columna `traslado`), no un viaje. `sitio_destino` es el fin de ruta más lejano.
10. **Escalas**: el tiempo entre el fin de una ruta y el inicio de la siguiente. Dentro de la escala se buscan las
   paradas de ≥ {ESCALA_POI_MIN_S // 60} min (puede haber varias, con movimientos cortos entre ellas) y, con `--poi`, se consulta
   en OSM qué hay a ≤ 300 m de cada una (`stop_detection`). `motor_apagado_pct` = % del tiempo entre
   IGNITION_OFF y el siguiente IGNITION_ON (el resto es ralentí o movimiento corto). `tipo_escala`, en este orden: escala en la pernocta del asset → pernocta en base; incluye las {NIGHT_HOUR:02d}:00 y dura entre
   {NIGHT_MIN_S // 3600} y {NIGHT_MAX_S // 3600} h → descanso nocturno; POI zona industrial/bodega → carga/descarga; hotel, tienda, farmacia u
   hospital → entrega probable; gasolinera → combustible/descanso; restaurante → alimentos; taller; si no, sin identificar.
   Es una lectura probable: un cliente que no está en OSM queda "sin identificar".

Tipos:
- **ida**: ruta que después se recorre de vuelta (por retorno detectado o tras parada larga).
- **regreso**: empieza donde los puntos empiezan a regresar; `ruta_ida_n` indica su ida.
- **trayecto**: ruta sin regreso por el mismo camino (termina en parada larga, hueco o fin de datos).
- **local**: movimiento que nunca se aleja más de {LOCAL_MAX_KM:.0f} km de su inicio (patio, maniobras).

## Resultados globales

- Rutas detectadas: **{len(routes)}** · ida: {(routes.tipo == 'ida').sum()} · regreso: {len(regresos)} · trayecto: {(routes.tipo == 'trayecto').sum()} · local: {(routes.tipo == 'local').sum()}
- Regresos: **{len(regresos)}** · retorno detectado en movimiento: {len(turn)} (confirmados por carretera: {int(method.get(True, 0))}, solo geométricos: {int(method.get(False, 0))}) · regreso tras parada larga: {len(after_stop)}
- Regresos que vuelven a ≤ 5 km del origen de su ida: {int((regresos.regresa_a_origen == True).sum())} de {len(regresos)}
- Distancia total: {routes.distancia_km.sum():,.0f} km · mediana por ruta: {routes.distancia_km.median():.1f} km · mediana de duración: {routes.duracion_h.median():.1f} h
- Fines de ruta por motivo: {'; '.join(f'{m}: {c}' for m, c in routes.fin_motivo.str.replace(r' [\d.]+ h$', '', regex=True).value_counts().items())}

## Viajes (base → base)

- Viajes: **{len(trips)}** · regresan a una base: {int(trips.regresa_a_base.sum()) if len(trips) else 0} · con escalas: {int((trips.tramos > 1).sum()) if len(trips) else 0}
- Assets sin base clara (ningún sitio con ≥ {BASE_MIN_SHARE:.0%} de sus inicios/fines; sus viajes juntan muchas rutas):
  {', '.join(map(str, sorted(trips.loc[~trips.base_clara, 'assetId'].unique()))) if len(trips) else ''}
- Escalas dentro de viajes: {len(in_trip)}{'' if used_poi else ' (sin consulta de POIs; correr con `--poi` para clasificarlas)'}

{md_table(escala_types)}

### Viajes más largos

{md_table(long_trips)}

## Por asset

{md_table(per_asset)}

## Sitios más visitados

{md_table(sites.head(10))}

## Pares origen → destino más frecuentes

{md_table(od)}

## Retornos más largos (ejemplos)

{md_table(examples)}

## Limitaciones

- El catálogo de carreteras cubre la red federal; en ciudad el retorno se detecta solo por geometría.
- Un retorno por una vía distinta a la de ida (circuito) no repasa el camino previo, así que no se corta; esa ruta
  queda como `trayecto` hasta la siguiente parada larga.
- Los huecos de GPS (puntos sin coordenadas, sección 4) reducen la densidad de puntos; si el retorno cae en un hueco,
  el ápice se ubica en el último punto con coordenadas.
- Los umbrales están al inicio del script y se pueden ajustar.
"""
    path.write_text(text, encoding="utf-8")


def write_map(path: Path, routes: pd.DataFrame, paths: dict, trips: pd.DataFrame, escalas: pd.DataFrame) -> None:
    """
    Mapa HTML (Leaflet) con el trazo, inicio, fin y retorno de cada ruta. Las rutas de cada
    asset van en mapa_datos/a_<assetId>.js y se cargan al seleccionarlo (con <script>, así
    funciona también abriendo el archivo directo, sin servidor).
    """
    data_dir = path.parent / "mapa_datos"
    data_dir.mkdir(exist_ok=True)
    for old in data_dir.glob("a_*.js"):
        old.unlink()
    records = routes.astype(object).where(routes.notna(), None).to_dict("records")
    by_asset: dict[int, list[dict]] = {}
    for r in records:
        r["path"] = paths[(r["assetId"], r["ruta_n"])]
        by_asset.setdefault(int(r["assetId"]), []).append(r)
    def recs(df: pd.DataFrame, cols: list[str]) -> dict[int, list[dict]]:
        out: dict[int, list[dict]] = {}
        if len(df):
            df = df[cols].astype(object).where(df[cols].notna(), None)
            for rec in df.to_dict("records"):
                out.setdefault(int(rec["assetId"]), []).append(rec)
        return out

    trip_recs = recs(trips, [c for c in trips.columns if not c.startswith("gmaps")])
    escala_recs = recs(escalas[escalas.viaje_n.notna()] if len(escalas) else escalas,
                       ["assetId", "viaje_n", "ruta_antes", "sitio", "llegada", "salida", "duracion_h", "tipo_escala",
                        "poi", "motor_apagado_pct", "lat", "lon", "_paradas"])
    for asset_id, rs in by_asset.items():
        extra = {"viajes": trip_recs.get(asset_id, []), "escalas": escala_recs.get(asset_id, [])}
        data = json.dumps(rs, ensure_ascii=False, separators=(",", ":"), default=str)
        more = json.dumps(extra, ensure_ascii=False, separators=(",", ":"), default=str)
        (data_dir / f"a_{asset_id}.js").write_text(f"window.loadRoutes({asset_id},{data},{more});", encoding="utf-8")
    assets = [{"assetId": a, "n": len(rs), "km": round(sum(r["distancia_km"] for r in rs))} for a, rs in sorted(by_asset.items())]
    template = (Path(__file__).with_name("mapa_rutas_template.html")).read_text(encoding="utf-8")
    path.write_text(template.replace("__ASSETS__", json.dumps(assets, separators=(",", ":"))), encoding="utf-8")


_WORKER: dict = {}


def _init_worker(db_path: str) -> None:
    con = duckdb.connect(db_path, read_only=True)
    con.execute("SET threads = 2")
    _WORKER["con"], _WORKER["roads"] = con, RoadIndex(con)


def _analyze_one(asset_id: int, client_id: int) -> tuple[int, int, list[dict], list[dict]]:
    df = load_track(_WORKER["con"], asset_id, client_id)
    if len(df) < 10:
        return asset_id, len(df), [], []
    routes, escalas = analyze_asset(df, _WORKER["roads"], asset_id, client_id)
    for e in escalas:
        e.update(assetId=asset_id, clientId=client_id)
    return asset_id, len(df), routes, escalas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--db", default=str(get_default_duckdb_path()))
    parser.add_argument("--assets-file", default=str(ROOT / "data" / "asset_id"))
    parser.add_argument("--client-id", type=int, help="analizar todos los assets de este cliente (ignora --assets-file)")
    parser.add_argument("--asset-id", type=int, nargs="+", help="con --client-id: analizar solo estas parejas (asset, cliente)")
    parser.add_argument("--out-dir", default=str(ROOT / "docs" / "rutas_inicio_fin"))
    parser.add_argument("--workers", type=int, default=4, help="procesos en paralelo (cada uno usa ~1.5 GB)")
    parser.add_argument("--poi", action="store_true",
                        help="consultar POIs de OSM (stop_detection) en las escalas de cada viaje (requiere red)")
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(args.db, read_only=True) as con:
        if args.asset_id is not None:
            if args.client_id is None:
                parser.error("--asset-id requiere --client-id")
            pairs = con.execute(
                "SELECT DISTINCT assetId, clientId FROM merged_pos_eventtype WHERE assetId IN (SELECT unnest(?)) AND clientId = ? ORDER BY 1",
                [args.asset_id, args.client_id],
            ).fetchall()
            missing = sorted(set(args.asset_id) - {a for a, _ in pairs})
            if not pairs:
                parser.error(f"sin registros para assets {args.asset_id} / cliente {args.client_id}")
            scope = (f"- Assets {', '.join(str(a) for a, _ in pairs)} del cliente {args.client_id}."
                     + (f" Sin registros: {', '.join(map(str, missing))}." if missing else ""))
        elif args.client_id is not None:
            pairs = con.execute(
                "SELECT DISTINCT assetId, clientId FROM merged_pos_eventtype WHERE clientId = ? ORDER BY 1",
                [args.client_id],
            ).fetchall()
            scope = f"- Todos los assets del cliente {args.client_id} en el snapshot (`merged_pos_eventtype`): **{len(pairs)}**."
        else:
            listed = [int(x) for x in re.findall(r"\d+", Path(args.assets_file).read_text())]
            con.execute("CREATE TEMP TABLE wanted AS SELECT unnest(?) AS assetId", [listed])
            pairs = con.execute(
                "SELECT DISTINCT assetId, clientId FROM merged_pos_eventtype JOIN wanted USING (assetId) ORDER BY 1"
            ).fetchall()
            clients = ", ".join(sorted({str(c) for _, c in pairs}))
            scope = (f"- Lista de assets: `{Path(args.assets_file).name}` ({len(listed)} IDs). **Solo "
                     f"{len({a for a, _ in pairs})} tienen registros** en el snapshot (`merged_pos_eventtype`, "
                     f"cliente {clients}); los otros {len(set(listed) - {a for a, _ in pairs})} no aparecen.")
        print(f"{len(pairs)} assets con datos; analizando con {args.workers} procesos…", flush=True)

    # Cada proceso abre su propia conexión de solo lectura y su índice de carreteras.
    results: dict[int, list[dict]] = {}
    escala_results: dict[int, list[dict]] = {}
    started = time.time()
    with ProcessPoolExecutor(args.workers, initializer=_init_worker, initargs=(args.db,)) as pool:
        futures = {pool.submit(_analyze_one, a, c): a for a, c in pairs}
        for k, fut in enumerate(as_completed(futures), 1):
            asset_id, n_points, routes, escalas = fut.result()
            results[asset_id] = routes
            escala_results[asset_id] = escalas
            eta = (time.time() - started) / k * (len(pairs) - k) / 60
            print(f"  [{k}/{len(pairs)}] asset {asset_id}: {n_points} puntos GPS → {len(routes)} rutas"
                  f" (faltan ~{eta:.0f} min)", flush=True)
    all_routes = [r for a in sorted(results) for r in results[a]]

    paths = {(r["assetId"], r["ruta_n"]): r["_path"] for r in all_routes}
    routes = pd.DataFrame(all_routes).drop(columns=["_i", "_j", "_path"])
    routes["ruta_ida_n"] = pd.to_numeric(routes["ruta_ida_n"], errors="coerce").astype("Int64")
    sites = assign_sites(routes)
    lead = ["assetId", "clientId", "ruta_n", "tipo", "inicio", "fin", "sitio_inicio", "sitio_fin"]
    routes = routes[lead + [c for c in routes.columns if c not in lead]]

    escalas = pd.DataFrame([e for a in sorted(escala_results) for e in escala_results[a]])
    trips, escalas, roles = build_trips(routes, escalas)
    print(f"{len(trips)} viajes, {len(escalas)} escalas" + (" · consultando POIs…" if args.poi else ""), flush=True)
    escalas = enrich_escalas(escalas, args.poi)
    elead = ["assetId", "clientId", "viaje_n", "ruta_antes", "ruta_despues", "sitio", "llegada", "salida",
             "duracion_h", "tipo_escala", "poi", "paradas_internas", "gmaps_parada_principal"]
    escalas = escalas[elead + [c for c in escalas.columns if c not in elead]]
    if len(trips):
        main_stops = (escalas[escalas.viaje_n.notna()]
                      .assign(txt=lambda e: e.sitio + " " + e.tipo_escala + " " + e.duracion_h.round(1).astype(str) + " h")
                      .groupby(["assetId", "viaje_n"]).txt.agg("; ".join).rename("escalas"))
        trips = trips.merge(main_stops, left_on=["assetId", "viaje_n"], right_index=True, how="left")
        trips["escalas"] = trips["escalas"].fillna("")

    if len(trips):
        trips["gmaps_destino"] = [gmaps_url(a, b) for a, b in zip(trips.destino_lat, trips.destino_lon)]

    routes_csv = routes.assign(gmaps_inicio=[gmaps_url(a, b) for a, b in zip(routes.inicio_lat, routes.inicio_lon)],
                               gmaps_fin=[gmaps_url(a, b) for a, b in zip(routes.fin_lat, routes.fin_lon)])
    routes_csv.to_csv(out / "rutas_inicio_fin.csv", index=False)
    sites.to_csv(out / "sitios.csv", index=False)
    roles.merge(sites[["sitio", "lat", "lon"]], on="sitio", how="left").assign(
        gmaps=lambda d: [gmaps_url(a, b) for a, b in zip(d.lat, d.lon)]).to_csv(out / "bases.csv", index=False)
    trips.to_csv(out / "viajes.csv", index=False)
    escalas.drop(columns=["_paradas"]).to_csv(out / "escalas.csv", index=False)
    write_map(out / "mapa_rutas.html", routes, paths, trips, escalas)
    idle = sorted({a for a, _ in pairs} - set(routes.assetId))
    write_summary(out / "resumen_rutas.md", routes, sites, scope, idle, trips, escalas, args.poi)
    print(f"Listo: {out}")


if __name__ == "__main__":
    main()
