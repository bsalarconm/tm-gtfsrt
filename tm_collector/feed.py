"""Descarga y lectura de los 3 feeds GTFS-Realtime de TransMilenio.

- positions.pb    posición de cada bus (cada ~26 s)
- tripupdates.pb  estado de cada viaje del día: programado / agregado / cancelado, retraso, bus asignado
- alerts.pb       novedades de operación (desvíos, ciclovía, lluvias, planes especiales)

Cada parser descarta lo que no cambió desde la consulta anterior, para no guardar duplicados.
"""
import hashlib
import math

import requests
import pyarrow as pa
from google.transit import gtfs_realtime_pb2

from . import config

# ---------------------------------------------------------------- esquemas
VP_SCHEMA = pa.schema([
    ("poll_ts", pa.int64()), ("feed_ts", pa.int64()), ("vehicle_ts", pa.int64()),
    ("vehicle_id", pa.string()), ("vehicle_label", pa.string()), ("license_plate", pa.string()),
    ("trip_id", pa.string()), ("route_id", pa.string()), ("direction_id", pa.int8()),
    ("start_date", pa.string()), ("start_time", pa.string()),
    ("lat", pa.float64()), ("lon", pa.float64()),
    ("bearing", pa.float32()), ("speed", pa.float32()),
    ("speed_calc_ms", pa.float32()),  # calculada entre dos posiciones seguidas del mismo bus
    ("stop_id", pa.string()), ("current_stop_sequence", pa.int32()),
    ("current_status", pa.int8()), ("occupancy_status", pa.int8()),
])

TU_SCHEMA = pa.schema([
    ("poll_ts", pa.int64()), ("feed_ts", pa.int64()), ("tu_ts", pa.int64()),
    ("trip_id", pa.string()), ("route_id", pa.string()),
    ("start_date", pa.string()), ("start_time", pa.string()),
    ("schedule_relationship", pa.int8()),  # 0 programado, 1 agregado, 3 cancelado
    ("delay_s", pa.int32()),
    ("vehicle_id", pa.string()), ("vehicle_label", pa.string()), ("license_plate", pa.string()),
    ("n_stop_updates", pa.int32()),
])

AL_SCHEMA = pa.schema([
    ("poll_ts", pa.int64()), ("feed_ts", pa.int64()),
    ("alert_id", pa.string()), ("content_hash", pa.string()), ("is_deleted", pa.bool_()),
    ("cause", pa.int16()), ("effect", pa.int16()),
    ("header_text", pa.string()), ("description_text", pa.string()),
    ("active_start", pa.int64()), ("active_end", pa.int64()), ("n_active_periods", pa.int32()),
    ("route_ids", pa.string()), ("stop_ids", pa.string()), ("trip_ids", pa.string()),
])

# ---------------------------------------------------------------- descarga
_session = requests.Session()
_headers = {}
if config.API_HEADER:
    k, v = config.API_HEADER.split(":", 1)
    _headers[k.strip()] = v.strip()


def fetch(url: str) -> bytes:
    """Descarga con 2 reintentos rápidos (fallas de red pasajeras)."""
    import time
    for intento in range(3):
        try:
            r = _session.get(url, headers=_headers, timeout=15)
            r.raise_for_status()
            return r.content
        except Exception:
            if intento == 2:
                raise
            time.sleep(2)


def _feed(raw: bytes):
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)
    return msg, (msg.header.timestamp or None)


def _haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _text(ts):
    return " | ".join(t.text for t in ts.translation) or None


# ---------------------------------------------------------------- parsers
class PositionParser:
    def __init__(self):
        self.last = {}  # vehicle_id -> (vehicle_ts, lat, lon)

    def parse(self, raw: bytes, poll_ts: int):
        msg, feed_ts = _feed(raw)
        rows, dup = [], 0
        for e in msg.entity:
            if not e.HasField("vehicle"):
                continue
            v = e.vehicle
            vid = v.vehicle.id or v.vehicle.label or e.id
            vts = v.timestamp or feed_ts or poll_ts
            prev = self.last.get(vid)
            if prev and prev[0] == vts:
                dup += 1
                continue
            t, p = v.trip, v.position
            has_pos = v.HasField("position")
            lat = p.latitude if has_pos else None
            lon = p.longitude if has_pos else None
            speed_calc = None
            if prev and has_pos and prev[1] is not None:
                dt = vts - prev[0]
                if 0 < dt <= 300:
                    s = _haversine_m(prev[1], prev[2], lat, lon) / dt
                    if s <= 35:  # descarta saltos de GPS (> 126 km/h)
                        speed_calc = round(s, 2)
            self.last[vid] = (vts, lat, lon)
            rows.append({
                "poll_ts": poll_ts, "feed_ts": feed_ts, "vehicle_ts": vts,
                "vehicle_id": vid, "vehicle_label": v.vehicle.label or None,
                "license_plate": v.vehicle.license_plate or None,
                "trip_id": t.trip_id or None, "route_id": t.route_id or None,
                "direction_id": t.direction_id if t.HasField("direction_id") else None,
                "start_date": t.start_date or None, "start_time": t.start_time or None,
                "lat": lat, "lon": lon,
                "bearing": p.bearing if has_pos and p.HasField("bearing") else None,
                "speed": p.speed if has_pos and p.HasField("speed") else None,
                "speed_calc_ms": speed_calc,
                "stop_id": v.stop_id or None,
                "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
                "current_status": v.current_status if v.HasField("current_status") else None,
                "occupancy_status": v.occupancy_status if v.HasField("occupancy_status") else None,
            })
        return rows, dup, len(msg.entity)


class TripUpdateParser:
    """Guarda un viaje solo cuando cambia su estado, su retraso o el bus asignado."""

    def __init__(self):
        self.state = {}

    def parse(self, raw: bytes, poll_ts: int):
        msg, feed_ts = _feed(raw)
        rows, dup = [], 0
        for e in msg.entity:
            if not e.HasField("trip_update"):
                continue
            tu = e.trip_update
            t = tu.trip
            key = t.trip_id or e.id
            sig = (t.schedule_relationship, tu.delay if tu.HasField("delay") else None,
                   tu.vehicle.id, len(tu.stop_time_update))
            if self.state.get(key) == sig:
                dup += 1
                continue
            self.state[key] = sig
            rows.append({
                "poll_ts": poll_ts, "feed_ts": feed_ts, "tu_ts": tu.timestamp or None,
                "trip_id": t.trip_id or None, "route_id": t.route_id or None,
                "start_date": t.start_date or None, "start_time": t.start_time or None,
                "schedule_relationship": t.schedule_relationship,
                "delay_s": tu.delay if tu.HasField("delay") else None,
                "vehicle_id": tu.vehicle.id or None, "vehicle_label": tu.vehicle.label or None,
                "license_plate": tu.vehicle.license_plate or None,
                "n_stop_updates": len(tu.stop_time_update),
            })
        # olvida viajes de días anteriores para no crecer sin límite
        if len(self.state) > 200_000:
            self.state.clear()
        return rows, dup, len(msg.entity)


class AlertParser:
    """Guarda una alerta cuando aparece o cambia, y marca las que desaparecen.
    El feed puede repetir el mismo id con contenidos distintos, así que la clave es (id, contenido)."""

    def __init__(self):
        self.seen = set()  # {(alert_id, hash)}

    def parse(self, raw: bytes, poll_ts: int):
        msg, feed_ts = _feed(raw)
        rows, dup, current = [], 0, set()
        for e in msg.entity:
            if not e.HasField("alert"):
                continue
            a = e.alert
            key = (e.id, hashlib.sha1(a.SerializeToString(deterministic=True)).hexdigest()[:16])
            if key in current:
                continue
            current.add(key)
            if key in self.seen:
                dup += 1
                continue
            per = list(a.active_period)
            ie = list(a.informed_entity)
            rows.append({
                "poll_ts": poll_ts, "feed_ts": feed_ts,
                "alert_id": key[0], "content_hash": key[1], "is_deleted": False,
                "cause": a.cause, "effect": a.effect,
                "header_text": _text(a.header_text), "description_text": _text(a.description_text),
                "active_start": per[0].start if per and per[0].start else None,
                "active_end": per[0].end if per and per[0].end else None,
                "n_active_periods": len(per),
                "route_ids": ";".join(sorted({x.route_id for x in ie if x.route_id})) or None,
                "stop_ids": ";".join(sorted({x.stop_id for x in ie if x.stop_id})) or None,
                "trip_ids": ";".join(sorted({x.trip.trip_id for x in ie if x.HasField("trip") and x.trip.trip_id})) or None,
            })
        for gone in self.seen - current:  # alerta retirada del feed
            rows.append({**{k: None for k in AL_SCHEMA.names}, **{
                "poll_ts": poll_ts, "feed_ts": feed_ts, "alert_id": gone[0],
                "content_hash": gone[1], "is_deleted": True}})
        self.seen = current
        return rows, dup, len(msg.entity)
