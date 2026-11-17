"""
Recolector GTFS-RT de TransMilenio: posiciones, viajes y alertas.

  python -m tm_collector          modo continuo (Render worker)
  python -m tm_collector --once   prueba: lee los 3 feeds y muestra un resumen (no guarda nada)
  python -m tm_collector --test   prueba completa: recolecta 2 minutos, guarda, sube a Drive y Supabase, y termina
"""
import logging
import signal
import sys
import time
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from . import config
from .feed import AL_SCHEMA, TU_SCHEMA, VP_SCHEMA, AlertParser, PositionParser, TripUpdateParser, fetch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("collector")


class Stream:
    """Un feed: URL, intervalo de consulta, parser, esquema y buffer de la hora en curso."""

    def __init__(self, prefix, url, every, parser, schema):
        self.prefix, self.url, self.every = prefix, url, every
        self.parser, self.schema = parser, schema
        self.buffer, self.last_poll, self.errors = [], 0.0, 0

    def due(self, now):
        return self.url and now - self.last_poll >= self.every


running = True


def _stop(*_):
    global running
    running = False


def ping(ok=True):
    if not config.HEALTHCHECK_URL:
        return
    try:
        url = config.HEALTHCHECK_URL if ok else config.HEALTHCHECK_URL.rstrip("/") + "/fail"
        requests.get(url, timeout=5)
    except Exception:
        pass


def write_parquet(stream, hour_key):
    day, hh = hour_key
    name = f"{stream.prefix}_{day}_{hh}_{int(time.time())}.parquet"
    pending = config.WORK_DIR / "pending"
    pending.mkdir(parents=True, exist_ok=True)
    path = pending / name
    pq.write_table(pa.Table.from_pylist(stream.buffer, schema=stream.schema), path, compression="zstd")
    log.info("Parquet: %s (%d filas, %.2f MB)", name, len(stream.buffer), path.stat().st_size / 1e6)
    return path


def upload_pending(rows_by_file):
    """Sube los Parquet pendientes; los que fallen se reintentan en la siguiente hora."""
    if not config.DRIVE_ENABLED:
        return
    from . import drive
    pending = config.WORK_DIR / "pending"
    for path in sorted(pending.glob("*.parquet")):
        try:
            drive_id = drive.upload(path, path.name)
        except Exception as ex:
            log.error("no se pudo subir %s (se reintentará): %s", path.name, ex)
            ping(False)
            continue
        if config.SUPABASE_ENABLED:
            try:
                from . import supabase_sink
                supabase_sink.push_file_log(path.name, rows_by_file.get(path.name), path.stat().st_size, drive_id)
            except Exception as ex:
                log.warning("no se registró el archivo en Supabase: %s", ex)
        if config.KEEP_LOCAL:
            done = config.WORK_DIR / "uploaded"
            done.mkdir(parents=True, exist_ok=True)
            path.replace(done / path.name)
        else:
            path.unlink()
        rows_by_file.pop(path.name, None)


def close_hour(streams, hour_key, rows_by_file):
    for s in streams:
        if not s.buffer:
            continue
        path = write_parquet(s, hour_key)
        rows_by_file[path.name] = len(s.buffer)
        if config.SUPABASE_ENABLED:
            try:
                from . import supabase_sink
                supabase_sink.push_aggregates(s.prefix, s.buffer)
            except Exception as ex:
                log.warning("no se enviaron agregados de %s a Supabase: %s", s.prefix, ex)
        s.buffer = []
    upload_pending(rows_by_file)


def build_streams():
    return [
        Stream("vp", config.FEED_URL, config.POLL_SECONDS, PositionParser(), VP_SCHEMA),
        Stream("tu", config.TRIPUPDATES_URL, config.TRIPUPDATES_POLL_SECONDS, TripUpdateParser(), TU_SCHEMA),
        Stream("al", config.ALERTS_URL, config.ALERTS_POLL_SECONDS, AlertParser(), AL_SCHEMA),
    ]


def run_once():
    for s in build_streams():
        if not s.url:
            print(f"[{s.prefix}] sin URL configurada")
            continue
        try:
            raw = fetch(s.url)
            rows, dup, n = s.parser.parse(raw, int(time.time()))
        except Exception as ex:
            print(f"[{s.prefix}] ERROR: {ex}")
            continue
        filled = sorted({k for r in rows for k, v in r.items() if v is not None})
        print(f"\n[{s.prefix}] {s.url}\n  entidades: {n} | filas: {len(rows)} | tamaño: {len(raw) / 1024:.0f} KB")
        print("  campos con datos:", ", ".join(filled))
        for r in rows[:2]:
            print("  ", r)


def main():
    if "--once" in sys.argv:
        run_once()
        return

    config.validate()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    streams = [s for s in build_streams() if s.url]
    rows_by_file = {}
    current_hour = None
    last_ping = 0.0
    test_end = time.time() + 120 if "--test" in sys.argv else None
    log.info("Recolector iniciado: %s", ", ".join(f"{s.prefix} cada {s.every}s" for s in streams))

    while running:
        t0 = time.time()
        if test_end and t0 >= test_end:
            log.info("Fin de la prueba de 2 minutos")
            break
        now = datetime.now(config.TZ)
        hour_key = (now.strftime("%Y-%m-%d"), now.strftime("%H"))
        if current_hour and hour_key != current_hour:
            close_hour(streams, current_hour, rows_by_file)
        current_hour = hour_key

        for s in streams:
            if not s.due(t0):
                continue
            s.last_poll = t0
            try:
                rows, dup, n = s.parser.parse(fetch(s.url), int(t0))
                s.buffer.extend(rows)
                s.errors = 0
            except Exception as ex:
                s.errors += 1
                log.warning("[%s] error (%d seguidos): %s", s.prefix, s.errors, ex)
                if s.errors * s.every >= 300:  # ~5 min fallando
                    ping(False)

        if t0 - last_ping > 300 and all(s.errors == 0 for s in streams):
            ping(True)
            last_ping = t0
            log.info("ok | " + " | ".join(f"{s.prefix}: {len(s.buffer)} en memoria" for s in streams))

        time.sleep(max(0.0, config.POLL_SECONDS - (time.time() - t0)))

    log.info("Deteniendo: guardando la hora en curso…")
    close_hour(streams, current_hour, rows_by_file)
    log.info("Detenido.")


if __name__ == "__main__":
    main()
