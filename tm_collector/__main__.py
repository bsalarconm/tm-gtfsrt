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


FLUSH_ROWS = 20_000  # vacía el buffer a disco cada 20.000 filas para no llenar la memoria


class Stream:
    """Un feed: URL, intervalo, parser y el archivo Parquet de la hora en curso (se escribe por bloques)."""

    def __init__(self, prefix, url, every, parser, schema):
        self.prefix, self.url, self.every = prefix, url, every
        self.parser, self.schema = parser, schema
        self.buffer, self.last_poll, self.errors = [], 0.0, 0
        self.writer, self.tmp_path, self.rows_written = None, None, 0
        self.agg = None
        self.reset_agg()

    def reset_agg(self):
        if config.SUPABASE_ENABLED:
            from .supabase_sink import AGGREGATORS
            self.agg = AGGREGATORS[self.prefix]()

    def due(self, now):
        return self.url and now - self.last_poll >= self.every

    def flush(self, hour_key):
        """Escribe el buffer como un bloque del Parquet de la hora y libera la memoria."""
        if not self.buffer:
            return
        if self.writer is None:
            day, hh = hour_key
            pending = config.WORK_DIR / "pending"
            pending.mkdir(parents=True, exist_ok=True)
            self.tmp_path = pending / f"{self.prefix}_{day}_{hh}_{int(time.time())}.parquet.part"
            self.writer = pq.ParquetWriter(str(self.tmp_path), self.schema, compression="zstd")
        self.writer.write_table(pa.Table.from_pylist(self.buffer, schema=self.schema))
        if self.agg is not None:
            self.agg.update(self.buffer)
        self.rows_written += len(self.buffer)
        self.buffer = []

    def close(self, hour_key):
        """Cierra el Parquet de la hora; devuelve (ruta_final, filas) o None."""
        self.flush(hour_key)
        if self.writer is None:
            return None
        self.writer.close()
        final = self.tmp_path.with_suffix("")  # quita .part
        self.tmp_path.replace(final)
        result = (final, self.rows_written)
        self.writer, self.tmp_path, self.rows_written = None, None, 0
        log.info("Parquet: %s (%d filas, %.2f MB)", final.name, result[1], final.stat().st_size / 1e6)
        return result


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
        res = s.close(hour_key)
        if res:
            rows_by_file[res[0].name] = res[1]
        if config.SUPABASE_ENABLED and s.agg is not None and res:
            try:
                from . import supabase_sink
                supabase_sink.push_aggregates(s.prefix, s.agg)
            except Exception as ex:
                log.warning("no se enviaron agregados de %s a Supabase: %s", s.prefix, ex)
        s.reset_agg()
    upload_pending(rows_by_file)


def cleanup_partials():
    """Borra archivos .part de una ejecución anterior que se interrumpió (están incompletos)."""
    pending = config.WORK_DIR / "pending"
    for p in pending.glob("*.part") if pending.exists() else []:
        log.warning("descartando archivo incompleto de una ejecución anterior: %s", p.name)
        p.unlink()


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

    cleanup_partials()
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
                if len(s.buffer) >= FLUSH_ROWS:
                    s.flush(current_hour)
            except Exception as ex:
                s.errors += 1
                log.warning("[%s] error (%d seguidos): %s", s.prefix, s.errors, ex)
                if s.errors * s.every >= 300:  # ~5 min fallando
                    ping(False)

        if t0 - last_ping > 300 and all(s.errors == 0 for s in streams):
            ping(True)
            last_ping = t0
            log.info("ok | " + " | ".join(f"{s.prefix}: {s.rows_written + len(s.buffer)} filas esta hora" for s in streams))

        time.sleep(max(0.0, config.POLL_SECONDS - (time.time() - t0)))

    log.info("Deteniendo: guardando la hora en curso…")
    close_hour(streams, current_hour, rows_by_file)
    log.info("Detenido.")


if __name__ == "__main__":
    main()
