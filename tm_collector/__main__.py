"""
Recolector GTFS-RT de TransMilenio: posiciones, viajes y alertas.

  python -m tm_collector          modo continuo (Render worker)
  python -m tm_collector --once   prueba: lee los 3 feeds y muestra un resumen (no guarda nada)
  python -m tm_collector --test   prueba completa: recolecta 2 minutos, guarda, sube a Drive y Supabase, y termina
"""
import logging
import os
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
CHUNK_MINUTES = int(os.environ.get("CHUNK_MINUTES", "10"))  # cada cuánto se cierra y sube un archivo
STALE_MINUTES = int(os.environ.get("STALE_MINUTES", "10"))  # alerta si vp no trae nada nuevo en este tiempo
_h = os.environ.get("ACTIVE_HOURS", "5-23").split("-")  # horario en que DEBE haber buses (hora Bogotá)
ACTIVE_FROM, ACTIVE_TO = int(_h[0]), int(_h[1])
CHUNKS = config.WORK_DIR / "chunks"    # parciales de 10 min (respaldo)
PENDING = config.WORK_DIR / "pending"  # archivos horarios por subir
uploaded_chunks = set()  # parciales ya respaldados en Drive
hour_chunks = {}         # archivo horario -> parciales que contiene (se borran cuando el horario ya está en Drive)


class Stream:
    """Un feed: URL, intervalo, parser y el archivo Parquet de la hora en curso (se escribe por bloques)."""

    def __init__(self, prefix, url, every, parser, schema):
        self.prefix, self.url, self.every = prefix, url, every
        self.parser, self.schema = parser, schema
        self.buffer, self.last_poll, self.errors = [], 0.0, 0
        self.writer, self.tmp_path, self.rows_written = None, None, 0
        self.last_new, self.last_n = time.time(), None  # última vez que llegaron filas nuevas / entidades en el feed
        self.agg = None
        self.reset_agg()

    def reset_agg(self):
        if config.SUPABASE_ENABLED:
            from .supabase_sink import AGGREGATORS
            self.agg = AGGREGATORS[self.prefix]()

    def due(self, now):
        return self.url and now - self.last_poll >= self.every

    def flush(self, hour_key):
        """Escribe el buffer en el parcial de 10 min en curso y libera la memoria."""
        if not self.buffer:
            return
        if self.writer is None:
            day, hh = hour_key
            CHUNKS.mkdir(parents=True, exist_ok=True)
            self.tmp_path = CHUNKS / f"{self.prefix}_{day}_{hh}_{int(time.time())}.parquet.part"
            self.writer = pq.ParquetWriter(str(self.tmp_path), self.schema, compression="zstd")
        self.writer.write_table(pa.Table.from_pylist(self.buffer, schema=self.schema))
        if self.agg is not None:
            self.agg.update(self.buffer)
        self.rows_written += len(self.buffer)
        self.buffer = []

    def close(self, hour_key):
        """Cierra el parcial de 10 min; devuelve (ruta_final, filas) o None."""
        self.flush(hour_key)
        if self.writer is None:
            return None
        self.writer.close()
        final = self.tmp_path.with_suffix("")  # quita .part
        self.tmp_path.replace(final)
        result = (final, self.rows_written)
        self.writer, self.tmp_path, self.rows_written = None, None, 0
        log.info("Parcial: %s (%d filas, %.2f MB)", final.name, result[1], final.stat().st_size / 1e6)
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


def upload_chunks():
    """Respalda en Drive cada parcial de 10 min apenas se cierra (nombre: parcial_vp_...)."""
    if not config.DRIVE_ENABLED or not CHUNKS.exists():
        return
    from . import drive
    for path in sorted(CHUNKS.glob("*.parquet")):
        if path.name in uploaded_chunks:
            continue
        try:
            drive.upload(path, "parcial_" + path.name)
            uploaded_chunks.add(path.name)
        except Exception as ex:
            log.error("no se pudo respaldar %s (queda local y entra en el horario): %s", path.name, ex)
            ping(False)


def consolidate(streams, day, hh, rows_by_file):
    """Une los parciales de una hora en un solo Parquet horario (por partes, sin cargar todo en memoria)."""
    PENDING.mkdir(parents=True, exist_ok=True)
    for s in streams:
        parts = sorted(CHUNKS.glob(f"{s.prefix}_{day}_{hh}_*.parquet")) if CHUNKS.exists() else []
        if not parts:
            continue
        out = PENDING / f"{s.prefix}_{day}_{hh}_{int(time.time())}.parquet"
        tmp = PENDING / (out.name + ".part")
        writer, n = None, 0
        try:
            for p in parts:
                pf = pq.ParquetFile(str(p))
                for i in range(pf.num_row_groups):
                    t = pf.read_row_group(i)
                    if writer is None:
                        writer = pq.ParquetWriter(str(tmp), s.schema, compression="zstd")
                    writer.write_table(t)
                    n += t.num_rows
            if writer is None:
                continue
            writer.close()
            tmp.replace(out)
        except Exception as ex:
            log.error("no se pudo consolidar %s %s %s (los parciales siguen en Drive): %s", s.prefix, day, hh, ex)
            tmp.unlink(missing_ok=True)
            continue
        rows_by_file[out.name] = n
        hour_chunks[out.name] = [p.name for p in parts]
        log.info("Parquet: %s (%d filas, %d parciales, %.2f MB)", out.name, n, len(parts), out.stat().st_size / 1e6)


def drop_chunks(names):
    """Borra los parciales (local y Drive) de un horario que ya quedó subido."""
    from . import drive
    for c in names:
        try:
            drive.delete_by_name("parcial_" + c)
        except Exception as ex:
            log.warning("no se borró parcial_%s de Drive (no se pierde nada): %s", c, ex)
        (CHUNKS / c).unlink(missing_ok=True)
        uploaded_chunks.discard(c)


def upload_pending(rows_by_file):
    """Sube los Parquet horarios pendientes; los que fallen se reintentan en el siguiente bloque."""
    if not config.DRIVE_ENABLED:
        return
    from . import drive
    pending = PENDING
    pending.mkdir(parents=True, exist_ok=True)
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
        drop_chunks(hour_chunks.pop(path.name, []))


def close_period(streams, key, rows_by_file, hour_changed=True):
    """Cierra el parcial (10 min) de cada feed y lo respalda en Drive; al cambiar de hora une los parciales.
    Los agregados de Supabase se acumulan por hora: se envían en cada bloque (upsert) y se reinician al cambiar la hora."""
    for s in streams:
        res = s.close(key)
        if config.SUPABASE_ENABLED and s.agg is not None and res:
            try:
                from . import supabase_sink
                supabase_sink.push_aggregates(s.prefix, s.agg)
            except Exception as ex:
                log.warning("no se enviaron agregados de %s a Supabase: %s", s.prefix, ex)
        if hour_changed:
            s.reset_agg()
    upload_chunks()
    if hour_changed:
        consolidate(streams, key[0], key[1][:2], rows_by_file)
    upload_pending(rows_by_file)


def cleanup_partials():
    """Borra archivos .part de una ejecución anterior que se interrumpió (están incompletos)."""
    for d in (PENDING, CHUNKS):
        for p in d.glob("*.part") if d.exists() else []:
            log.warning("descartando archivo incompleto de una ejecución anterior: %s", p.name)
            p.unlink()


def recover_leftovers(streams, rows_by_file):
    """Si quedaron parciales cerrados de una ejecución anterior, los consolida y sube."""
    if not CHUNKS.exists():
        return
    hours = sorted({tuple(p.name.split("_")[1:3]) for p in CHUNKS.glob("*.parquet")})
    for day, hh in hours:
        consolidate(streams, day, hh, rows_by_file)
    if hours:
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

    cleanup_partials()
    streams = [s for s in build_streams() if s.url]
    rows_by_file = {}
    recover_leftovers(streams, rows_by_file)
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
        block = (now.minute // CHUNK_MINUTES) * CHUNK_MINUTES
        period = (now.strftime("%Y-%m-%d"), f"{now:%H}_{block:02d}")  # archivo: vp_2026-11-23_04_10_<ts>.parquet
        if current_hour and period != current_hour:
            close_period(streams, current_hour, rows_by_file,
                         hour_changed=period[1][:2] != current_hour[1][:2] or period[0] != current_hour[0])
        current_hour = period

        for s in streams:
            if not s.due(t0):
                continue
            s.last_poll = t0
            try:
                rows, dup, n = s.parser.parse(fetch(s.url), int(t0))
                s.buffer.extend(rows)
                s.errors, s.last_n = 0, n
                if rows:
                    s.last_new = t0
                if len(s.buffer) >= FLUSH_ROWS:
                    s.flush(current_hour)
            except Exception as ex:
                s.errors += 1
                log.warning("[%s] error (%d seguidos): %s", s.prefix, s.errors, ex)
                if s.errors * s.every >= 300:  # ~5 min fallando
                    ping(False)

        if t0 - last_ping > 300:
            last_ping = t0
            resumen = " | ".join(f"{s.prefix}: {s.rows_written + len(s.buffer)} filas (feed: {s.last_n} entidades)" for s in streams)
            vp = next((s for s in streams if s.prefix == "vp"), None)
            quieto = (t0 - vp.last_new) / 60 if vp else 0
            en_servicio = ACTIVE_FROM <= now.hour < ACTIVE_TO
            if any(s.errors for s in streams):
                log.warning("CON ERRORES | " + resumen)  # el aviso de error ya se envía arriba
            elif en_servicio and quieto >= STALE_MINUTES:
                ping(False)
                log.warning("FEED CONGELADO: vp sin datos nuevos hace %.0f min | %s", quieto, resumen)
            else:
                ping(True)
                log.info("ok | " + resumen)

        time.sleep(max(0.0, config.POLL_SECONDS - (time.time() - t0)))

    log.info("Deteniendo: guardando el bloque en curso…")
    if current_hour:
        close_period(streams, current_hour, rows_by_file)
    log.info("Detenido.")


if __name__ == "__main__":
    main()
