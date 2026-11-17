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