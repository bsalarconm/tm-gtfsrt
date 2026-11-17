"""Configuración leída de variables de entorno."""
import os
from pathlib import Path
try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python 3.8
    from backports.zoneinfo import ZoneInfo

# Carga un archivo .env local si existe (para pruebas en tu PC; en Render se usan las variables del panel)
_env = Path(".env")
if _env.exists():
    for _line in _env.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())


def _bool(name, default="0"):
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "si")


TZ = ZoneInfo(os.environ.get("TZ_NAME", "America/Bogota"))

# Feeds GTFS-Realtime de TransMilenio (https://gtfs.transmilenio.gov.co/)
FEED_URL = os.environ.get("FEED_URL", "https://gtfs.transmilenio.gov.co/positions.pb")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "20"))
TRIPUPDATES_URL = os.environ.get("TRIPUPDATES_URL", "https://gtfs.transmilenio.gov.co/tripupdates.pb")
TRIPUPDATES_POLL_SECONDS = int(os.environ.get("TRIPUPDATES_POLL_SECONDS", "300"))
ALERTS_URL = os.environ.get("ALERTS_URL", "https://gtfs.transmilenio.gov.co/alerts.pb")
ALERTS_POLL_SECONDS = int(os.environ.get("ALERTS_POLL_SECONDS", "60"))
API_HEADER = os.environ.get("API_HEADER", "")  # ej. "x-api-key: ABC123" si el feed lo pide

# Carpeta temporal local (en Render es efímera: los archivos se suben y se borran)
WORK_DIR = Path(os.environ.get("WORK_DIR", "/tmp/tm_gtfsrt"))
KEEP_LOCAL = _bool("KEEP_LOCAL", "0")  # 1 = no borrar el Parquet local tras subirlo (útil en pruebas)

# Google Drive (archivo histórico)
DRIVE_ENABLED = _bool("DRIVE_ENABLED", "1")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")

# Supabase (indicadores agregados)
SUPABASE_ENABLED = _bool("SUPABASE_ENABLED", "1")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/").removesuffix("/rest/v1") if hasattr(str, "removesuffix") else os.environ.get("SUPABASE_URL", "").rstrip("/").replace("/rest/v1", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
AGG_MINUTES = int(os.environ.get("AGG_MINUTES", "60"))

# Monitoreo (healthchecks.io u otro "dead man's switch")
HEALTHCHECK_URL = os.environ.get("HEALTHCHECK_URL", "")


def validate():
    errors = []
    if not FEED_URL:
        errors.append("FEED_URL")
    if DRIVE_ENABLED and not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN, DRIVE_FOLDER_ID]):
        errors.append("GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN / DRIVE_FOLDER_ID (o DRIVE_ENABLED=0)")
    if SUPABASE_ENABLED and not (SUPABASE_URL and SUPABASE_SERVICE_KEY):
        errors.append("SUPABASE_URL / SUPABASE_SERVICE_KEY (o SUPABASE_ENABLED=0)")
    if errors:
        raise SystemExit("Faltan variables de entorno: " + "; ".join(errors))
