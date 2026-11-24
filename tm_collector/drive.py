"""Subida de archivos a Google Drive con OAuth (refresh token de tu cuenta)."""
import logging
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from . import config

log = logging.getLogger("drive")
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
_service = None


def _svc():
    global _service
    if _service is None:
        creds = Credentials(
            token=None,
            refresh_token=config.GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=config.GOOGLE_CLIENT_ID,
            client_secret=config.GOOGLE_CLIENT_SECRET,
            scopes=SCOPES,
        )
        _service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _service


def upload(path: Path, name: str) -> str:
    """Sube un archivo a la carpeta DRIVE_FOLDER_ID y devuelve su id en Drive."""
    media = MediaFileUpload(str(path), mimetype="application/octet-stream", resumable=True)
    meta = {"name": name, "parents": [config.DRIVE_FOLDER_ID]}
    f = _svc().files().create(body=meta, media_body=media, fields="id,size").execute()
    log.info("subido a Drive: %s (%s bytes)", name, f.get("size"))
    return f["id"]


def delete_by_name(name: str) -> int:
    """Borra de la carpeta los archivos con ese nombre (solo los que creó esta app). Devuelve cuántos borró."""
    q = f"name = '{name}' and '{config.DRIVE_FOLDER_ID}' in parents and trashed = false"
    files = _svc().files().list(q=q, fields="files(id)", spaces="drive").execute().get("files", [])
    for f in files:
        _svc().files().delete(fileId=f["id"]).execute()
    return len(files)
