# Recolector GTFS-Realtime de TransMilenio

Guarda los 3 feeds GTFS-Realtime de TransMilenio (https://gtfs.transmilenio.gov.co/) y construye un
**histórico abierto de la operación de TransMilenio**:

| Feed | Qué trae | Consulta | Se guarda |
|---|---|---|---|
| `positions.pb` | posición, viaje, ruta, bus, placa y parada de cada bus (sin velocidad: se calcula) | cada 20 s | cada lectura nueva |
| `tripupdates.pb` | cada viaje del día: programado / agregado / cancelado, retraso, bus asignado | cada 5 min | solo los viajes que cambiaron |
| `alerts.pb` | novedades de operación (desvíos por ciclovía, lluvias, planes especiales) | cada 1 min | cuando aparecen, cambian o se retiran |

En Drive queda un Parquet por hora y por feed: `vp_…` (posiciones), `tu_…` (viajes), `al_…` (alertas).

- **Google Drive**: un archivo Parquet por hora con todas las lecturas (archivo histórico completo).
- **Supabase**: indicadores agregados por ruta y ventana de tiempo + inventario de archivos subidos.
- **healthchecks.io**: alerta por correo si el recolector deja de funcionar.
- **Render**: corre el recolector 24/7 como *Background Worker*.

```
tm/
├── tm_collector/        código del recolector
│   ├── __main__.py      bucle principal (python -m tm_collector)
│   ├── config.py        variables de entorno
│   ├── feed.py          descarga y lectura del feed
│   ├── drive.py         subida a Google Drive
│   └── supabase_sink.py agregados e inventario en Supabase
├── scripts/get_drive_token.py   autorización de Google Drive (se corre una vez en tu PC)
├── supabase/schema.sql          tablas de Supabase
├── render.yaml                  despliegue en Render
├── requirements.txt
└── .env.example
```

---

## Paso 0. URL del feed

Las tres URL ya vienen configuradas por defecto (`positions.pb`, `tripupdates.pb`, `alerts.pb`
en https://gtfs.transmilenio.gov.co/). Se actualizan cada ~26 s y no piden clave.

## Paso 1. Prueba local (Windows)

```powershell
cd C:\Users\ASUS\Documents\2026\tm
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env      # y pega la URL en FEED_URL
python -m tm_collector --once
```

Debe mostrar, para cada uno de los 3 feeds, cuántas entidades trae, qué campos vienen con datos y 2 filas de ejemplo.
Si sale un error, revisa la URL o si el feed pide autenticación.

## Paso 2. Google Drive (una sola vez)

1. Entra a https://console.cloud.google.com, crea un proyecto (ej. `tm-gtfsrt`).
2. **APIs y servicios > Biblioteca** → habilita **Google Drive API**.
3. **Pantalla de consentimiento OAuth** → tipo *Externo*, agrega tu correo como usuario.
   **Importante:** cambia el estado de publicación a **"En producción"**. Si queda en *Prueba*,
   el token vence a los 7 días y el recolector deja de subir archivos.
4. **Credenciales > Crear credenciales > ID de cliente OAuth** → tipo **Aplicación de escritorio**.
   Descarga el JSON y guárdalo como `scripts\client_secret.json` (está en `.gitignore`).
5. Ejecuta:
   ```powershell
   pip install google-auth-oauthlib
   python scripts\get_drive_token.py
   ```
   Se abre el navegador para autorizar. Al final imprime `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`,
   `GOOGLE_REFRESH_TOKEN` y `DRIVE_FOLDER_ID`, y crea la carpeta `TM_GTFSRT_historico` en tu Drive.
   Pégalos en tu `.env` (para pruebas) y luego en Render.

> El permiso `drive.file` solo deja a la app ver los archivos que ella misma crea, no el resto de tu Drive.

## Paso 3. Supabase

1. Crea un proyecto en https://supabase.com.
2. **SQL Editor** → pega y ejecuta `supabase/schema.sql`.
3. **Project Settings > API**: copia la **Project URL** (`SUPABASE_URL`) y la clave **service_role**
   (`SUPABASE_SERVICE_KEY`). La service_role es secreta: solo va en Render y en tu `.env`.

Capacidad: con `AGG_MINUTES=60` se generan del orden de unas pocas decenas de miles de filas al día
(según el número de rutas); vigila el tamaño en *Settings > Usage* frente al límite del plan gratuito.
Si se acerca, sube `AGG_MINUTES` o pasa a un plan pago.

## Paso 4. Alertas

En https://healthchecks.io crea un *check* con periodo de 10 minutos y gracia de 10 minutos.
Copia su URL de ping en `HEALTHCHECK_URL`. Te llegará un correo si el recolector deja de reportar
o si lleva ~5 minutos sin poder leer el feed o subir archivos.

## Paso 5. Prueba local completa

Con el `.env` lleno:

```powershell
python -m tm_collector
```

Déjalo correr hasta que cambie la hora (o detenlo con Ctrl+C: guarda la hora en curso y la sube).
Verifica que aparezca el Parquet en tu carpeta de Drive y filas en las tablas de Supabase.

## Paso 6. Git y Render

```powershell
git init
git add .
git commit -m "Recolector GTFS-RT TransMilenio"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/tm-gtfsrt.git
git push -u origin main
```

Verifica antes del push que `.env` y `scripts/client_secret.json` **no** aparezcan en `git status`.

En Render: **New > Blueprint** → conecta el repositorio → detecta `render.yaml` y te pide los secretos
(`FEED_URL`, Google, Supabase, `HEALTHCHECK_URL`). El servicio es un *Background Worker* en plan
*Starter* (de pago: los workers no tienen plan gratuito).

Revisa los *Logs* de Render: cada 5 minutos debe aparecer una línea `ok: ... entidades`.

## Operación

- **Reinicios/deploys**: Render envía una señal de parada; el recolector guarda y sube la hora en curso.
  Si un reinicio es abrupto, se pierde como máximo la hora en memoria.
- **Fallas de subida**: los Parquet quedan pendientes y se reintentan en la siguiente hora
  (mientras el contenedor no se reinicie).
- **Espacio en Drive**: del orden de cientos de MB por día. Revisa tu cuota de Google Drive.
- **Leer los datos**:
  ```python
  import pyarrow.dataset as ds
  tabla = ds.dataset("ruta/a/los/parquet", format="parquet").to_table()
  df = tabla.to_pandas()
  ```

## Campos guardados

- **vp (posiciones):** `poll_ts`, `feed_ts`, `vehicle_ts` (epoch s) · `vehicle_id`, `vehicle_label`, `license_plate` ·
  `trip_id`, `route_id`, `start_date`, `start_time` · `lat`, `lon` · `speed_calc_ms` (calculada) · `stop_id`,
  `current_stop_sequence`. El feed hoy no trae `speed`, `bearing`, `current_status` ni `occupancy_status` (quedan vacíos).
- **tu (viajes):** `trip_id`, `route_id`, `start_date`, `start_time`, `schedule_relationship`
  (0 programado, 1 agregado, 3 cancelado), `delay_s`, `vehicle_id`, `vehicle_label`, `license_plate`, `tu_ts`.
- **al (alertas):** `alert_id`, `content_hash`, `is_deleted`, `cause`, `effect`, `header_text`, `description_text`,
  `active_start`, `active_end`, `route_ids`, `stop_ids`, `trip_ids`.

Notas: algunos buses reportan posiciones con días de antigüedad ("fantasma"); filtra por `poll_ts - vehicle_ts`.
El feed de alertas puede repetir un mismo `alert_id` con contenidos distintos.
