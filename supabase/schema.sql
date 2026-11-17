-- Ejecutar en Supabase > SQL Editor

-- Posiciones: por ventana y ruta
create table if not exists public.vp_agg (
    bucket_start  timestamptz not null,
    agg_minutes   smallint    not null,
    route_id      text        not null,
    readings      integer     not null,
    vehicles      integer     not null,
    avg_speed_ms  real,                 -- velocidad calculada entre posiciones seguidas
    inserted_at   timestamptz not null default now(),
    primary key (bucket_start, agg_minutes, route_id)
);
create index if not exists vp_agg_route_idx on public.vp_agg (route_id, bucket_start);

-- Viajes: programados vistos, cancelados, agregados y retraso promedio
create table if not exists public.tu_agg (
    bucket_start  timestamptz not null,
    agg_minutes   smallint    not null,
    route_id      text        not null,
    trips         integer     not null,
    canceled      integer     not null,
    added         integer     not null,
    avg_delay_s   real,
    inserted_at   timestamptz not null default now(),
    primary key (bucket_start, agg_minutes, route_id)
);
create index if not exists tu_agg_route_idx on public.tu_agg (route_id, bucket_start);

-- Alertas: cada aparición, cambio o retiro
create table if not exists public.alerts_log (
    alert_id          text        not null,
    content_hash      text        not null,
    is_deleted        boolean     not null,
    seen_at           timestamptz not null,
    header_text       text,
    description_text  text,
    route_ids         text,
    active_start      timestamptz,
    active_end        timestamptz,
    primary key (alert_id, content_hash, is_deleted)
);

-- Inventario de los Parquet subidos a Google Drive
create table if not exists public.collector_files (
    file_name      text primary key,
    rows           integer,
    size_bytes     bigint,
    drive_file_id  text,
    uploaded_at    timestamptz not null default now()
);

alter table public.vp_agg enable row level security;
alter table public.tu_agg enable row level security;
alter table public.alerts_log enable row level security;
alter table public.collector_files enable row level security;

-- Salud del recolector: archivos por día y tipo (vp = posiciones, tu = viajes, al = alertas)
create or replace view public.collector_daily as
select (uploaded_at at time zone 'America/Bogota')::date as dia,
       split_part(file_name, '_', 1) as tipo,
       count(*)   as archivos,
       sum(rows)  as filas,
       round(sum(size_bytes) / 1e6, 1) as mb
from public.collector_files
group by 1, 2
order by 1 desc, 2;
