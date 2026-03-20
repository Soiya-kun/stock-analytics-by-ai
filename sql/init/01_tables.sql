create table if not exists ingest.kabuplus_files (
    source_zip text not null,
    source_entry text not null,
    dataset_key text not null,
    dataset_name text not null,
    frequency text not null,
    source_file_name text not null,
    file_date date,
    file_size bigint not null,
    zip_crc bigint not null,
    status text not null check (status in ('running', 'completed', 'failed')),
    imported_rows integer,
    imported_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (source_zip, source_entry)
);

create table if not exists raw.kabuplus_records (
    id bigserial primary key,
    dataset_key text not null,
    dataset_name text not null,
    frequency text not null,
    source_zip text not null,
    source_entry text not null,
    source_file_name text not null,
    file_date date,
    record_date date,
    security_code text,
    row_number integer not null,
    payload jsonb not null,
    loaded_at timestamptz not null default now(),
    unique (source_zip, source_entry, row_number)
);

create index if not exists idx_kabuplus_files_status
    on ingest.kabuplus_files (status, dataset_key, file_date);

create index if not exists idx_kabuplus_records_dataset_date
    on raw.kabuplus_records (dataset_key, record_date);

create index if not exists idx_kabuplus_records_security_date
    on raw.kabuplus_records (security_code, record_date);

create index if not exists idx_kabuplus_records_payload
    on raw.kabuplus_records using gin (payload jsonb_path_ops);
