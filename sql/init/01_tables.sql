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

create table if not exists analytics.inferred_price_actions (
    sc text not null,
    action_date date not null,
    action_type text not null check (action_type in ('split', 'reverse_split')),
    integer_factor integer not null check (integer_factor between 2 and 10),
    price_multiplier numeric(20, 10) not null check (price_multiplier > 0),
    detection_method text not null check (detection_method in ('ohlc_integer_jump')),
    evidence_json jsonb not null,
    created_at timestamptz not null default now(),
    primary key (sc, action_date, detection_method)
);

create index if not exists idx_inferred_price_actions_sc_action_date
    on analytics.inferred_price_actions (sc, action_date);

create index if not exists idx_inferred_price_actions_action_type
    on analytics.inferred_price_actions (action_type, integer_factor);

create index if not exists idx_inferred_price_actions_evidence
    on analytics.inferred_price_actions using gin (evidence_json jsonb_path_ops);

create table if not exists research.entry_study_runs (
    run_id text primary key,
    command_name text not null,
    source_relation text not null,
    train_start_date date not null,
    train_end_date date not null,
    validation_start_date date not null,
    validation_end_date date not null,
    parameters_json jsonb not null,
    manifest_path text,
    summary_path text,
    notes text,
    created_at timestamptz not null default now()
);

create index if not exists idx_entry_study_runs_created_at
    on research.entry_study_runs (created_at desc);

create table if not exists research.entry_cases (
    run_id text not null references research.entry_study_runs (run_id) on delete cascade,
    case_id text not null,
    sc text not null,
    name text,
    market text,
    industry text,
    trade_date date not null,
    trade_seq integer not null,
    dataset_split text not null check (dataset_split in ('train', 'validation', 'other')),
    label text not null check (label in ('trend', 'non_trend', 'neutral', 'incomplete')),
    label_reason text not null,
    lookback_obs integer not null,
    entry_price numeric(20, 8) not null,
    raw_close_price numeric(20, 8),
    adjusted_close_price numeric(20, 8),
    adjustment_factor numeric(20, 10),
    range_high numeric(20, 8),
    range_low numeric(20, 8),
    range_width_pct numeric(20, 10),
    breakout_margin_pct numeric(20, 10),
    day_return_pct numeric(20, 10),
    gap_pct numeric(20, 10),
    body_ratio numeric(20, 10),
    upper_wick_ratio numeric(20, 10),
    lower_wick_ratio numeric(20, 10),
    volume_ratio numeric(20, 10),
    is_bullish boolean,
    bullish_count_10 integer,
    bullish_count_20 integer,
    bullish_count_60 integer,
    up_day_count_10 integer,
    up_day_count_20 integer,
    up_day_count_60 integer,
    high_volume_bullish_count_20 integer,
    high_volume_bullish_count_60 integer,
    long_upper_wick_count_20 integer,
    long_upper_wick_count_60 integer,
    long_lower_wick_count_20 integer,
    long_lower_wick_count_60 integer,
    prior_return_20d_pct numeric(20, 10),
    prior_return_60d_pct numeric(20, 10),
    ma_gap_20_pct numeric(20, 10),
    ma_gap_60_pct numeric(20, 10),
    ma_slope_20_pct numeric(20, 10),
    ma_slope_60_pct numeric(20, 10),
    range_high_touch_count_120 integer,
    higher_high_count_20 integer,
    higher_low_count_20 integer,
    atr_20_pct numeric(20, 10),
    atr_20_to_range_ratio numeric(20, 10),
    return_20d_pct numeric(20, 10),
    return_60d_pct numeric(20, 10),
    return_120d_pct numeric(20, 10),
    return_240d_pct numeric(20, 10),
    future_max_return_240d_pct numeric(20, 10),
    future_min_return_60d_pct numeric(20, 10),
    created_at timestamptz not null default now(),
    primary key (run_id, case_id)
);

create index if not exists idx_entry_cases_run_split_label
    on research.entry_cases (run_id, dataset_split, label);

create index if not exists idx_entry_cases_run_sc_trade_date
    on research.entry_cases (run_id, sc, trade_date);

create table if not exists research.entry_hypotheses (
    run_id text not null references research.entry_study_runs (run_id) on delete cascade,
    hypothesis_id text not null,
    stage text not null check (stage in ('train', 'validation')),
    rule_name text not null,
    rule_json jsonb not null,
    metrics_json jsonb not null,
    created_at timestamptz not null default now(),
    primary key (run_id, hypothesis_id, stage)
);

create index if not exists idx_entry_hypotheses_run_stage
    on research.entry_hypotheses (run_id, stage);
