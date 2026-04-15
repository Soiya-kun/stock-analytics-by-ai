create schema if not exists ingest;
create schema if not exists raw;
create schema if not exists analytics;
create schema if not exists research;

alter table ingest.x_monitored_accounts
    add column if not exists account_role text;

update ingest.x_monitored_accounts
set account_role = 'benchmark'
where account_role is null;

alter table ingest.x_monitored_accounts
    alter column account_role set default 'benchmark';

alter table ingest.x_monitored_accounts
    alter column account_role set not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'x_monitored_accounts_account_role_check'
    ) then
        alter table ingest.x_monitored_accounts
            add constraint x_monitored_accounts_account_role_check
            check (account_role in ('benchmark', 'candidate'));
    end if;
end
$$;

alter table ingest.x_monitored_accounts
    add column if not exists benchmark_weight numeric(10, 4);

update ingest.x_monitored_accounts
set benchmark_weight = 1.0
where benchmark_weight is null;

alter table ingest.x_monitored_accounts
    alter column benchmark_weight set default 1.0;

alter table ingest.x_monitored_accounts
    alter column benchmark_weight set not null;

alter table ingest.x_monitored_accounts
    add column if not exists evaluation_notes text;

create table if not exists research.x_signal_analysis_runs (
    run_id text primary key,
    command_name text not null,
    source_relation text not null,
    start_date date not null,
    end_date date not null,
    account_role text not null check (account_role in ('benchmark', 'candidate', 'all')),
    target_username text,
    batch_size integer,
    parameters_json jsonb not null,
    manifest_path text,
    summary_path text,
    notes text,
    created_at timestamptz not null default now()
);

create index if not exists idx_x_signal_analysis_runs_created_at
    on research.x_signal_analysis_runs (created_at desc);

create table if not exists research.x_signal_analysis_post_reviews (
    post_id text primary key references raw.x_posts (post_id) on delete cascade,
    target_username text not null,
    account_role text not null check (account_role in ('benchmark', 'candidate')),
    source_run_id text not null references research.x_signal_analysis_runs (run_id),
    review_status text not null check (review_status in ('reviewed', 'needs_revisit')),
    analysis_version text not null,
    review_json jsonb not null,
    reviewed_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_x_signal_analysis_post_reviews_target
    on research.x_signal_analysis_post_reviews (target_username, reviewed_at desc);

create table if not exists research.x_post_stock_signals (
    post_id text not null references raw.x_posts (post_id) on delete cascade,
    sc text not null,
    author_username text not null,
    target_username text not null,
    account_role text not null check (account_role in ('benchmark', 'candidate')),
    post_created_at timestamptz not null,
    tweet_url text not null,
    tweet_text text not null,
    company_name text not null,
    match_confidence text not null check (match_confidence in ('high', 'medium', 'low')),
    extraction_rationale text not null,
    signal_label text not null check (signal_label in ('bullish', 'non_bullish', 'irrelevant')),
    signal_confidence text not null check (signal_confidence in ('high', 'medium', 'low')),
    signal_rationale text not null,
    tweet_session text check (tweet_session in ('pre_market', 'intraday', 'after_market', 'non_trading_day', 'unknown')),
    event_trade_date date,
    previous_close_price numeric(20, 8),
    event_close_price numeric(20, 8),
    volume_ratio_20d numeric(20, 10),
    max_close_return_5d_pct numeric(20, 10),
    max_close_return_20d_pct numeric(20, 10),
    analysis_version text not null,
    source_run_id text not null references research.x_signal_analysis_runs (run_id),
    analysis_json jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (post_id, sc)
);

create index if not exists idx_x_post_stock_signals_target_created_at
    on research.x_post_stock_signals (target_username, post_created_at desc);

create index if not exists idx_x_post_stock_signals_sc_created_at
    on research.x_post_stock_signals (sc, post_created_at desc);

create index if not exists idx_x_post_stock_signals_signal_label
    on research.x_post_stock_signals (signal_label, post_created_at desc);

create table if not exists research.x_account_trust_runs (
    run_id text primary key,
    command_name text not null,
    source_relation text not null,
    start_date date not null,
    end_date date not null,
    candidate_username text,
    parameters_json jsonb not null,
    manifest_path text,
    summary_path text,
    notes text,
    created_at timestamptz not null default now()
);

create index if not exists idx_x_account_trust_runs_created_at
    on research.x_account_trust_runs (created_at desc);

create table if not exists research.x_account_trust_clusters (
    run_id text not null references research.x_account_trust_runs (run_id) on delete cascade,
    candidate_username text not null,
    cluster_id text not null,
    sc text not null,
    company_name text,
    cluster_start_at timestamptz not null,
    cluster_end_at timestamptz not null,
    candidate_first_post_id text not null,
    candidate_first_post_at timestamptz not null,
    candidate_signal_count integer not null,
    benchmark_overlap boolean not null,
    benchmark_first_post_at timestamptz,
    candidate_beat_benchmark boolean,
    lead_hours numeric(20, 8),
    unique_pick boolean not null,
    cluster_success boolean,
    cluster_success_return_pct numeric(20, 10),
    benchmark_signal_count integer not null default 0,
    benchmark_user_count integer not null default 0,
    candidate_post_ids jsonb not null,
    benchmark_usernames jsonb not null,
    details_json jsonb not null,
    created_at timestamptz not null default now(),
    primary key (run_id, candidate_username, cluster_id)
);

create index if not exists idx_x_account_trust_clusters_run_candidate
    on research.x_account_trust_clusters (run_id, candidate_username);

create index if not exists idx_x_account_trust_clusters_run_sc
    on research.x_account_trust_clusters (run_id, sc);

create table if not exists research.x_account_trust_scores (
    run_id text not null references research.x_account_trust_runs (run_id) on delete cascade,
    candidate_username text not null,
    benchmark_overlap_rate numeric(20, 10),
    early_overlap_rate numeric(20, 10),
    unique_pick_success_rate numeric(20, 10),
    median_lead_hours numeric(20, 8),
    bullish_cluster_count integer not null,
    overlap_cluster_count integer not null,
    early_overlap_count integer not null,
    unique_pick_count integer not null,
    successful_unique_pick_count integer not null,
    insufficient_data_flag boolean not null,
    trust_score numeric(20, 10) not null,
    verdict text not null check (verdict in ('trusted_candidate', 'watch', 'low_confidence', 'insufficient_data')),
    summary_json jsonb not null,
    created_at timestamptz not null default now(),
    primary key (run_id, candidate_username)
);

create index if not exists idx_x_account_trust_scores_candidate_created_at
    on research.x_account_trust_scores (candidate_username, created_at desc);

drop view if exists analytics.monitored_x_posts;

create view analytics.monitored_x_posts as
select
    (p.created_at at time zone 'Asia/Tokyo')::date as post_date_jst,
    p.created_at at time zone 'Asia/Tokyo' as created_at_jst,
    m.target_username,
    m.account_role,
    m.benchmark_weight,
    m.evaluation_notes,
    p.author_user_id,
    p.author_username,
    u.name as author_name,
    u.protected as is_protected_source,
    p.post_id,
    ('https://x.com/' || p.author_username || '/status/' || p.post_id) as tweet_url,
    p.conversation_id,
    p.lang,
    p.text,
    util.to_bigint_or_null(p.public_metrics_json ->> 'like_count') as like_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'reply_count') as reply_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'quote_count') as quote_count,
    util.to_bigint_or_null(coalesce(
        p.public_metrics_json ->> 'repost_count',
        p.public_metrics_json ->> 'retweet_count'
    )) as repost_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'bookmark_count') as bookmark_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'impression_count') as impression_count,
    p.created_at,
    p.first_seen_at,
    p.last_seen_at,
    p.public_metrics_json,
    p.referenced_posts_json,
    p.entities_json,
    p.attachments_json,
    p.payload
from raw.x_posts p
join ingest.x_monitored_accounts m
    on m.target_user_id = p.author_user_id
left join raw.x_users u
    on u.user_id = p.author_user_id
where m.is_active;

create or replace view analytics.x_bullish_stock_signals as
select
    (s.post_created_at at time zone 'Asia/Tokyo')::date as post_date_jst,
    s.post_created_at at time zone 'Asia/Tokyo' as post_created_at_jst,
    s.post_id,
    s.sc,
    s.company_name,
    s.author_username,
    s.target_username,
    s.account_role,
    coalesce(m.benchmark_weight, 1.0) as benchmark_weight,
    s.post_created_at,
    s.tweet_url,
    s.tweet_text,
    s.signal_confidence,
    s.match_confidence,
    s.tweet_session,
    s.event_trade_date,
    s.previous_close_price,
    s.event_close_price,
    s.volume_ratio_20d,
    s.max_close_return_5d_pct,
    s.max_close_return_20d_pct,
    s.analysis_version,
    s.source_run_id,
    s.created_at,
    s.updated_at
from research.x_post_stock_signals s
left join ingest.x_monitored_accounts m
    on m.target_username = s.target_username
where s.signal_label = 'bullish';

create or replace view analytics.x_account_trust_latest as
select distinct on (s.candidate_username)
    s.candidate_username,
    s.run_id,
    r.start_date,
    r.end_date,
    s.benchmark_overlap_rate,
    s.early_overlap_rate,
    s.unique_pick_success_rate,
    s.median_lead_hours,
    s.bullish_cluster_count,
    s.unique_pick_count,
    s.insufficient_data_flag,
    s.trust_score,
    s.verdict,
    r.manifest_path,
    r.summary_path,
    r.created_at as run_created_at
from research.x_account_trust_scores s
join research.x_account_trust_runs r
    on r.run_id = s.run_id
order by s.candidate_username, r.created_at desc, s.created_at desc;
