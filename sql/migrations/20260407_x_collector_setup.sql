create schema if not exists ingest;
create schema if not exists raw;
create schema if not exists analytics;

create table if not exists ingest.x_monitored_accounts (
    target_username text primary key,
    target_user_id text unique,
    is_active boolean not null default true,
    last_resolved_at timestamptz,
    last_access_check_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_x_monitored_accounts_active
    on ingest.x_monitored_accounts (is_active, target_username);

create table if not exists ingest.x_timeline_state (
    target_user_id text primary key,
    since_id text,
    last_polled_at timestamptz,
    last_success_at timestamptz,
    last_seen_post_id text,
    last_seen_created_at timestamptz,
    consecutive_failures integer not null default 0,
    last_http_status integer,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists ingest.x_poll_runs (
    run_id bigserial primary key,
    run_mode text not null check (run_mode in ('poll_once', 'daemon')),
    status text not null check (status in ('running', 'completed', 'completed_with_errors', 'failed')),
    target_count integer not null default 0,
    success_count integer not null default 0,
    failure_count integer not null default 0,
    fetched_post_count integer not null default 0,
    inserted_post_count integer not null default 0,
    updated_post_count integer not null default 0,
    last_error text,
    started_at timestamptz not null default now(),
    finished_at timestamptz
);

create index if not exists idx_x_poll_runs_started_at
    on ingest.x_poll_runs (started_at desc);

create table if not exists ingest.x_usage_daily (
    usage_date date not null,
    app_id text not null,
    project_id text,
    project_cap bigint,
    posts_consumed bigint not null,
    raw_payload jsonb not null,
    fetched_at timestamptz not null default now(),
    primary key (usage_date, app_id)
);

create table if not exists raw.x_users (
    user_id text primary key,
    username text not null,
    name text,
    protected boolean,
    verified boolean,
    payload jsonb not null,
    updated_at timestamptz not null default now()
);

create index if not exists idx_x_users_username
    on raw.x_users (username);

create table if not exists raw.x_posts (
    post_id text primary key,
    author_user_id text not null,
    author_username text not null,
    created_at timestamptz not null,
    text text not null,
    conversation_id text,
    lang text,
    public_metrics_json jsonb not null,
    referenced_posts_json jsonb,
    entities_json jsonb,
    attachments_json jsonb,
    payload jsonb not null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now()
);

create index if not exists idx_x_posts_author_created_at
    on raw.x_posts (author_user_id, created_at desc);

create index if not exists idx_x_posts_created_at
    on raw.x_posts (created_at desc);

create or replace view analytics.monitored_x_posts as
select
    (p.created_at at time zone 'Asia/Tokyo')::date as post_date_jst,
    p.created_at at time zone 'Asia/Tokyo' as created_at_jst,
    m.target_username,
    p.author_user_id,
    p.author_username,
    u.name as author_name,
    u.protected as is_protected_source,
    p.post_id,
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
