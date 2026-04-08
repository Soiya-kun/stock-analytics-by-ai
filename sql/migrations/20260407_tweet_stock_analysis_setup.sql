create schema if not exists analytics;
create schema if not exists raw;
create schema if not exists research;

create table if not exists research.tweet_analysis_runs (
    run_id text primary key,
    command_name text not null,
    source_relation text not null,
    company_relation text not null,
    start_date date not null,
    end_date date not null,
    target_username text,
    parameters_json jsonb not null,
    manifest_path text,
    summary_path text,
    notes text,
    created_at timestamptz not null default now()
);

create index if not exists idx_tweet_analysis_runs_created_at
    on research.tweet_analysis_runs (created_at desc);

create table if not exists research.tweet_stock_mentions (
    run_id text not null references research.tweet_analysis_runs (run_id) on delete cascade,
    mention_id text not null,
    post_id text not null references raw.x_posts (post_id),
    target_username text not null,
    author_user_id text not null,
    author_username text not null,
    post_created_at timestamptz not null,
    post_date_jst date not null,
    tweet_url text not null,
    tweet_text text not null,
    sc text not null,
    company_name text not null,
    market text,
    industry text,
    match_confidence text not null check (match_confidence in ('high', 'medium', 'low')),
    extraction_rationale text not null,
    tweet_session text check (tweet_session in ('pre_market', 'intraday', 'after_market', 'non_trading_day', 'unknown')),
    event_trade_date date,
    previous_trade_date date,
    next_trade_date date,
    previous_close_price numeric(20, 8),
    event_open_price numeric(20, 8),
    event_high_price numeric(20, 8),
    event_low_price numeric(20, 8),
    event_close_price numeric(20, 8),
    next_close_price numeric(20, 8),
    event_volume numeric(20, 8),
    avg_volume_20d numeric(20, 8),
    volume_ratio_20d numeric(20, 10),
    event_day_return_pct numeric(20, 10),
    intraday_peak_return_pct numeric(20, 10),
    max_close_return_5d_pct numeric(20, 10),
    max_close_return_20d_pct numeric(20, 10),
    volume_spike_flag boolean not null,
    volume_spike_reason text not null,
    price_jump_flag boolean not null,
    price_jump_reason text not null,
    analysis_summary text not null,
    analysis_json jsonb not null,
    created_at timestamptz not null default now(),
    primary key (run_id, mention_id)
);

create index if not exists idx_tweet_stock_mentions_run_sc
    on research.tweet_stock_mentions (run_id, sc);

create index if not exists idx_tweet_stock_mentions_run_post
    on research.tweet_stock_mentions (run_id, post_id);

create index if not exists idx_tweet_stock_mentions_flags
    on research.tweet_stock_mentions (volume_spike_flag, price_jump_flag, post_date_jst);

drop view if exists analytics.monitored_x_posts;

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

drop view if exists analytics.listed_companies_latest;

create or replace view analytics.listed_companies_latest as
with latest_daily_file as (
    select max(file_date) as file_date
    from ingest.kabuplus_files
    where dataset_key = 'japan-all-stock-prices/daily'
      and status = 'completed'
),
latest_rows as (
    select distinct on (security_code)
        security_code as sc,
        util.null_if_blank_or_dash(payload ->> '名称') as name,
        util.null_if_blank_or_dash(payload ->> '市場') as market,
        util.null_if_blank_or_dash(payload ->> '業種') as industry,
        coalesce(util.to_date_compact_or_null(payload ->> '日付'), record_date, file_date) as trade_date,
        util.to_numeric_or_null(payload ->> '株価') as close_price,
        util.to_bigint_or_null(payload ->> '出来高') as volume,
        util.to_numeric_or_null(payload ->> '時価総額（百万円）') as market_cap_million_yen
    from raw.kabuplus_records
    where dataset_key = 'japan-all-stock-prices/daily'
      and record_date = (select file_date from latest_daily_file)
      and security_code is not null
    order by security_code, row_number
)
select
    sc,
    name,
    market,
    industry,
    trade_date,
    close_price,
    volume,
    market_cap_million_yen
from latest_rows;
