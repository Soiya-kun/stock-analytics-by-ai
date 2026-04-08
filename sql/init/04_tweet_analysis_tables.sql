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
