create or replace view analytics.import_status as
select
    dataset_key,
    dataset_name,
    frequency,
    count(*) filter (where status = 'completed') as completed_files,
    sum(imported_rows) filter (where status = 'completed') as completed_rows,
    min(file_date) filter (where status = 'completed') as first_file_date,
    max(file_date) filter (where status = 'completed') as last_file_date,
    max(imported_at) filter (where status = 'completed') as last_imported_at
from ingest.kabuplus_files
group by dataset_key, dataset_name, frequency;

create or replace view analytics.stock_prices_daily as
select
    security_code as sc,
    util.null_if_blank_or_dash(payload ->> '名称') as name,
    util.null_if_blank_or_dash(payload ->> '市場') as market,
    util.null_if_blank_or_dash(payload ->> '業種') as industry,
    coalesce(util.to_date_compact_or_null(payload ->> '日付'), record_date, file_date) as trade_date,
    util.to_numeric_or_null(payload ->> '株価') as close_price,
    util.to_numeric_or_null(payload ->> '前日比') as day_change,
    util.to_numeric_or_null(payload ->> '前日比（％）') as day_change_pct,
    util.to_numeric_or_null(payload ->> '前日終値') as previous_close_price,
    util.to_numeric_or_null(payload ->> '始値') as open_price,
    util.to_numeric_or_null(payload ->> '高値') as high_price,
    util.to_numeric_or_null(payload ->> '安値') as low_price,
    util.to_bigint_or_null(payload ->> '出来高') as volume,
    util.to_numeric_or_null(payload ->> '売買代金（千円）') as turnover_thousand_yen,
    util.to_numeric_or_null(payload ->> '時価総額（百万円）') as market_cap_million_yen,
    util.to_numeric_or_null(payload ->> '値幅下限') as price_limit_lower,
    util.to_numeric_or_null(payload ->> '値幅上限') as price_limit_upper,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'japan-all-stock-prices/daily';

create or replace view analytics.stock_prices_daily_extended as
select
    security_code as sc,
    util.null_if_blank_or_dash(payload ->> '名称') as name,
    util.null_if_blank_or_dash(payload ->> '市場') as market,
    util.null_if_blank_or_dash(payload ->> '業種') as industry,
    coalesce(util.to_date_compact_or_null(payload ->> '日付'), record_date, file_date) as trade_date,
    util.to_numeric_or_null(payload ->> '株価') as close_price,
    util.to_numeric_or_null(payload ->> '前日比') as day_change,
    util.to_numeric_or_null(payload ->> '前日比（％）') as day_change_pct,
    util.to_numeric_or_null(payload ->> '前日終値') as previous_close_price,
    util.to_numeric_or_null(payload ->> '始値') as open_price,
    util.to_numeric_or_null(payload ->> '高値') as high_price,
    util.to_numeric_or_null(payload ->> '安値') as low_price,
    util.to_numeric_or_null(payload ->> 'VWAP') as vwap,
    util.to_bigint_or_null(payload ->> '出来高') as volume,
    util.to_numeric_or_null(payload ->> '出来高率') as volume_ratio,
    util.to_numeric_or_null(payload ->> '売買代金（千円）') as turnover_thousand_yen,
    util.to_numeric_or_null(payload ->> '時価総額（百万円）') as market_cap_million_yen,
    util.to_numeric_or_null(payload ->> '値幅下限') as price_limit_lower,
    util.to_numeric_or_null(payload ->> '値幅上限') as price_limit_upper,
    util.to_date_compact_or_null(payload ->> '高値日付') as ytd_high_date,
    util.to_numeric_or_null(payload ->> '年初来高値') as ytd_high_price,
    util.to_numeric_or_null(payload ->> '年初来高値乖離率') as ytd_high_gap_pct,
    util.to_date_compact_or_null(payload ->> '安値日付') as ytd_low_date,
    util.to_numeric_or_null(payload ->> '年初来安値') as ytd_low_price,
    util.to_numeric_or_null(payload ->> '年初来安値乖離率') as ytd_low_gap_pct,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'japan-all-stock-prices-2/daily';

create or replace view analytics.stock_snapshot_daily as
select
    security_code as sc,
    util.null_if_blank_or_dash(payload ->> '名称') as name,
    util.null_if_blank_or_dash(payload ->> '市場') as market,
    util.null_if_blank_or_dash(payload ->> '業種') as industry,
    coalesce(util.to_date_compact_or_null(payload ->> '日付'), record_date, file_date) as snapshot_date,
    util.to_numeric_or_null(payload ->> '時価総額（百万円）') as market_cap_million_yen,
    util.to_bigint_or_null(payload ->> '発行済株式数') as shares_outstanding,
    util.to_numeric_or_null(payload ->> '配当利回り（予想）') as dividend_yield_forecast,
    util.to_numeric_or_null(payload ->> '1株配当（予想）') as dividend_per_share_forecast,
    util.to_numeric_or_null(payload ->> 'PER（予想）') as per_forecast,
    util.to_numeric_or_null(payload ->> 'PBR（実績）') as pbr_actual,
    util.to_numeric_or_null(payload ->> 'EPS（予想）') as eps_forecast,
    util.to_numeric_or_null(payload ->> 'BPS（実績）') as bps_actual,
    util.to_numeric_or_null(payload ->> '最低購入額') as minimum_purchase_amount,
    util.to_bigint_or_null(payload ->> '単元株') as unit_shares,
    util.to_date_compact_or_null(payload ->> '高値日付') as ytd_high_date,
    util.to_numeric_or_null(payload ->> '年初来高値') as ytd_high_price,
    util.to_date_compact_or_null(payload ->> '安値日付') as ytd_low_date,
    util.to_numeric_or_null(payload ->> '年初来安値') as ytd_low_price,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'japan-all-stock-data/daily';

create or replace view analytics.tosho_stock_ohlc_daily as
select
    security_code as sc,
    coalesce(util.to_date_compact_or_null(payload ->> '日付'), record_date, file_date) as trade_date,
    util.to_numeric_or_null(payload ->> '始値') as open_price,
    util.to_numeric_or_null(payload ->> '高値') as high_price,
    util.to_numeric_or_null(payload ->> '安値') as low_price,
    util.to_numeric_or_null(payload ->> '終値') as close_price,
    util.to_numeric_or_null(payload ->> 'VWAP') as vwap,
    util.to_bigint_or_null(payload ->> '出来高') as volume,
    util.to_numeric_or_null(payload ->> '売買代金') as turnover_yen,
    util.to_numeric_or_null(payload ->> '前場始値') as am_open_price,
    util.to_numeric_or_null(payload ->> '前場高値') as am_high_price,
    util.to_numeric_or_null(payload ->> '前場安値') as am_low_price,
    util.to_numeric_or_null(payload ->> '前場終値') as am_close_price,
    util.to_numeric_or_null(payload ->> '後場始値') as pm_open_price,
    util.to_numeric_or_null(payload ->> '後場高値') as pm_high_price,
    util.to_numeric_or_null(payload ->> '後場安値') as pm_low_price,
    util.to_numeric_or_null(payload ->> '後場終値') as pm_close_price,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'tosho-stock-ohlc/daily';

create or replace view analytics.financial_results_monthly as
select
    security_code as sc,
    util.null_if_blank_or_dash(payload ->> '名称') as name,
    util.to_date_compact_or_null(payload ->> '決算期') as fiscal_period_month,
    util.to_date_compact_or_null(payload ->> '決算発表日（本決算）') as full_year_announcement_date,
    util.to_numeric_or_null(payload ->> '売上高（百万円）') as revenue_million_yen,
    util.to_numeric_or_null(payload ->> '営業利益（百万円）') as operating_income_million_yen,
    util.to_numeric_or_null(payload ->> '経常利益（百万円）') as ordinary_income_million_yen,
    util.to_numeric_or_null(payload ->> '当期利益（百万円）') as net_income_million_yen,
    util.to_numeric_or_null(payload ->> '総資産（百万円）') as total_assets_million_yen,
    util.to_numeric_or_null(payload ->> '自己資本（百万円）') as equity_million_yen,
    util.to_numeric_or_null(payload ->> '資本金（百万円）') as capital_million_yen,
    util.to_numeric_or_null(payload ->> '有利子負債（百万円）') as interest_bearing_debt_million_yen,
    util.to_numeric_or_null(payload ->> '自己資本比率') as equity_ratio_pct,
    util.to_numeric_or_null(payload ->> 'ROE') as roe_pct,
    util.to_numeric_or_null(payload ->> 'ROA') as roa_pct,
    util.to_bigint_or_null(payload ->> '発行済株式数') as shares_outstanding,
    file_date as source_month,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'japan-all-stock-financial-results/monthly';

create or replace view analytics.listing_information_monthly as
select
    security_code as sc,
    util.to_date_compact_or_null(payload ->> '上場年月') as listed_month,
    file_date as source_month,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'japan-all-stock-information/monthly';

create or replace view analytics.margin_transactions_weekly as
select
    security_code as sc,
    file_date as week_date,
    util.to_bigint_or_null(payload ->> '信用買残高') as margin_buy_balance,
    util.to_bigint_or_null(payload ->> '信用買残高 前週比') as margin_buy_balance_wow,
    util.to_bigint_or_null(payload ->> '信用売残高') as margin_sell_balance,
    util.to_bigint_or_null(payload ->> '信用売残高 前週比') as margin_sell_balance_wow,
    util.to_numeric_or_null(payload ->> '貸借倍率') as margin_ratio,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'japan-all-stock-margin-transactions/weekly';

create or replace view analytics.corporate_actions_monthly as
select
    util.to_date_compact_or_null(payload ->> '分割併合日') as action_date,
    security_code as sc,
    util.to_numeric_or_null(payload ->> '分割併合比率') as split_ratio,
    file_date as source_month,
    source_zip,
    source_entry,
    loaded_at
from raw.kabuplus_records
where dataset_key = 'corporate-action/monthly';

create or replace view analytics.stock_prices_adjusted_daily as
with price_base as (
    select
        sc,
        name,
        market,
        industry,
        trade_date,
        open_price as raw_open_price,
        high_price as raw_high_price,
        low_price as raw_low_price,
        close_price as raw_close_price,
        previous_close_price as raw_previous_close_price,
        day_change as raw_day_change,
        day_change_pct as raw_day_change_pct,
        volume::numeric as raw_volume,
        turnover_thousand_yen as raw_turnover_thousand_yen,
        market_cap_million_yen as raw_market_cap_million_yen,
        price_limit_lower as raw_price_limit_lower,
        price_limit_upper as raw_price_limit_upper,
        source_zip,
        source_entry,
        loaded_at
    from analytics.stock_prices_daily
),
scored as (
    select
        p.*,
        coalesce(
            (
                select exp(sum(ln(ipa.price_multiplier::double precision)))::numeric(20, 10)
                from analytics.inferred_price_actions ipa
                where ipa.sc = p.sc
                  and ipa.action_date > p.trade_date
            ),
            1::numeric(20, 10)
        ) as adjustment_factor
    from price_base p
)
select
    sc,
    name,
    market,
    industry,
    trade_date,
    raw_open_price,
    raw_high_price,
    raw_low_price,
    raw_close_price,
    raw_previous_close_price,
    raw_day_change,
    raw_day_change_pct,
    raw_volume,
    raw_turnover_thousand_yen,
    raw_market_cap_million_yen,
    raw_price_limit_lower,
    raw_price_limit_upper,
    adjustment_factor,
    (raw_open_price * adjustment_factor)::numeric(20, 8) as adjusted_open_price,
    (raw_high_price * adjustment_factor)::numeric(20, 8) as adjusted_high_price,
    (raw_low_price * adjustment_factor)::numeric(20, 8) as adjusted_low_price,
    (raw_close_price * adjustment_factor)::numeric(20, 8) as adjusted_close_price,
    (raw_previous_close_price * adjustment_factor)::numeric(20, 8) as adjusted_previous_close_price,
    (raw_day_change * adjustment_factor)::numeric(20, 8) as adjusted_day_change,
    raw_day_change_pct as adjusted_day_change_pct,
    (raw_volume / nullif(adjustment_factor, 0))::numeric(20, 8) as adjusted_volume,
    (raw_open_price * adjustment_factor)::numeric(20, 8) as open_price,
    (raw_high_price * adjustment_factor)::numeric(20, 8) as high_price,
    (raw_low_price * adjustment_factor)::numeric(20, 8) as low_price,
    (raw_close_price * adjustment_factor)::numeric(20, 8) as close_price,
    (raw_previous_close_price * adjustment_factor)::numeric(20, 8) as previous_close_price,
    (raw_day_change * adjustment_factor)::numeric(20, 8) as day_change,
    raw_day_change_pct as day_change_pct,
    (raw_volume / nullif(adjustment_factor, 0))::numeric(20, 8) as volume,
    raw_turnover_thousand_yen as turnover_thousand_yen,
    raw_market_cap_million_yen as market_cap_million_yen,
    raw_price_limit_lower as price_limit_lower,
    raw_price_limit_upper as price_limit_upper,
    source_zip,
    source_entry,
    loaded_at
from scored
;
