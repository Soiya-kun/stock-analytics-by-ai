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
