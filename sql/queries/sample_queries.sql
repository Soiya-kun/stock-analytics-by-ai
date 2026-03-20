select *
from analytics.import_status
order by dataset_key;

select
    sc,
    trade_date,
    close_price,
    day_change_pct,
    volume
from analytics.stock_prices_daily
where sc = '1301'
order by trade_date desc
limit 10;

select
    p.sc,
    p.trade_date,
    p.close_price,
    s.per_forecast,
    s.pbr_actual,
    s.dividend_yield_forecast
from analytics.stock_prices_daily as p
join analytics.stock_snapshot_daily as s
    on s.sc = p.sc
   and s.snapshot_date = p.trade_date
where p.sc = '1301'
order by p.trade_date desc
limit 10;

select
    f.sc,
    f.fiscal_period_month,
    f.revenue_million_yen,
    f.operating_income_million_yen,
    l.listed_month
from analytics.financial_results_monthly as f
left join analytics.listing_information_monthly as l
    on l.sc = f.sc
where f.sc = '1301'
order by f.fiscal_period_month desc
limit 10;
