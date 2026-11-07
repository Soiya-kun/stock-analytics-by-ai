insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (
    '8136',
    date '2026-03-30',
    'split',
    5,
    0.2,
    'ohlc_integer_jump',
    '{
      "name": "サンリオ",
      "manual_override": true,
      "official_effective_date": "2026-04-01",
      "official_split_ratio": "1:5",
      "prev_trade_date": "2026-03-27",
      "prev_close_price": 5303.0,
      "ohlc": {
        "open": 1030.5,
        "high": 1040.0,
        "low": 970.0,
        "close": 980.4
      },
      "source": "Sanrio IR and JPX corporate-action notice"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (
    '8053',
    date '2026-06-29',
    'split',
    4,
    0.25,
    'ohlc_integer_jump',
    '{
      "name": "住友商事",
      "manual_override": true,
      "official_record_date": "2026-06-30",
      "official_effective_date": "2026-07-01",
      "official_split_ratio": "1:4",
      "prev_trade_date": "2026-06-26",
      "prev_close_price": 6201.0,
      "first_adjusted_trade_date": "2026-06-29",
      "first_adjusted_close_price": 1532.5,
      "source": "https://www.sumitomocorp.com/-/media/Files/hq/news/release/2026/21060/21060.pdf?sc_lang=ja"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (
    '3110',
    date '2026-06-29',
    'split',
    5,
    0.2,
    'ohlc_integer_jump',
    '{
      "name": "日東紡績",
      "manual_override": true,
      "official_record_date": "2026-06-30",
      "official_effective_date": "2026-07-01",
      "official_split_ratio": "1:5",
      "prev_trade_date": "2026-06-26",
      "prev_close_price": 19630.0,
      "first_adjusted_trade_date": "2026-06-29",
      "first_adjusted_close_price": 4580.0,
      "source": "https://www2.jpx.co.jp/disc/31100/140120260518540008.pdf"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (
    '4452',
    date '2026-06-29',
    'split',
    2,
    0.5,
    'ohlc_integer_jump',
    '{
      "name": "花王",
      "manual_override": true,
      "official_effective_date": "2026-07-01",
      "official_split_ratio": "1:2",
      "prev_trade_date": "2026-06-26",
      "prev_close_price": 6544.0,
      "first_adjusted_trade_date": "2026-06-29",
      "first_adjusted_close_price": 3250.0,
      "source": "https://www.kao.com/jp/newsroom/news/release/2026/20260205-002/"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values
(
    '6590',
    date '2026-02-26',
    'split',
    5,
    0.2,
    'ohlc_integer_jump',
    '{
      "name": "芝浦メカトロニクス",
      "manual_override": true,
      "official_effective_date": "2026-03-01",
      "official_split_ratio": "1:5",
      "prev_trade_date": "2026-02-25",
      "prev_close_price": 28400.0,
      "first_adjusted_trade_date": "2026-02-26",
      "first_adjusted_close_price": 6000.0,
      "source": "https://www.shibaura.co.jp/ir/report/pdf/koukoku_260213_SM.pdf"
    }'::jsonb
),
(
    '485A',
    date '2026-05-28',
    'split',
    3,
    0.3333333333,
    'ohlc_integer_jump',
    '{
      "name": "パワーエックス",
      "manual_override": true,
      "official_effective_date": "2026-06-01",
      "official_split_ratio": "1:3",
      "prev_trade_date": "2026-05-27",
      "prev_close_price": 13590.0,
      "first_adjusted_trade_date": "2026-05-28",
      "first_adjusted_close_price": 4340.0,
      "source": "https://assets.ctfassets.net/rvycng606pot/vEOM0ajHummhU48muGlM3/88c2a3a5ea4519bcab4accbd028c2b4f/Public_Notice_of_Record_Date.pdf"
    }'::jsonb
),
(
    '3563',
    date '2026-06-29',
    'split',
    2,
    0.5,
    'ohlc_integer_jump',
    '{
      "name": "FOOD & LIFE COMPANIES",
      "manual_override": true,
      "official_effective_date": "2026-07-01",
      "official_split_ratio": "1:2",
      "prev_trade_date": "2026-06-26",
      "prev_close_price": 9969.0,
      "first_adjusted_trade_date": "2026-06-29",
      "first_adjusted_close_price": 5055.0,
      "source": "https://food-and-life.co.jp/wp-content/uploads/2026/05/885cce3810768a286837726c456d4b13.pdf"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values
(
    '7181',
    date '2026-03-30',
    'split',
    3,
    0.3333333333,
    'ohlc_integer_jump',
    '{
      "name": "かんぽ生命保険",
      "manual_override": true,
      "official_effective_date": "2026-04-01",
      "official_split_ratio": "1:3",
      "prev_trade_date": "2026-03-27",
      "prev_close_price": 4916.0,
      "first_adjusted_trade_date": "2026-03-30",
      "first_adjusted_close_price": 1571.0,
      "source": "https://www.jp-life.japanpost.jp/IR/notice/assets/pdf/koukoku_20260311.pdf"
    }'::jsonb
),
(
    '9602',
    date '2026-02-26',
    'split',
    5,
    0.2,
    'ohlc_integer_jump',
    '{
      "name": "東宝",
      "manual_override": true,
      "official_effective_date": "2026-03-01",
      "official_split_ratio": "1:5",
      "prev_trade_date": "2026-02-25",
      "prev_close_price": 7763.0,
      "first_adjusted_trade_date": "2026-02-26",
      "first_adjusted_close_price": 1553.5,
      "source": "https://www.toho.co.jp/files/bc48574e4f53099b1e95473bc3997d2d907cf47d7439844b3e6db3e9ed5db40f"
    }'::jsonb
),
(
    '6227',
    date '2026-03-30',
    'split',
    3,
    0.3333333333,
    'ohlc_integer_jump',
    '{
      "name": "AIメカテック",
      "manual_override": true,
      "official_effective_date": "2026-04-01",
      "official_split_ratio": "1:3",
      "prev_trade_date": "2026-03-27",
      "prev_close_price": 17940.0,
      "first_adjusted_trade_date": "2026-03-30",
      "first_adjusted_close_price": 5560.0,
      "source": "https://www2.jpx.co.jp/disc/62270/140120260514532424.pdf"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (
    '8309',
    date '2026-07-30',
    'split',
    4,
    0.25,
    'ohlc_integer_jump',
    '{
      "name": "三井住友トラストグループ",
      "manual_override": true,
      "official_record_date": "2026-07-31",
      "official_effective_date": "2026-08-01",
      "official_split_ratio": "1:4",
      "prev_trade_date": "2026-07-29",
      "prev_close_price": 6842.0,
      "first_adjusted_trade_date": "2026-07-30",
      "first_adjusted_close_price": 1672.0,
      "source": "https://www.smtg.jp/-/media/tg/about_us/koukoku/pdf/260715.pdf"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (
    '7012',
    date '2026-03-30',
    'split',
    5,
    0.2,
    'ohlc_integer_jump',
    '{
      "name": "川崎重工業",
      "manual_override": true,
      "official_record_date": "2026-03-31",
      "official_effective_date": "2026-04-01",
      "official_split_ratio": "1:5",
      "prev_trade_date": "2026-03-27",
      "prev_close_price": 15195.0,
      "first_adjusted_trade_date": "2026-03-30",
      "first_adjusted_close_price": 2917.0,
      "source": "https://www.khi.co.jp/pressrelease/news_260209-5.pdf"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (
    '7735',
    date '2026-03-30',
    'split',
    2,
    0.5,
    'ohlc_integer_jump',
    '{
      "name": "SCREENホールディングス",
      "manual_override": true,
      "official_record_date": "2026-03-31",
      "official_effective_date": "2026-04-01",
      "official_split_ratio": "1:2",
      "prev_trade_date": "2026-03-27",
      "prev_close_price": 19080.0,
      "first_adjusted_trade_date": "2026-03-30",
      "first_adjusted_close_price": 9119.0,
      "source": "https://hdjp-corporateweb-files.screen.co.jp/8717/6975/4929/20260130_Notice1_J.pdf"
    }'::jsonb
)
on conflict (sc, action_date, detection_method) do update set
    action_type = excluded.action_type,
    integer_factor = excluded.integer_factor,
    price_multiplier = excluded.price_multiplier,
    evidence_json = excluded.evidence_json;

-- 20260926_metaplanet_split
-- Source: https://www2.jpx.co.jp/disc/33500/140120250324598909.pdf p129
-- Record date 2025-03-31 / legal effective date 2025-04-01.
-- Apply to market prices at ex-date 2025-03-28, observed in raw daily prices.
INSERT INTO analytics.inferred_price_actions
(sc,action_date,action_type,integer_factor,price_multiplier,detection_method,evidence_json)
VALUES ('3350','2025-03-28','split',10,0.1,'ohlc_integer_jump',
'{"manual_override":true,"official_effective_date":"2025-04-01","record_date":"2025-03-31","market_ex_date":"2025-03-28","ratio":"1:10","source":"https://www2.jpx.co.jp/disc/33500/140120250324598909.pdf","raw_prev_close":4795,"raw_ex_close":436}'::jsonb)
ON CONFLICT (sc,action_date,detection_method) DO UPDATE SET
price_multiplier=excluded.price_multiplier,integer_factor=excluded.integer_factor,
action_type=excluded.action_type,evidence_json=excluded.evidence_json;
CREATE TABLE IF NOT EXISTS analytics.verified_price_actions (
 sc text NOT NULL, action_date date NOT NULL,
 price_multiplier numeric(24,12) NOT NULL CHECK(price_multiplier>0),
 action_type text NOT NULL CHECK(action_type IN ('split','reverse_split')),
 evidence_json jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(sc,action_date)
);
CREATE OR REPLACE VIEW analytics.effective_price_actions AS
 SELECT sc,action_date,price_multiplier,action_type,evidence_json FROM analytics.verified_price_actions
 UNION ALL
 SELECT i.sc,i.action_date,i.price_multiplier,i.action_type,i.evidence_json
 FROM analytics.inferred_price_actions i
 WHERE NOT EXISTS (SELECT 1 FROM analytics.verified_price_actions v WHERE v.sc=i.sc AND v.action_date=i.action_date);

-- Official announcement: https://metaplanet.jp/wp-content/uploads/2024/06/EGM_202406.pdf
-- 10 old shares -> 1 new share, legal effective 2024-08-01, market ex-date 2024-07-30.
-- Raw KABU+ on ex-date: close1510, change-500, previous_close201; reference=2010=201*10.
INSERT INTO analytics.inferred_price_actions
(sc,action_date,action_type,integer_factor,price_multiplier,detection_method,evidence_json)
VALUES ('3350','2024-07-30','reverse_split',10,10,'ohlc_integer_jump',
'{"manual_override":true,"official_effective_date":"2024-08-01","market_ex_date":"2024-07-30","ratio":"10:1","source":"https://metaplanet.jp/wp-content/uploads/2024/06/EGM_202406.pdf","previous_inferred_multiplier":8,"raw_previous_close":201,"raw_close":1510,"raw_day_change":-500}'::jsonb)
ON CONFLICT (sc,action_date,detection_method) DO UPDATE SET
price_multiplier=excluded.price_multiplier,integer_factor=excluded.integer_factor,
action_type=excluded.action_type,evidence_json=excluded.evidence_json;
