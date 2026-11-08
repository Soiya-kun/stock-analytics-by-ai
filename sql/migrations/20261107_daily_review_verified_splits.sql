-- Official September splits missing from the daily chart history.
-- JPX monthly table lists the market ex-date, not the legal effective date.
-- Raw KABU+ prices are preserved. Verified actions override inference.
BEGIN;

CREATE TEMP TABLE daily_review_official_splits (
 sc text PRIMARY KEY, name text NOT NULL, split_factor integer NOT NULL
) ON COMMIT DROP;
INSERT INTO daily_review_official_splits VALUES
 ('8035','東京エレクトロン',5),
 ('4062','イビデン',2),
 ('8316','三井住友フィナンシャルグループ',2),
 ('8766','東京海上ホールディングス',15),
 ('5706','三井金属',10),
 ('1925','大和ハウス工業',2),
 ('5334','日本特殊陶業',2),
 ('278A','Terra Drone',2),
 ('5805','SWCC',5),
 ('2282','日本ハム',3),
 ('6368','オルガノ',5);

-- Check the vendor's ex-rights comparison base independently of the price jump.
-- Small rounding of reported daily changes (notably 8766) is permitted.
DO $$
BEGIN
 IF EXISTS (
  SELECT 1 FROM daily_review_official_splits s
  LEFT JOIN raw.kabuplus_records r
    ON r.dataset_key='japan-all-stock-prices/daily'
   AND r.security_code=s.sc AND r.record_date=date '2026-09-29'
  WHERE r.security_code IS NULL
     OR util.to_numeric_or_null(r.payload->>'株価') IS NULL
     OR util.to_numeric_or_null(r.payload->>'前日比') IS NULL
     OR coalesce(util.to_numeric_or_null(r.payload->>'前日終値'),0)<=0
     OR abs((util.to_numeric_or_null(r.payload->>'株価')
            -util.to_numeric_or_null(r.payload->>'前日比'))
            / nullif(util.to_numeric_or_null(r.payload->>'前日終値'),0)
            *s.split_factor-1)>0.0001
 ) THEN
  RAISE EXCEPTION 'Official split conflicts with or lacks KABU+ comparison data';
 END IF;
END $$;

INSERT INTO analytics.verified_price_actions
 (sc,action_date,price_multiplier,action_type,evidence_json)
SELECT sc,date '2026-09-29',1::numeric/split_factor,'split',
 jsonb_build_object(
  'official',true,'name',name,'ratio','1:'||split_factor,
  'market_ex_date','2026-09-29','record_date','2026-09-30',
  'source','https://www.jpx.co.jp/markets/statistics-equities/monthly/t13vrt000002h6ia-att/17_kenri2609.pdf',
  'verification','Official JPX ex-rights table; checked against KABU+ (close minus daily change) / previous close',
  'maintenance','daily-review-20261107'
 )
FROM daily_review_official_splits
ON CONFLICT(sc,action_date) DO UPDATE SET
 price_multiplier=excluded.price_multiplier,
 action_type=excluded.action_type,
 evidence_json=excluded.evidence_json,
 updated_at=now();
COMMIT;
