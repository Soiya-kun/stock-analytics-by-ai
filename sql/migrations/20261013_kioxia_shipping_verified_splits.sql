-- Official splits missing from effective_price_actions during the 2026-10-13
-- Kioxia / COVID shipping comparison. Dates below are market ex-dates, not
-- legal effective dates. Original raw KABU+ prices remain unchanged.
BEGIN;
INSERT INTO analytics.verified_price_actions
 (sc,action_date,price_multiplier,action_type,evidence_json)
VALUES
 ('285A','2026-09-29',0.333333333333,'split',
  '{"official":true,"ratio":"1:3","record_date":"2026-09-30","official_effective_date":"2026-10-01","market_ex_date":"2026-09-29","source":"https://ssl4.eir-parts.net/doc/285A/tdnet/2859912/00.pdf","verification":"DB raw close 53340 on 2026-09-28 and 17880 on 2026-09-29; reported change +0.56 percent matches one-third adjustment","analysis":"kioxia-shipping-20261013"}'::jsonb),
 ('9101','2022-09-29',0.333333333333,'split',
  '{"official":true,"ratio":"1:3","record_date":"2022-09-30","official_effective_date":"2022-10-01","market_ex_date":"2022-09-29","source":"https://www.nyk.com/ir/news/2022/__icsFiles/afieldfile/2022/05/26/20220526h_1.pdf","verification":"Official FAQ Q7 explicitly identifies market ex-date; DB raw close 8910 to 2500, reported change -15.82 percent matches one-third adjustment","analysis":"kioxia-shipping-20261013"}'::jsonb),
 ('9104','2022-03-30',0.333333333333,'split',
  '{"official":true,"ratio":"1:3","record_date":"2022-03-31","official_effective_date":"2022-04-01","market_ex_date":"2022-03-30","source":"https://ir.mol.co.jp/ja/ir/ir_news/news-961974605881823244/main/0/link/news_220228_2.pdf","verification":"Official FAQ specifies market ex-date; DB raw close 10340 to 3275, reported change -4.98 percent matches one-third adjustment","analysis":"kioxia-shipping-20261013"}'::jsonb)
ON CONFLICT(sc,action_date) DO NOTHING;
COMMIT;
