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
