from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from stock_review_page import add_moving_averages, build_payload, weekly_candles


class ChartSeriesTests(unittest.TestCase):
    def test_weekly_ohlcv_across_year_and_holiday_shortened_week(self):
        daily = [
            dict(date="2025-12-29", open=10, high=15, low=9, close=14, volume=100),
            dict(date="2025-12-30", open=14, high=16, low=12, close=13, volume=200),
            dict(date="2026-01-05", open=20, high=22, low=18, close=21, volume=300),
            dict(date="2026-01-07", open=21, high=24, low=19, close=23, volume=400),
        ]
        weeks = weekly_candles(daily)
        self.assertEqual(weeks, [
            dict(date="2025-12-29", through="2025-12-30", open=10, high=16, low=9, close=13, volume=300),
            dict(date="2026-01-05", through="2026-01-07", open=20, high=24, low=18, close=23, volume=700),
        ])
        self.assertNotIn("through", daily[0])

    def test_unknown_volume_is_preserved(self):
        rows = [dict(date=f"2026-01-0{day}", open=1, high=2, low=1, close=2, volume=volume)
                for day, volume in [(5, 10), (6, None), (7, 20)]]
        self.assertIsNone(weekly_candles(rows)[0]["volume"])

    def test_sma_requires_full_period_and_carries_warmup_into_view(self):
        rows = [dict(close=n) for n in range(1, 81)]
        result = add_moving_averages(rows, (5, 25, 75))
        self.assertEqual(result[3]["ma"], [None, None, None])
        self.assertEqual(result[4]["ma"], [3, None, None])
        self.assertEqual(result[74]["ma"], [73, 63, 38])
        self.assertEqual(result[-1]["ma"], [78, 68, 43])
        self.assertEqual(result[75:][0]["ma"], [74, 64, 39])
        self.assertNotIn("ma", rows[0])

    def test_payload_weekly_sma_uses_weekly_closes_and_keeps_history(self):
        start = date(2025, 1, 6)
        rows = []
        for week in range(60):
            for offset in (0, 4):
                close = week + 1 if offset == 4 else 999
                rows.append(dict(sc="1234", trade_date=start + timedelta(weeks=week, days=offset),
                                 open_price=Decimal(close), high_price=Decimal(close + 1),
                                 low_price=Decimal(close - 1), close_price=Decimal(close), volume=100))
        candidate = dict(sc="1234", name="銘柄", market="東証", industry="業種",
                         close_price=60, day_change_pct=0, turnover_thousand_yen=4000000)
        payload = build_payload(candidates=[candidate], price_rows=list(reversed(rows)),
                                target_date=rows[-1]["trade_date"], min_turnover_million_yen=Decimal(3000),
                                lookback_bars=20, matched_count=1)
        stock = payload["stocks"][0]
        self.assertEqual(len(stock["prices"]), 120)
        self.assertEqual(len(stock["weeklyPrices"]), 60)
        self.assertEqual(stock["weeklyPrices"][51]["ma"], [46, 39.5, 26.5])
        self.assertEqual(stock["weeklyPrices"][-1]["ma"], [54, 47.5, 34.5])
        self.assertEqual(stock["weeklyPrices"][-1]["through"], rows[-1]["trade_date"].isoformat())
        self.assertEqual(stock["prices"][-1]["close"], 60)


if __name__ == "__main__":
    unittest.main()
