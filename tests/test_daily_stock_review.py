from datetime import date
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import daily_stock_review as review
import generate_turnover_chart_viewer as viewer
from decimal import Decimal


class DailyReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(patch.stopall)
        patch.object(review, "ROOT", self.root).start()
        patch.object(review, "OUTPUT", self.root / "outputs" / "daily-stock-review").start()
        self.args = review.argparse.Namespace(
            date="2026-11-05", skip_fetch=False, action="run",
            min_turnover_million_yen=3000, lookback_bars=250, limit=300,

        )

    def html(self, path, day="2026-11-05", close=100):
        payload = {"meta": {"targetDate": day}, "stocks": [
            {"sc": "1234", "close": 100, "prices": [{"date": day, "close": close}]},
        ]}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('<script id="viewer-data" type="application/json">' + json.dumps(payload) + '</script>', encoding="utf-8")

    def latest(self):
        return json.loads((review.OUTPUT / "latest-run.json").read_text(encoding="utf-8"))

    def test_unpublished_day_does_not_import_or_publish(self):
        with patch.object(review, "docker_python", return_value='{"target_date":"2026-11-05","imported":false}'), \
             patch.object(review, "command") as command:
            self.assertFalse(review.ensure_imported(date(2026, 11, 5), False))
        # Only the listing-first downloader ran. There is no importer fallback.
        self.assertEqual(command.call_count, 1)
        self.assertIn("/workspace/scripts/fetch_kabuplus_daily_csv.py", command.call_args.args[0])

    def test_completed_import_avoids_credentials_and_download(self):
        with patch.object(review, "docker_python", return_value='{"target_date":"2026-11-05","imported":true}'), \
             patch.object(review, "command") as command:
            self.assertTrue(review.ensure_imported(date(2026, 11, 5), False))
        command.assert_not_called()

    def test_saved_csv_can_retry_failed_import_without_download(self):
        csv = self.root / "stock/kabuplus-2026/japan-all-stock-prices_20261105.csv"
        csv.parent.mkdir(parents=True)
        csv.write_text("saved")
        states = ['{"target_date":"2026-11-05","imported":false}', '{"target_date":"2026-11-05","imported":true}']
        with patch.object(review, "docker_python", side_effect=states), patch.object(review, "command") as command:
            self.assertTrue(review.ensure_imported(date(2026, 11, 5), False))
        self.assertEqual(command.call_count, 1)
        self.assertIn("--csv-file", command.call_args.args[0])

    def test_wrong_date_or_price_basis_blocks_page(self):
        path = self.root / "index.html"
        self.html(path)
        with self.assertRaisesRegex(ValueError, "wrong date"):
            review.validate_html(path, date(2026, 11, 4))
        self.html(path, close=50)
        with self.assertRaisesRegex(ValueError, "basis differ"):
            review.validate_html(path, date(2026, 11, 5))

    def test_generation_failure_preserves_last_publication(self):
        previous = {"target_date": "2026-11-04", "status": "published"}
        review.write_json(review.OUTPUT / "last-published.json", previous)
        with patch.object(review, "ensure_imported", return_value=True), \
             patch.object(review, "docker_python", side_effect=RuntimeError("generation failed")), \
             patch("builtins.print"):
            self.assertEqual(review.run(self.args), 1)
        self.assertEqual(self.latest()["phase"], "generate")
        self.assertEqual(json.loads((review.OUTPUT / "last-published.json").read_text()), previous)
        self.assertFalse((review.OUTPUT / "pipeline.lock").exists())

    def test_unpublished_run_records_skip_without_generation(self):
        with patch.object(review, "ensure_imported", return_value=False), \
             patch.object(review, "docker_python") as generate, patch("builtins.print"):
            self.assertEqual(review.run(self.args), 0)
        generate.assert_not_called()
        self.assertEqual(self.latest()["reason"], "csv_not_published")

    def test_local_run_never_uses_aws_and_validates_artifact(self):
        def generate(*parts):
            container_path = parts[parts.index("--output") + 1]
            self.html(self.root / container_path.removeprefix("/workspace/"))
        with patch.object(review, "ensure_imported", return_value=True), \
             patch.object(review, "docker_python", side_effect=generate), \
             patch("builtins.print"):
            self.assertEqual(review.run(self.args), 0)
        self.assertEqual(self.latest()["status"], "generated")
        self.assertEqual(len(self.latest()["sha256"]), 64)

    def test_local_import_only_stops_after_verifying_database(self):
        self.args.action = "import"
        with patch.object(review, "ensure_imported", return_value=True), \
             patch.object(review, "docker_python") as generate, patch("builtins.print"):
            self.assertEqual(review.run(self.args), 0)
        generate.assert_not_called()
        self.assertEqual(self.latest()["status"], "imported")



class PriceAdjustmentTests(unittest.TestCase):
    def test_chart_uses_official_actions_only_through_display_date(self):
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, params):
                self.sql = sql
                if "effective_price_actions" in sql:
                    self_outer.assertIn("action_date <= %s", sql)
                    self_outer.assertEqual(params[-1], date(2026, 11, 5))
            def fetchall(self):
                if "effective_price_actions" in self.sql:
                    return [{"sc": "1234", "action_date": date(2026, 11, 5), "price_multiplier": Decimal("0.5")}]
                return [{"sc": "1234", "trade_date": date(2026, 11, 4), "raw_open_price": Decimal(200), "raw_high_price": Decimal(210), "raw_low_price": Decimal(190), "raw_close_price": Decimal(200), "raw_volume": Decimal(1000)}]
        class Connection:
            def cursor(self): return Cursor()
        self_outer = self
        result = viewer.fetch_price_history(Connection(), stock_codes=["1234"], target_date=date(2026, 11, 5), lookback_bars=250)
        self.assertEqual(result[0]["close_price"], Decimal(100))
        self.assertEqual(result[0]["volume"], Decimal(2000))


if __name__ == "__main__":
    unittest.main()
