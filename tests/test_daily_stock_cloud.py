from datetime import date
from decimal import Decimal
import gzip
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import hashlib
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import daily_stock_cloud as cloud
import stock_review_page as page

HEADER = "SC,名称,市場,業種,日付,株価,前日比,前日比（％）,前日終値,始値,高値,安値,出来高,売買代金（千円）\n"


def daily_csv(day, *, count=1000, close=100, change=0, previous=100):
    return (HEADER + "".join(
        f"{1000+i},銘柄{i},東証,業種,{day:%Y%m%d},{close},{change},0,{previous},{close},{close+1},{close-1},10000,4000000\n"
        for i in range(count)
    )).encode("cp932")


def listing(*days):
    return ("<table>" + "".join(f"<tr><td>japan-all-stock-prices_{d:%Y%m%d}.csv</td><td>CSV</td><td>today</td><td>500000</td></tr>" for d in days) + "</table>").encode()


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.metadata = {}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ApiError("NoSuchKey", 404)
        content = self.objects[Bucket, Key]
        return {"Body": io.BytesIO(content), "ETag": hashlib.md5(content).hexdigest()}

    def head_object(self, *, Bucket, Key):
        result = self.get_object(Bucket=Bucket, Key=Key)
        return {"ETag": result["ETag"], "Metadata": self.metadata.get((Bucket, Key), {})}

    def put_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise ApiError("PreconditionFailed", 412)
        if "IfMatch" in kwargs and (key not in self.objects or kwargs["IfMatch"] != hashlib.md5(self.objects[key]).hexdigest()):
            raise ApiError("PreconditionFailed", 412)
        self.objects[kwargs["Bucket"], kwargs["Key"]] = kwargs["Body"]
        self.metadata[key] = kwargs.get("Metadata", {})
        self.writes.append(kwargs)
        return {"ETag": hashlib.md5(kwargs["Body"]).hexdigest()}


class ApiError(Exception):
    def __init__(self, code, status):
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {
            "DATA_BUCKET": "private", "SITE_BUCKET": "public-origin", "SITE_URL": "https://example.com",
            "DISTRIBUTION_ID": "DIST", "CSVEX_SECRET_ARN": "secret-arn",
        }).start()
        self.s3 = MemoryS3()
        path = self.root / "seed.sqlite"
        self.conn = cloud.open_state(path)
        cloud.import_csv(self.conn, daily_csv(date(2026, 11, 5)), date(2026, 11, 5))
        self.conn.close()
        self.s3.objects["private", cloud.STATE_KEY] = gzip.compress(path.read_bytes())
        self.secret = SimpleNamespace(get_secret_value=lambda **kwargs: {"SecretString": '{"username":"test","password":"never-log-this"}'})
        self.cf = SimpleNamespace(create_invalidation=lambda **kwargs: {"Invalidation": {"Id": "INV"}}, get_waiter=lambda *args: SimpleNamespace(wait=lambda **kwargs: None))
        template = Path(__file__).resolve().parents[1] / "site/turnover-chart-viewer/template.html"
        patch.object(cloud, "write_html", lambda unused, output, payload: page.write_html(template, output, payload)).start()
        patch("builtins.print").start()
        self.today = date(2026, 11, 7)

    def run_cloud(self, target="2026-11-06", **kwargs):
        return cloud.process({"target_date": target}, SimpleNamespace(aws_request_id="run-1"), s3=self.s3, secrets=self.secret, cloudfront=self.cf, today=self.today, **kwargs)

    def test_not_listed_does_not_fall_back_or_publish(self):
        original = self.s3.objects["private", cloud.STATE_KEY]
        with patch.object(cloud, "http_get", return_value=listing(date(2026, 11, 5))) as get:
            result = self.run_cloud()
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(get.call_count, 1)
        self.assertNotIn(("public-origin", "daily/index.html"), self.s3.objects)
        self.assertEqual(original, self.s3.objects["private", cloud.STATE_KEY])

    def test_real_csv_updates_private_history_and_only_one_public_html(self):
        with patch.object(cloud, "http_get", side_effect=[listing(date(2026, 11, 6)), daily_csv(date(2026, 11, 6))]), \
             patch.object(cloud.urllib.request, "urlopen", side_effect=lambda *args, **kwargs: io.BytesIO(self.s3.objects["public-origin", "daily/index.html"])):
            result = self.run_cloud()
        self.assertEqual(result["status"], "published")
        self.assertEqual(result["displayed"], 300)
        self.assertEqual([(b, k) for b, k in self.s3.objects if b == "public-origin"], [("public-origin", "daily/index.html")])
        self.assertIn(("private", "raw/daily/japan-all-stock-prices_20261106.csv"), self.s3.objects)
        self.assertIn(("private", "status/last-published.json"), self.s3.objects)
        self.assertNotIn("never-log-this", json.dumps(result))

    def test_successful_day_is_idempotent_without_new_provider_requests(self):
        with patch.object(cloud, "http_get", side_effect=[listing(date(2026, 11, 6)), daily_csv(date(2026, 11, 6))]), \
             patch.object(cloud.urllib.request, "urlopen", side_effect=lambda *args, **kwargs: io.BytesIO(self.s3.objects["public-origin", "daily/index.html"])):
            self.run_cloud()
        writes = len(self.s3.writes)
        with patch.object(cloud, "http_get") as get:
            result = self.run_cloud()
        get.assert_not_called()
        self.assertEqual(result["status"], "already_current")
        self.assertEqual(len(self.s3.writes), writes + 2)  # Only private run receipts.

    def test_chart_history_is_kept_before_visible_window_and_adjusted_once(self):
        conn = cloud.open_state(self.root / "warmup.sqlite")
        self.addCleanup(conn.close)
        cloud.import_csv(conn, daily_csv(date(2026, 11, 5), count=1), date(2026, 11, 5), min_rows=1)
        cloud.import_csv(conn, daily_csv(date(2026, 11, 6), count=1, close=48, change=-2), date(2026, 11, 6), min_rows=1)
        stock = cloud.make_payload(conn, date(2026, 11, 6), lookback=1)["stocks"][0]
        self.assertEqual(len(stock["prices"]), 2)
        week = stock["weeklyPrices"][0]
        self.assertEqual(week["open"], 50)
        self.assertEqual(week["high"], 50.5)
        self.assertEqual(week["low"], 47)
        self.assertEqual(week["close"], 48)
        self.assertEqual(week["volume"], 30000)

    def test_only_one_lease_holder_and_release_allows_next(self):
        first = cloud.acquire_lock(self.s3, "private", "owner-1", now=100)
        self.assertIsNotNone(first)
        self.assertIsNone(cloud.acquire_lock(self.s3, "private", "owner-2", now=101))
        cloud.release_lock(self.s3, "private", "owner-1", first)
        self.assertIsNotNone(cloud.acquire_lock(self.s3, "private", "owner-2", now=102))

    def test_expired_lease_recovers_and_old_owner_cannot_release_new_owner(self):
        old = cloud.acquire_lock(self.s3, "private", "old", now=100)
        new = cloud.acquire_lock(self.s3, "private", "new", now=1061)
        self.assertIsNotNone(new)
        with self.assertRaises(ApiError):
            cloud.release_lock(self.s3, "private", "old", old)
        self.assertIsNone(cloud.acquire_lock(self.s3, "private", "third", now=1062))

    def test_bad_csv_preserves_both_state_and_live_page(self):
        original = self.s3.objects["private", cloud.STATE_KEY]
        self.s3.objects["public-origin", "daily/index.html"] = b"previous-page"
        with patch.object(cloud, "http_get", side_effect=[listing(date(2026, 11, 6)), b"<html>not a csv</html>"]):
            with self.assertRaisesRegex(RuntimeError, "import: ValueError"):
                self.run_cloud()
        self.assertEqual(original, self.s3.objects["private", cloud.STATE_KEY])
        self.assertEqual(self.s3.objects["public-origin", "daily/index.html"], b"previous-page")

    def test_empty_or_changed_listing_is_a_failure(self):
        with patch.object(cloud, "http_get", return_value=b"<html>sign in again</html>"):
            with self.assertRaisesRegex(RuntimeError, "listing: ValueError"):
                self.run_cloud()

    def test_https_mismatch_is_not_recorded_as_publication_success(self):
        with patch.object(cloud, "http_get", side_effect=[listing(date(2026, 11, 6)), daily_csv(date(2026, 11, 6))]), \
             patch.object(cloud.urllib.request, "urlopen", return_value=io.BytesIO(b"old-page")):
            with self.assertRaisesRegex(RuntimeError, "verify_https: ValueError"):
                self.run_cloud()
        self.assertNotIn(("private", "status/last-published.json"), self.s3.objects)
        status = json.loads(self.s3.objects["private", "status/latest-run.json"])
        self.assertTrue(status["uploaded"])
        self.assertEqual(status["status"], "failed")

    def test_credentials_never_escape_third_party_exception(self):
        with patch.object(cloud, "http_get", side_effect=ValueError("secret never-log-this")):
            with self.assertRaises(RuntimeError) as caught:
                self.run_cloud()
        self.assertNotIn("never-log-this", str(caught.exception))
        self.assertNotIn("never-log-this", self.s3.objects["private", "status/latest-run.json"].decode())

    def test_older_request_does_not_overwrite_newer_state(self):
        with self.assertRaisesRegex(RuntimeError, "load_state: ValueError"):
            self.run_cloud("2026-11-04")
        self.assertNotIn(("public-origin", "daily/index.html"), self.s3.objects)

    def test_vendor_reference_split_preserves_market_move_and_adjusts_history(self):
        conn = cloud.open_state(self.root / "split.sqlite")
        self.addCleanup(conn.close)
        cloud.import_csv(conn, daily_csv(date(2026, 11, 5), count=1), date(2026, 11, 5), min_rows=1)
        cloud.import_csv(conn, daily_csv(date(2026, 11, 6), count=1, close=48, change=-2), date(2026, 11, 6), min_rows=1)
        payload = cloud.make_payload(conn, date(2026, 11, 6))
        self.assertEqual(payload["stocks"][0]["prices"][0]["close"], 50)
        self.assertEqual(payload["stocks"][0]["prices"][0]["volume"], 20000)
        self.assertEqual(payload["stocks"][0]["prices"][1]["close"], 48)
        self.assertEqual(payload["meta"]["provisionalAdjustments"], 1)

    def test_seeded_official_action_has_precedence_over_vendor_inference(self):
        conn = cloud.open_state(self.root / "official.sqlite")
        self.addCleanup(conn.close)
        conn.execute("insert into actions values ('1000','2026-11-06',0.25,'verified')")
        conn.commit()
        cloud.import_csv(conn, daily_csv(date(2026, 11, 6), count=1, close=48, change=-2), date(2026, 11, 6), min_rows=1)
        action = conn.execute("select * from actions").fetchone()
        self.assertEqual(action["source"], "verified")
        self.assertEqual(action["multiplier"], 0.25)

    def test_ordinary_large_price_move_does_not_create_split(self):
        conn = cloud.open_state(self.root / "jump.sqlite")
        self.addCleanup(conn.close)
        cloud.import_csv(conn, daily_csv(date(2026, 11, 6), count=1, close=50, change=-50), date(2026, 11, 6), min_rows=1)
        self.assertEqual(conn.execute("select count(*) from actions").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
