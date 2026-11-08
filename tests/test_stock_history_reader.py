import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from generate_stock_history_reader import render_reader


class StockHistoryReaderTest(unittest.TestCase):
    def setUp(self):
        self.config = {'sc': '285A', 'title': '相場史', 'subtitle': '確認',
                       'price_basis_date': '2026-09-29', 'information_date': '2026-11-08',
                       'public_path': 'market-history/kioxia',
                       'sections': [{'heading': '分割の日', 'start': '2026-09-29', 'end': '2026-09-29', 'kind': 'timeline'}]}
        self.prices = {'sc': '285A', 'private_posts': 'DO_NOT_PUBLISH', 'candles': [
            {'date': '2026-09-28', 'raw': {'open': 54000, 'high': 55000, 'low': 52000, 'close': 53340, 'volume': 10},
             'adjusted': {'open': 18000, 'high': 55000 / 3, 'low': 52000 / 3, 'close': 17780, 'volume': 30}},
            {'date': '2026-09-29', 'raw': {'open': 18000, 'high': 18100, 'low': 17600, 'close': 17880, 'volume': 40},
             'adjusted': {'open': 18000, 'high': 18100, 'low': 17600, 'close': 17880, 'volume': 40}}]}

    def payload(self, html):
        return json.loads(html.split('<script id="reader-data" type="application/json">')[1].split('</script>')[0])

    def test_chart_dates_are_editorial_not_future_forecast_dates(self):
        html, manifest = render_reader('導入\n\n# 分割の日\n2031年3月期予想。', self.config, self.prices)
        self.assertEqual(self.payload(html)['segments'][1]['end'], '2026-09-29')
        self.assertEqual(manifest['files'].keys(), {'index.html'})
        self.assertNotIn('DO_NOT_PUBLISH', html)

    def test_split_keeps_adjusted_and_contemporaneous_prices_separate(self):
        html, _ = render_reader('# 分割の日\n本文。', self.config, self.prices)
        c = self.payload(html)['candles']
        self.assertLess(abs(c[1]['adjusted']['close'] / c[0]['adjusted']['close'] - 1), .01)
        self.assertEqual(c[0]['raw']['close'], 53340)

    def test_unsafe_markdown_and_json_cannot_inject_script(self):
        self.config['title'] = '</script><script>BAD</script>'
        self.config['sections'][0]['heading'] = '</script><img src=x onerror=BAD>'
        html, _ = render_reader('# </script><img src=x onerror=BAD>\n<script>BAD</script> [link](javascript:alert(1))', self.config, self.prices)
        self.assertNotIn('<img src=x', html)
        self.assertNotIn('<script>BAD', html)
        self.assertNotIn('href="javascript:', html)
        self.assertEqual(self.payload(html)['segments'][1]['heading'], self.config['sections'][0]['heading'])

    def test_missing_or_changed_heading_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'heading mismatch'):
            render_reader('# 違う見出し\n本文', self.config, self.prices)
        with self.assertRaisesRegex(ValueError, 'explicit timeline'):
            render_reader('見出しがない', self.config, self.prices)

    def test_future_or_stale_ranges_and_invalid_prices_are_rejected(self):
        cfg = copy.deepcopy(self.config)
        cfg['sections'][0]['end'] = '2031-03-31'
        with self.assertRaisesRegex(ValueError, 'outside available'):
            render_reader('# 分割の日', cfg, self.prices)
        prices = copy.deepcopy(self.prices)
        prices['candles'][0]['adjusted']['high'] = 1
        with self.assertRaisesRegex(ValueError, 'Inconsistent OHLC'):
            render_reader('# 分割の日', self.config, prices)
        cfg = copy.deepcopy(self.config)
        cfg['price_basis_date'] = '2026-10-01'
        with self.assertRaisesRegex(ValueError, 'basis date'):
            render_reader('# 分割の日', cfg, self.prices)


if __name__ == '__main__':
    unittest.main()
