(() => {
  'use strict';
  const data = JSON.parse(document.getElementById('reader-data').textContent);
  const candles = data.candles, segments = data.segments;
  const byId = id => document.getElementById(id);
  const panel = byId('chart-panel'), plot = byId('plot'), chart = byId('chart');
  const elements = segments.map(s => byId(s.id));
  const fmt = n => new Intl.NumberFormat('ja-JP', {maximumFractionDigits: 1}).format(n);
  const day = s => s.replaceAll('-', '/');
  let active = -1, follow = true, frozen = 0, selected = -1, geometry, pending = false;

  // Ranges are keyed to editorial sections, not dates mentioned in earnings forecasts.
  function rangeFor(segment, setting) {
    const first = Math.max(0, candles.findIndex(c => c.date >= segment.start));
    let end = candles.findLastIndex(c => c.date <= segment.end);
    if (end < first) end = first;
    if (setting === 'all') return {first, start: 0, end: candles.length - 1, focusEnd: end};
    const span = Math.max(Number(setting), end - first + 13);
    return {first, start: Math.max(0, Math.min(first - 12, end - span + 1)), end, focusEnd: end};
  }
  function draw() {
    const segment = segments[follow ? Math.max(0, active) : frozen];
    const r = rangeFor(segment, byId('window').value);
    const w = chart.clientWidth, h = chart.clientHeight;
    if (!w || !h) return;
    const left = 1, right = w - 53, top = 9, priceBottom = h * .69, volumeTop = h * .77, bottom = h - 21;
    const rows = candles.slice(r.start, r.end + 1), count = rows.length, step = (right - left) / (count + 1);
    const log = byId('log-scale').checked;
    const transform = log ? Math.log : n => n;
    const lo = Math.min(...rows.map(c => transform(c.adjusted.low)));
    const hi = Math.max(...rows.map(c => transform(c.adjusted.high)));
    const spread = Math.max(hi - lo, log ? .01 : 1);
    const low = log ? lo - spread * .06 : Math.max(0, lo - spread * .06), high = hi + spread * .08;
    const y = p => top + (high - transform(p)) / (high - low) * (priceBottom - top);
    const x = i => left + (i - r.start + 1) * step;
    const volumeMax = Math.max(1, ...rows.map(c => c.adjusted.volume));
    const pieces = ['<title id="plot-title">日足チャート</title>',
      `<desc id="plot-desc">${day(candles[r.start].date)}から${day(candles[r.end].date)}。緑の帯は本文の対象期間。分割調整後の株価と出来高。</desc>`];
    const focusLeft = Math.max(left, x(r.first) - step * .6);
    pieces.push(`<rect x="${focusLeft}" y="${top}" width="${Math.max(2, x(r.focusEnd) + step * .6 - focusLeft)}" height="${bottom - top}" fill="#dcece0"/>`);
    for (let i = 0; i < 4; i++) {
      const level = low + (high - low) * i / 3;
      const price = log ? Math.exp(level) : level, gy = y(price);
      pieces.push(`<path d="M${left},${gy}H${right}" stroke="#d4ded5" stroke-width=".7"/><text x="${right + 6}" y="${gy + 3}">${Math.round(price).toLocaleString('ja-JP')}</text>`);
    }
    pieces.push(`<text x="${right + 6}" y="${volumeTop + 7}" style="font-size:8px">出来高</text>`);
    rows.forEach((c, offset) => {
      const a = c.adjusted, cx = x(offset + r.start), color = a.close >= a.open ? '#17735c' : '#cb654f';
      const width = Math.max(.6, Math.min(7, step * .65));
      const bodyTop = y(Math.max(a.open, a.close)), bodyHeight = Math.max(1, Math.abs(y(a.open) - y(a.close)));
      const vh = a.volume / volumeMax * Math.max(4, bottom - volumeTop);
      pieces.push(`<path d="M${cx},${y(a.high)}V${y(a.low)}" stroke="${color}" stroke-width="${Math.min(1.2, width)}"/>`);
      pieces.push(`<rect x="${cx - width / 2}" y="${bodyTop}" width="${width}" height="${bodyHeight}" fill="${color}"/>`);
      pieces.push(`<rect x="${cx - width / 2}" y="${bottom - vh}" width="${width}" height="${vh}" fill="${color}" opacity=".42"/>`);
    });
    const labels = [...new Set([r.start, Math.round((r.start + r.end) / 2), r.end])];
    labels.forEach((index, i) => pieces.push(`<text x="${i === 0 ? left : i === labels.length - 1 ? right : x(index)}" y="${h - 4}" text-anchor="${i === 0 ? 'start' : i === labels.length - 1 ? 'end' : 'middle'}">${candles[index].date.slice(2).replaceAll('-', '/')}</text>`));
    if (selected >= r.start && selected <= r.end) {
      const cx = x(selected), cy = y(candles[selected].adjusted.close);
      pieces.push(`<path d="M${cx},${top}V${bottom}" stroke="#536b60" stroke-dasharray="3 3"/><circle cx="${cx}" cy="${cy}" r="3" fill="#202b2b" stroke="white"/>`);
    }
    plot.setAttribute('viewBox', `0 0 ${w} ${h}`);
    plot.innerHTML = pieces.join('');
    byId('period-label').textContent = segment.start === segment.end ? day(segment.end) : `${day(segment.start)} — ${day(segment.end)}`;
    chart.dataset.start = candles[r.start].date;
    chart.dataset.end = candles[r.end].date;
    chart.dataset.section = segment.id;
    geometry = {...r, left, right, step};
  }
  function clearQuote() {
    selected = -1;
    byId('quote').classList.remove('visible');
  }
  function setActive(index) {
    if (index === active) return;
    if (active >= 0) elements[active].classList.remove('is-active');
    active = index;
    elements[active].classList.add('is-active');
    document.querySelectorAll('#toc a').forEach(a => {
      if (a.hash === '#' + segments[active].id) a.setAttribute('aria-current', 'true');
      else a.removeAttribute('aria-current');
    });
    if (follow) {clearQuote(); draw();}
  }
  function track() {
    pending = false;
    const line = panel.getBoundingClientRect().bottom + 72;
    let index = 0;
    for (let i = 0; i < elements.length; i++) {
      if (elements[i].getBoundingClientRect().top <= line) index = i;
      else break;
    }
    setActive(index);
    const max = document.documentElement.scrollHeight - innerHeight;
    byId('progress').style.width = `${max > 0 ? Math.min(100, Math.max(0, scrollY / max * 100)) : 0}%`;
  }
  function schedule() {if (!pending) {pending = true; requestAnimationFrame(track);}}
  function showQuote(index) {
    if (!geometry) return;
    selected = Math.max(geometry.start, Math.min(geometry.end, index));
    const c = candles[selected], a = c.adjusted, raw = c.raw;
    byId('quote').innerHTML = `<b>${day(c.date)}</b>　始 ${fmt(a.open)}　高 ${fmt(a.high)}　安 ${fmt(a.low)}　終 <b>${fmt(a.close)}円</b>　出来高 ${fmt(a.volume)}株<br><span class="quote-raw">当時価格：始 ${fmt(raw.open)}　高 ${fmt(raw.high)}　安 ${fmt(raw.low)}　終 ${fmt(raw.close)}円</span>`;
    byId('quote').classList.add('visible');
    draw();
  }
  let pointerStart;
  chart.addEventListener('pointerdown', e => {pointerStart = {x: e.clientX, y: e.clientY};});
  chart.addEventListener('pointercancel', () => {pointerStart = null;});
  chart.addEventListener('pointerup', e => {
    if (!pointerStart || Math.hypot(e.clientX - pointerStart.x, e.clientY - pointerStart.y) > 12 || !geometry) return;
    pointerStart = null;
    const px = e.clientX - chart.getBoundingClientRect().left;
    showQuote(geometry.start + Math.round((px - geometry.left) / geometry.step) - 1);
  });
  chart.addEventListener('keydown', e => {
    if (e.key === 'Escape') {clearQuote(); draw(); return;}
    if (!['ArrowLeft', 'ArrowRight'].includes(e.key) || !geometry) return;
    e.preventDefault();
    showQuote((selected < 0 ? geometry.focusEnd : selected) + (e.key === 'ArrowRight' ? 1 : -1));
  });
  byId('follow').addEventListener('click', () => {
    frozen = active; follow = !follow;
    byId('follow').setAttribute('aria-pressed', String(follow));
    byId('follow-label').textContent = follow ? '本文と連動' : '連動を再開';
    clearQuote(); draw();
  });
  byId('window').addEventListener('change', () => {clearQuote(); draw();});
  byId('log-scale').addEventListener('change', draw);
  const dialog = byId('toc');
  byId('contents').addEventListener('click', () => dialog.showModal());
  byId('close-toc').addEventListener('click', () => dialog.close());
  dialog.addEventListener('click', e => {if (e.target === dialog) dialog.close();});
  dialog.querySelectorAll('a').forEach(a => a.addEventListener('click', () => dialog.close()));
  document.querySelectorAll('.story-section table').forEach(table => {
    const wrap = document.createElement('div');
    wrap.className = 'table-wrap'; wrap.tabIndex = 0;
    wrap.setAttribute('role', 'region'); wrap.setAttribute('aria-label', '表。横にスクロールできます');
    table.before(wrap); wrap.append(table);
  });
  const observer = new ResizeObserver(() => {
    document.documentElement.style.setProperty('--panel-height', `${panel.getBoundingClientRect().height}px`);
    draw(); schedule();
  });
  observer.observe(panel); observer.observe(chart);
  addEventListener('scroll', schedule, {passive: true});
  addEventListener('resize', schedule);
  addEventListener('hashchange', schedule);
  track();
})();
