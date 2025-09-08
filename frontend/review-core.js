// frontend/review-core.js
// Safe drop-in: preserves existing segment list + routes, adds smart axes & toggles.

(() => {
  const qs  = (s, r=document) => r.querySelector(s);
  const qsa = (s, r=document) => Array.from(r.querySelectorAll(s));

  const State = {
    chunk: 2000,
    noThinning: false,
    showTicks: true,
    data: { segm: null, layers: {} },
    echarts: null,
    xMin: null, xMax: null,
    segmDelegated: false
  };

  // ---------- Non-invasive UI mounts ----------
  function mountToggles() {
    // Prefer .layers-row if present; else put in a small footer slot.
    let host = qs('.layers-row') || qs('#layersRow') || qs('#review-footer');
    if (!host) {
      host = document.createElement('div');
      host.id = 'review-footer';
      (qs('#review') || document.body).appendChild(host);
    }

    if (!qs('#toggleNoThin')) {
      const el = document.createElement('label');
      el.style.marginLeft = '12px';
      el.innerHTML = `<input type="checkbox" id="toggleNoThin"> No thinning`;
      host.appendChild(el);
      el.firstElementChild.addEventListener('change', e => {
        State.noThinning = e.target.checked;
        render();
      });
    }

    if (!qs('#toggleShowTicks')) {
      const el = document.createElement('label');
      el.style.marginLeft = '12px';
      el.innerHTML = `<input type="checkbox" id="toggleShowTicks" checked> Show ticks`;
      host.appendChild(el);
      el.firstElementChild.addEventListener('change', e => {
        State.showTicks = e.target.checked;
        render();
      });
    }
  }

  function wireChunk() {
    const inp = qs('#chunkInput') || qs('input[name="chunk"]');
    if (!inp) return;
    const apply = () => {
      const v = +inp.value;
      if (v > 0 && Number.isFinite(v)) {
        State.chunk = v;
        if (!State.noThinning) render();
      }
    };
    inp.addEventListener('change', apply);
    apply();
  }

  // ---------- Helpers ----------
  function thin(arr, n) {
    if (!arr || arr.length <= n) return arr || [];
    const stride = Math.ceil(arr.length / n);
    const out = [];
    for (let i = 0; i < arr.length; i += stride) out.push(arr[i]);
    if (arr.length && out[out.length - 1] !== arr[arr.length - 1]) out.push(arr[arr.length - 1]);
    return out;
  }

  function ticksToSeries(segm) {
    return (segm?.ticks || []).map(t => [new Date(t.ts).getTime(), t.mid]);
  }
  function atrToSeries(rows) {
    return (rows || [])
      .map(r => [new Date(r.ts).getTime(), r.value ?? r.atr1 ?? r.atr])
      .filter(p => p[1] != null);
  }

  // ---------- Chart ----------
  function chart() {
    if (State.echarts) return State.echarts;
    const el = qs('#reviewChart') || qs('#chart');
    if (!el) return null;
    State.echarts = echarts.init(el);
    window.addEventListener('resize', () => State.echarts && State.echarts.resize());
    State.echarts.on('dataZoom', () => {
      const [mn, mx] = currentX();
      State.xMin = mn; State.xMax = mx;
      refitY();
    });
    return State.echarts;
  }

  function currentX() {
    const c = chart();
    const opt = c?.getOption?.();
    const xa  = opt?.xAxis?.[0];
    const mn  = (xa && xa.min != null) ? +xa.min : State.xMin;
    const mx  = (xa && xa.max != null) ? +xa.max : State.xMax;

    if (mn != null && mx != null) return [mn, mx];
    const pts = ticksToSeries(State.data.segm);
    if (!pts.length) return [null, null];
    return [pts[0][0], pts[pts.length - 1][0]];
  }

  function extent(series, xMin, xMax, padFrac=0.08) {
    let lo=Infinity, hi=-Infinity;
    for (const [x,y] of series) {
      if (xMin!=null && x<xMin) continue;
      if (xMax!=null && x>xMax) continue;
      if (y<lo) lo=y; if (y>hi) hi=y;
    }
    if (!isFinite(lo) || !isFinite(hi)) return null;
    if (lo===hi) { const p = Math.max(1e-6, Math.abs(hi)*0.001); lo-=p; hi+=p; }
    const pad = (hi-lo)*padFrac;
    return [lo-pad, hi+pad];
  }

  function atrWanted() {
    const cb = qs('input[type=checkbox][data-layer="atr1"]') || qs('input[type=checkbox][name="atr1"]');
    return cb ? cb.checked : true;
  }

  function refitY() {
    const c = chart(); if (!c) return;
    const [xmn, xmx] = currentX();

    let t = ticksToSeries(State.data.segm);
    let a = atrToSeries(State.data.layers['atr1']);
    if (!State.noThinning) { t = thin(t, State.chunk); a = thin(a, State.chunk); }

    const left  = State.showTicks ? extent(t, xmn, xmx) : null;
    const right = atrWanted() && a.length ? extent(a, xmn, xmx) : null;

    c.setOption({
      yAxis: [
        { type:'value', scale:true, name:'Price',
          axisLabel:{formatter:v=>(+v).toFixed(2)}, ...(left?{min:left[0],max:left[1]}:{})
        },
        { type:'value', scale:true, name:'ATR', position:'right',
          axisLabel:{formatter:v=>(+v).toFixed(4)}, ...(right?{min:right[0],max:right[1]}:{})
        }
      ]
    });
  }

  function isSegmRow(el) {
    // works with <tr data-start="..."> or any row carrying dataset.start/end
    if (!el) return false;
    const row = el.closest('tr,[data-start]');
    if (!row) return false;
    const s = row.getAttribute('data-start') || row.dataset.start;
    const e = row.getAttribute('data-end')   || row.dataset.end;
    return !!(s && e);
  }

  function bindSegmDelegation() {
    if (State.segmDelegated) return;
    const host = qs('#segmTable') || qs('.segm-table') || qs('#segments') || qs('.segments-panel');
    if (!host) return; // do nothing — we won’t break anything
    host.addEventListener('click', (evt) => {
      const row = evt.target.closest('tr,[data-start]');
      if (!row) return;
      const s = row.getAttribute('data-start') || row.dataset.start;
      const e = row.getAttribute('data-end')   || row.dataset.end;
      if (!s || !e) return;
      const sMs = isFinite(+s) ? +s : Date.parse(s);
      const eMs = isFinite(+e) ? +e : Date.parse(e);
      if (!isFinite(sMs) || !isFinite(eMs)) return;

      State.xMin = sMs; State.xMax = eMs;
      const c = chart();
      c && c.setOption({ xAxis: [{ min:sMs, max:eMs }] });
      refitY();
    });
    State.segmDelegated = true;
  }

  // ---------- Render ----------
  function render() {
    const c = chart(); if (!c) return;

    let t = ticksToSeries(State.data.segm);
    let a = atrToSeries(State.data.layers['atr1']);
    if (!State.noThinning) { t = thin(t, State.chunk); a = thin(a, State.chunk); }

    const series = [];
    if (State.showTicks) {
      series.push({
        name:'mid', type:'line', yAxisIndex:0, showSymbol:false,
        data:t, sampling: State.noThinning? undefined : 'lttb',
        large:true, largeThreshold:200000
      });
    }
    if (atrWanted() && a.length) {
      series.push({
        name:'atr1', type:'line', yAxisIndex:1, showSymbol:false,
        data:a, sampling: State.noThinning? undefined : 'lttb',
        large:true, largeThreshold:200000, lineStyle:{width:1.5}
      });
    }

    const [xmn, xmx] = (State.xMin!=null && State.xMax!=null)
      ? [State.xMin, State.xMax]
      : (t.length ? [t[0][0], t[t.length-1][0]] : [null,null]);

    c.setOption({
      animation:false,
      tooltip:{ trigger:'axis', axisPointer:{type:'cross'} },
      xAxis:[{ type:'time', min: xmn ?? 'dataMin', max: xmx ?? 'dataMax' }],
      yAxis:[
        { type:'value', scale:true, name:'Price', axisLabel:{formatter:v=>(+v).toFixed(2)} },
        { type:'value', scale:true, name:'ATR', position:'right', axisLabel:{formatter:v=>(+v).toFixed(4)} }
      ],
      dataZoom:[ {type:'inside', throttle:0}, {type:'slider', height:20} ],
      series
    }, true);

    refitY();
    bindSegmDelegation();   // attach once; never touches the DOM structure
  }

  // ---------- Public API (same names) ----------
  window.ReviewCore = {
    setSegmData(segmObj) {
      State.data.segm = segmObj;
      // adopt segm window on first load if provided
      if (segmObj?.start_ts && segmObj?.end_ts && State.xMin==null && State.xMax==null) {
        const s = Date.parse(segmObj.start_ts), e = Date.parse(segmObj.end_ts);
        if (isFinite(s) && isFinite(e)) { State.xMin=s; State.xMax=e; }
      }
      render();
    },
    setLayerData(name, rows) {
      State.data.layers[name] = rows || [];
      render();
    },
    focusSegment(startTs, endTs) {
      const s = isFinite(+startTs)?+startTs:Date.parse(startTs);
      const e = isFinite(+endTs)?+endTs:Date.parse(endTs);
      if (isFinite(s) && isFinite(e)) {
        State.xMin=s; State.xMax=e;
        chart()?.setOption({ xAxis:[{min:s, max:e}] });
        refitY();
      }
    },
    init() {
      mountToggles();
      wireChunk();
      render();
    }
  };

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(() => window.ReviewCore.init(), 0);
  } else {
    document.addEventListener('DOMContentLoaded', () => window.ReviewCore.init());
  }
})();