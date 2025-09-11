// ===== chart-core.js =====
(function (global) {
  'use strict';

  async function fetchJSON(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`${r.status} : ${url}`);
    return r.json();
  }

  function ticksToLine(ticks, key) {
    const out = [];
    for (const t of ticks) {
      const v = t[key];
      if (v !== null && v !== undefined) out.push([t.id, v]);
    }
    return out;
  }

  function legsToPath(legs) {
    const out = [];
    for (const r of legs) {
      out.push([r.start_id, r.start_price]);
      out.push([r.end_id,   r.end_price]);
    }
    return out;
  }

  function makeChart(dom) {
    const chart = echarts.init(dom, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: '#0b1220',
      animation: false,
      grid: { left: 50, right: 20, top: 40, bottom: 70 },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'line' },
        confine: true,
        backgroundColor: 'rgba(20,20,20,0.95)',
        borderColor: '#333',
        textStyle: { color: '#d8d8d8', fontSize: 12 },
      },
      xAxis: {
        type: 'value',
        minInterval: 1,
        scale: true,
        axisLine: { lineStyle: { color: '#8a93a6' } },
        axisLabel: { color: '#cfd5e1' },
      },
      yAxis: {
        type: 'value',
        scale: true,
        axisLine: { lineStyle: { color: '#8a93a6' } },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        axisLabel: { color: '#cfd5e1', formatter: (v) => Math.round(v) },
      },
      dataZoom: [
        { type: 'inside', throttle: 50 },
        { type: 'slider', bottom: 30, height: 18 }
      ],
      series: []
    });
    return chart;
  }

  function priceLineSeries(name, data, z) {
    return {
      name,
      type: 'line',
      xAxisIndex: 0, yAxisIndex: 0,
      data,
      showSymbol: false,
      connectNulls: true,
      smooth: 0.15,
      lineStyle: { width: 1.5 },
      z
    };
  }

  global.ChartCore = {
    fetchJSON,
    makeChart,
    priceLineSeries,
    ticksToLine,
    legsToPath,
  };
})(window);
