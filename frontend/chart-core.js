// ===== Shared helpers for Live/Review =====
export const $ = (s, root=document) => root.querySelector(s);

export function makeChart(dom){
  const c = echarts.init(dom);
  c.setOption({
    darkMode: true, animation: false,
    backgroundColor: '#0d1117',
    grid: { left: 56, right: 18, top: 26, bottom: 100 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    xAxis: { type: 'time', axisLabel: { color: '#9ca3af' } },
    yAxis: {
      type: 'value', scale: true, minInterval: 1, splitNumber: 8,
      axisLabel: { color: '#9ca3af', formatter: v => String(Math.round(v)) },
      splitLine: { lineStyle: { color: '#263241' } }
    },
    dataZoom: [
      { type: 'inside', filterMode: 'none' },
      { type: 'slider', bottom: 56, height: 30 }
    ],
    series: []
  });
  return c;
}

export function priceSeries(name, data, width=1.4){
  return { name, type: 'line', showSymbol: false, smooth: 0, lineStyle: { width }, data };
}

export function rowsToZigzag(rows, name){
  const pts = [];
  for(const r of rows || []){
    if(r.start_ts && r.end_ts && r.start_price != null && r.end_price != null){
      pts.push([r.start_ts, r.start_price], [r.end_ts, r.end_price], [null, null]);
    }
  }
  return {
    name,
    type: 'line',
    connectNulls: false,
    showSymbol: true,
    symbolSize: 4,
    emphasis: { disabled: true },
    z: 20,
    lineStyle: { width: name==='max' ? 2 : name==='mid' ? 1.6 : 1.2 },
    data: pts
  };
}

export async function j(url){
  const r = await fetch(url);
  const t = await r.text();
  let b; try{ b = t ? JSON.parse(t) : null; }catch{ b = t; }
  if(!r.ok) throw new Error(`${r.status} : ${url}\n${typeof b==='string'?b:JSON.stringify(b)}`);
  return b;
}

// Keep viewport unless user is at far right ~98%+
export function keepOrFollowRight(chart, afterUpdate){
  const dz = chart.getOption().dataZoom?.[1];
  const end = dz ? dz.end : 100;
  const follow = end == null || end > 98;
  afterUpdate();
  if(follow){
    const opt = chart.getOption();
    if(opt.dataZoom && opt.dataZoom[1]){
      opt.dataZoom[1].end = 100;
      chart.setOption({ dataZoom: opt.dataZoom }, { replaceMerge: ['dataZoom'] });
    }
  }
}
