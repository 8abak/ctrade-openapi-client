// frontend/evals-core.js
// Simple evals visualizer using /api/evals/window.
//
// Requirements (already present in backend):
//   GET /api/evals/window?tick_from=...&tick_to=...&min_level=...
//
// Dot size: by level
// Dot color: by base_sign
// Optional line connecting points in tick order.

(function () {
  let chart;
  let statusEl;
  let inputFrom;
  let inputTo;
  let inputMinLevel;
  let btnLoad;
  let btnLast;

  function setStatus(msg) {
    if (statusEl) statusEl.textContent = msg || "";
  }

  function initChart() {
    const dom = document.getElementById("chart");
    chart = echarts.init(dom, "dark");

    chart.setOption({
      title: {
        text: "Evals window",
        left: "center",
        textStyle: { fontSize: 14 },
      },
      tooltip: {
        trigger: "item",
        formatter: function (p) {
          const d = p.data && p.data._meta;
          if (!d) return "";
          const parts = [];
          parts.push(
            `<b>tick</b>: ${d.tick_id} | <b>mid</b>: ${d.mid.toFixed(2)}`
          );
          parts.push(
            `<b>level</b>: ${d.level} | <b>sign</b>: ${d.base_sign} | <b>imp</b>: ${d.signed_importance}`
          );
          if (d.promotion_path) {
            parts.push(`<b>path</b>: ${d.promotion_path}`);
          }
          return parts.join("<br/>");
        },
      },
      legend: {
        type: "scroll",
        top: 26,
      },
      grid: {
        top: 60,
        left: 60,
        right: 20,
        bottom: 50,
      },
      xAxis: {
        type: "value",
        name: "tick_id",
        axisLabel: { color: "#aaa" },
        boundaryGap: ["5%", "5%"],
      },
      yAxis: {
        type: "value",
        name: "mid price",
        axisLabel: { color: "#aaa" },
        scale: true,
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
        },
        {
          type: "slider",
          xAxisIndex: 0,
          height: 20,
          bottom: 20,
        },
      ],
      series: [],
    });
  }

  function colorForSign(sign) {
    if (sign > 0) return "#46c37b"; // up
    if (sign < 0) return "#f35b64"; // down
    return "#aaaaaa"; // neutral
  }

  function sizeForLevel(level) {
    const lvl = Number(level) || 1;
    return Math.max(4, Math.min(20, 3 + lvl * 2));
  }

  function buildSeries(rows) {
    // Group by level (so we get one scatter line per level)
    const byLevel = new Map();
    rows.forEach((r) => {
      const level = Number(r.level) || 0;
      if (!byLevel.has(level)) byLevel.set(level, []);
      byLevel.get(level).push(r);
    });

    // Sort each level by tick_id
    for (const arr of byLevel.values()) {
      arr.sort((a, b) => a.tick_id - b.tick_id);
    }

    const series = [];
    const allPoints = [];

    Array.from(byLevel.keys())
      .sort((a, b) => a - b)
      .forEach((level) => {
        const points = byLevel.get(level).map((r) => {
          const mid = Number(r.mid);
          const val = [r.tick_id, mid];
          const dataPoint = {
            value: val,
            symbolSize: sizeForLevel(level),
            itemStyle: { color: colorForSign(Number(r.base_sign) || 0) },
            _meta: {
              tick_id: r.tick_id,
              mid: mid,
              base_sign: Number(r.base_sign) || 0,
              level: level,
              signed_importance: Number(r.signed_importance) || 0,
              promotion_path: r.promotion_path,
            },
          };
          allPoints.push(dataPoint);
          return dataPoint;
        });

        series.push({
          name: `L${level}`,
          type: "scatter",
          data: points,
        });
      });

    // Optional: one polyline showing the eval path regardless of level
    if (allPoints.length) {
      // sort by tick_id
      const sorted = allPoints
        .slice()
        .sort((a, b) => a._meta.tick_id - b._meta.tick_id);
      series.push({
        name: "sequence",
        type: "line",
        data: sorted.map((p) => p.value),
        symbol: "none",
        lineStyle: {
          width: 1,
          type: "dotted",
          color: "#8888ff",
        },
        emphasis: { disabled: true },
      });
    }

    return series;
  }

  async function fetchLastTicksWindow(defaultWindow) {
    // Use /api/sql to get MAX(id) from ticks
    const q = "select max(id) as max_id from ticks";
    const res = await fetch(`/api/sql?q=${encodeURIComponent(q)}`);
    if (!res.ok) {
      throw new Error("sql max(id) failed");
    }
    const payload = await res.json();
    const rows = payload.rows || [];
    if (!rows.length || rows[0].max_id == null) {
      throw new Error("no ticks");
    }
    const maxId = Number(rows[0].max_id);
    const win = defaultWindow || 50000;
    return {
      tick_to: maxId,
      tick_from: Math.max(1, maxId - win),
    };
  }

  async function loadWindow(fromId, toId, minLevel) {
    const minLvl = Number(minLevel) || 1;
    if (!fromId || !toId) {
      setStatus("tick_from / tick_to missing");
      return;
    }

    setStatus("Loading…");
    if (btnLoad) btnLoad.disabled = true;

    try {
      const url =
        `/api/evals/window?tick_from=${fromId}` +
        `&tick_to=${toId}` +
        `&min_level=${minLvl}`;
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const data = await res.json();
      const rows = data.evals || [];
      setStatus(
        `Loaded ${rows.length} evals [tick ${data.tick_from}..${data.tick_to}], min_level=${data.min_level}`
      );

      if (!chart) initChart();
      const series = buildSeries(rows);
      chart.setOption({
        xAxis: {
          min: data.tick_from,
          max: data.tick_to,
        },
        series: series,
      });
    } catch (err) {
      console.error("load window failed", err);
      setStatus("Error: " + err.message);
    } finally {
      if (btnLoad) btnLoad.disabled = false;
    }
  }

  function wireControls() {
    statusEl = document.getElementById("status");
    inputFrom = document.getElementById("tick-from");
    inputTo = document.getElementById("tick-to");
    inputMinLevel = document.getElementById("min-level");
    btnLoad = document.getElementById("btn-load");
    btnLast = document.getElementById("btn-last");

    if (btnLoad) {
      btnLoad.addEventListener("click", () => {
        const fromId = Number(inputFrom.value);
        const toId = Number(inputTo.value);
        const minLevel = Number(inputMinLevel.value) || 1;
        loadWindow(fromId, toId, minLevel);
      });
    }

    if (btnLast) {
      btnLast.addEventListener("click", async () => {
        try {
          setStatus("Finding last ticks…");
          btnLast.disabled = true;
          const win = await fetchLastTicksWindow(50000);
          inputFrom.value = win.tick_from;
          inputTo.value = win.tick_to;
          const minLevel = Number(inputMinLevel.value) || 1;
          await loadWindow(win.tick_from, win.tick_to, minLevel);
        } catch (err) {
          console.error(err);
          setStatus("Error: " + err.message);
        } finally {
          btnLast.disabled = false;
        }
      });
    }

    // Optional: auto-load last window on first load
    (async () => {
      try {
        const win = await fetchLastTicksWindow(50000);
        inputFrom.value = win.tick_from;
        inputTo.value = win.tick_to;
        const minLevel = Number(inputMinLevel.value) || 1;
        await loadWindow(win.tick_from, win.tick_to, minLevel);
      } catch (err) {
        console.error("auto load failed", err);
        setStatus("Ready (no auto window)");
      }
    })();
  }

  document.addEventListener("DOMContentLoaded", () => {
    initChart();
    wireControls();
  });
})();