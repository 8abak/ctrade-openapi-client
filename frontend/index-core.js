// frontend/evals-core.js
// Evals visualiser for Segmeling.
//
// Backend contract (main.py):
//   GET /api/evals/window
//     ?tick_from=<int>
//     &tick_to=<int>
//     &min_level=<int, default 1>
//     &max_rows=<int, default 200000>
//   Response:
//     {
//       tick_from, tick_to, min_level, max_rows,
//       truncated: bool,
//       evals: [
//         {
//           id, tick_id, mid, timestamp,
//           base_sign, level, signed_importance,
//           promotion_path, computed_at
//         }, ...
//       ]
//     }
//
// Also uses /api/sql?q=SELECT max(id) AS max_id FROM ticks

(function () {
  // ---- Tunables -----------------------------------------------------

  // How many rows we allow backend to return in one window.
  // Should be <= default max_rows in backend.
  const MAX_FETCH_ROWS = 200_000;

  // How many points we actually plot. If we get more,
  // we sample evenly along the array index.
  const MAX_POINTS = 40_000;

  // -------------------------------------------------------------------

  let chart;
  let statusEl;
  let inputFrom;
  let inputTo;
  let inputMinLevel;
  let btnLoad;
  let btnLast;

  // ----------------- DOM helpers -------------------------------------

  function $(id) {
    return document.getElementById(id);
  }

  function setStatus(msg) {
    if (statusEl) statusEl.textContent = msg || "";
  }

  // ----------------- Visual helpers ----------------------------------

  function colorForSign(sign) {
    if (sign > 0) return "#4caf50"; // up
    if (sign < 0) return "#f44336"; // down
    return "#9e9e9e"; // neutral / 0
  }

  function sizeForLevel(level) {
    const lvl = Number(level) || 1;
    // Base 4 px plus 2 per level, clamped
    return Math.max(4, Math.min(20, 4 + lvl * 2));
  }

  function initChart() {
    const dom = $("chart");
    if (!dom) {
      console.error("evals-core: #chart not found");
      return;
    }

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

  // ----------------- Series builder ----------------------------------

  function buildSeries(rows) {
    if (!chart) {
      console.error("evals-core: chart not initialised");
      return [];
    }

    if (!rows || !rows.length) {
      chart.__evalDiag = {
        total: 0,
        used: 0,
        sampledFrom: 0,
      };
      return [];
    }

    const total = rows.length;

    // Down-sample if too many rows
    let usedRows = rows;
    let sampledFrom = total;
    if (total > MAX_POINTS) {
      const stride = Math.ceil(total / MAX_POINTS);
      usedRows = rows.filter((_, idx) => idx % stride === 0);
      sampledFrom = total;
    }

    // Group by level
    const byLevel = new Map();
    let used = 0;

    for (const r of usedRows) {
      const tickId = Number(r.tick_id);
      const mid = Number(r.mid);
      const level = Number(r.level);
      const baseSign = Number(r.base_sign);
      const imp = Number(r.signed_importance) || 0;
      const promotionPath = r.promotion_path;

      if (!Number.isFinite(tickId) || !Number.isFinite(mid)) continue;
      if (!Number.isFinite(level)) continue;

      if (!byLevel.has(level)) byLevel.set(level, []);
      byLevel.get(level).push({
        tickId,
        mid,
        level,
        baseSign,
        imp,
        promotionPath,
      });
      used++;
    }

    chart.__evalDiag = {
      total,
      used,
      sampledFrom,
    };

    // Sort by tick_id within each level
    for (const arr of byLevel.values()) {
      arr.sort((a, b) => a.tickId - b.tickId);
    }

    const series = [];
    const allPoints = [];

    // Scatter per level
    Array.from(byLevel.keys())
      .sort((a, b) => a - b)
      .forEach((level) => {
        const arr = byLevel.get(level);
        const points = arr.map((r) => {
          const dp = {
            value: [r.tickId, r.mid],
            symbolSize: sizeForLevel(r.level),
            itemStyle: { color: colorForSign(r.baseSign) },
            _meta: {
              tick_id: r.tickId,
              mid: r.mid,
              level: r.level,
              base_sign: r.baseSign,
              signed_importance: r.imp,
              promotion_path: r.promotionPath,
            },
          };
          allPoints.push(dp);
          return dp;
        });

        series.push({
          name: `L${level}`,
          type: "scatter",
          data: points,
        });
      });

    // Sequence line across all evals
    if (allPoints.length) {
      const seq = allPoints
        .slice()
        .sort((a, b) => a._meta.tick_id - b._meta.tick_id)
        .map((p) => p.value);

      series.push({
        name: "sequence",
        type: "line",
        data: seq,
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

  // ----------------- Backend helpers ---------------------------------

  async function fetchLastTicksWindow(defaultWindow) {
    // Use /api/sql to get MAX(id) from ticks
    const q = "SELECT max(id) AS max_id FROM ticks";
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
    const win = defaultWindow || 50_000;
    return {
      tick_to: maxId,
      tick_from: Math.max(1, maxId - win),
    };
  }

  async function loadWindow(fromId, toId, minLevel) {
    let start = Number(fromId);
    let end = Number(toId);
    const minLvl = Number(minLevel) || 1;

    if (!Number.isFinite(start) || !Number.isFinite(end)) {
      setStatus("tick_from / tick_to missing or invalid");
      return;
    }

    if (end < start) {
      const tmp = start;
      start = end;
      end = tmp;
    }

    setStatus("Loading…");
    if (btnLoad) btnLoad.disabled = true;

    try {
      const url =
        `/api/evals/window?tick_from=${start}` +
        `&tick_to=${end}` +
        `&min_level=${minLvl}` +
        `&max_rows=${MAX_FETCH_ROWS}`;

      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      const data = await res.json();
      const rows = data.evals || [];

      if (!chart) initChart();
      const series = buildSeries(rows);
      const diag = chart.__evalDiag || {};
      const used = diag.used || 0;
      const sampledFrom = diag.sampledFrom || rows.length;
      const truncated = !!data.truncated;

      let msg =
        `Loaded ${rows.length} evals [tick ${data.tick_from}..${data.tick_to}], ` +
        `min_level=${data.min_level}; plotted=${used}`;

      if (sampledFrom > used) {
        msg += ` (sampled from ${sampledFrom})`;
      }
      if (truncated) {
        msg += ` – WARNING: truncated at max_rows=${data.max_rows}, narrow tick window`;
      }

      setStatus(msg);

      chart.setOption({
        xAxis: {
          min: data.tick_from,
          max: data.tick_to,
        },
        series: series,
      });
    } catch (err) {
      console.error("evals-core: loadWindow failed", err);
      setStatus("Error: " + err.message);
    } finally {
      if (btnLoad) btnLoad.disabled = false;
    }
  }

  // ----------------- Wiring DOM events --------------------------------

  function wireControls() {
    statusEl = $("status");
    inputFrom = $("tick-from");
    inputTo = $("tick-to");
    inputMinLevel = $("min-level");
    btnLoad = $("btn-load");
    btnLast = $("btn-last");

    if (btnLoad) {
      btnLoad.addEventListener("click", () => {
        const fromId = inputFrom.value;
        const toId = inputTo.value;
        const minLevel = inputMinLevel.value;
        loadWindow(fromId, toId, minLevel);
      });
    }

    if (btnLast) {
      btnLast.addEventListener("click", async () => {
        try {
          setStatus("Finding last ticks…");
          btnLast.disabled = true;
          const win = await fetchLastTicksWindow(50_000);
          inputFrom.value = win.tick_from;
          inputTo.value = win.tick_to;
          const minLevel = inputMinLevel.value || 1;
          await loadWindow(win.tick_from, win.tick_to, minLevel);
        } catch (err) {
          console.error("evals-core: last window failed", err);
          setStatus("Error: " + err.message);
        } finally {
          btnLast.disabled = false;
        }
      });
    }

    // Auto-load last window on first load
    (async () => {
      try {
        const win = await fetchLastTicksWindow(50_000);
        inputFrom.value = win.tick_from;
        inputTo.value = win.tick_to;
        const minLevel = inputMinLevel.value || 1;
        await loadWindow(win.tick_from, win.tick_to, minLevel);
      } catch (err) {
        console.error("evals-core: auto load failed", err);
        setStatus("Ready (no auto window)");
      }
    })();
  }

  // ----------------- Init --------------------------------------------

  document.addEventListener("DOMContentLoaded", () => {
    initChart();
    wireControls();
  });
})();