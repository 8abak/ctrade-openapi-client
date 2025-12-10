// frontend/confirm.js
//
// Pure frontend "confirm lab" viewer.
// - Loads confirm_spots CSV from ../src/train/confirm_spots_tags/
// - Lets you choose a tag index (row)
// - Fetches ticks around pivot via existing review API
// - Draws the window with pivot/L1/H1/confirm/exit markers
// - Now also shows Kalman-smoothed price (ticks.kal) as optional overlay.

(() => {
  // ---------- CONFIG --------------------------------------------------------

  const CSV_PATH_PREFIX = "../src/train/confirm_spots_tags/";

  const WINDOW_BEFORE_TICKS = 500;
  const WINDOW_AFTER_TICKS  = 1000;

  // Use /api/review/window (from_id, window)
  function tickApiUrl(symbol, fromId, toId) {
    const windowSize = Math.max(1, toId - fromId + 1);
    const p = new URLSearchParams({
      from_id: String(fromId),
      window: String(windowSize),
    });
    return `/api/review/window?${p.toString()}`;
  }

  // ---------- DOM / ECharts setup ------------------------------------------

  const symbolInput   = document.getElementById("symbolInput");
  const datasetInput  = document.getElementById("datasetInput");
  const tagInput      = document.getElementById("tagInput");
  const showTagsInput = document.getElementById("showTagsInput");
  const showKalInput  = document.getElementById("showKalInput");
  const loadBtn       = document.getElementById("loadBtn");
  const infoDiv       = document.getElementById("info");

  const chartDom = document.getElementById("chart-container");
  const chart = echarts.init(chartDom);

  // ---------- CSV loading & parsing ----------------------------------------

  let csvRows = null;

  async function loadCsvOnce(datasetName) {
    if (csvRows && csvRows._datasetName === datasetName) {
      return csvRows;
    }
    const url = CSV_PATH_PREFIX + datasetName;
    const resp = await fetch(url);
    if (!resp.ok) {
      throw new Error(`Failed to load CSV: ${resp.status} ${resp.statusText}`);
    }
    const text = await resp.text();
    csvRows = parseCsv(text);
    csvRows._datasetName = datasetName;
    console.log(`Loaded CSV ${datasetName}, rows=${csvRows.length}`);
    return csvRows;
  }

  function parseCsv(text) {
    const lines = text.split(/\r?\n/).filter(line => line.trim().length > 0);
    if (lines.length === 0) return [];
    const header = lines[0].split(",").map(h => h.trim());
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i];
      if (!line.trim()) continue;
      const cols = splitCsvLine(line);
      const row = {};
      for (let j = 0; j < header.length; j++) {
        row[header[j]] = cols[j] !== undefined ? cols[j] : "";
      }
      rows.push(row);
    }
    return rows;
  }

  function splitCsvLine(line) {
    const result = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === "," && !inQuotes) {
        result.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
    result.push(current);
    return result;
  }

  // ---------- Tick API helpers ---------------------------------------------

  async function loadTicks(symbol, fromId, toId) {
    const url = tickApiUrl(symbol, fromId, toId);
    const resp = await fetch(url);
    if (!resp.ok) {
      throw new Error(`Failed to load ticks: ${resp.status} ${resp.statusText}`);
    }
    const data = await resp.json();
    let arr;
    if (Array.isArray(data)) {
      arr = data;
    } else if (Array.isArray(data.ticks)) {
      arr = data.ticks;
    } else {
      throw new Error("Unexpected tick API format");
    }
    return arr.map(normalizeTick).filter(t => t !== null);
  }

  function normalizeTick(raw) {
    const tick_id = raw.tick_id ?? raw.id;
    const tsRaw   = raw.ts ?? raw.timestamp ?? raw.time;
    const mid     = raw.mid ?? raw.price;
    const kal     = raw.kal ?? null;  // new: bring kalman value through

    if (tick_id == null || tsRaw == null || mid == null) {
      return null;
    }
    const evalLevel = raw.eval_level ?? raw.level ?? 0;
    return {
      tick_id: Number(tick_id),
      ts: new Date(tsRaw).toISOString(),
      mid: Number(mid),
      kal: kal != null ? Number(kal) : null,
      eval_level: Number(evalLevel),
    };
  }

  function nearestTick(ticks, targetTime) {
    if (!ticks.length) return null;
    const target = targetTime.getTime();
    let best = null;
    let bestAbs = Infinity;
    for (const t of ticks) {
      const dt = Math.abs(new Date(t.ts).getTime() - target);
      if (dt < bestAbs) {
        bestAbs = dt;
        best = t;
      }
    }
    return best;
  }

  // ---------- Chart option builder -----------------------------------------

  function buildOption(data, showTags, showKal) {
    const ticks = data.ticks;
    const series = [];

    // Price (mid)
    series.push({
      name: "mid",
      type: "line",
      data: ticks.map(t => [t.ts, t.mid]),
      showSymbol: false,
      smooth: false,
      lineStyle: { width: 1 },
    });

    // Kalman overlay (if requested and available)
    if (showKal) {
      const hasKal = ticks.some(t => t.kal != null);
      if (hasKal) {
        series.push({
          name: "kal",
          type: "line",
          data: ticks.map(t =>
            t.kal != null ? [t.ts, t.kal] : [t.ts, null]
          ),
          showSymbol: false,
          smooth: false,
          lineStyle: { width: 1 },
        });
      }
    }

    // Eval tags as scatter
    if (showTags) {
      const tagPoints = ticks
        .filter(t => t.eval_level >= 2)
        .map(t => ({
          value: [t.ts, t.mid],
          eval_level: t.eval_level,
        }));
      if (tagPoints.length > 0) {
        series.push({
          name: "L2+ tags",
          type: "scatter",
          data: tagPoints,
          symbolSize: 4,
        });
      }
    }

    // Markers for pivot / L1 / H1 / confirm / exit (on mid)
    function marker(event, color) {
      if (!event || !event.tick) return null;
      return {
        coord: [event.tick.ts, event.tick.mid],
        value: event.name,
        itemStyle: { color },
        label: { formatter: event.name, position: "top", fontSize: 10 },
      };
    }

    const markPoints = [
      marker({ name: "pivot", tick: data.pivot },   "#ffaa00"),
      marker({ name: "L1",    tick: data.L1 },      "#00ffaa"),
      marker({ name: "H1",    tick: data.H1 },      "#ff00ff"),
      marker({ name: "conf",  tick: data.confirm }, "#00aaff"),
      marker({ name: "exit",  tick: data.exit },    "#ffffff"),
    ].filter(Boolean);

    series.push({
      name: "markers",
      type: "line",
      data: ticks.map(t => [t.ts, t.mid]),
      showSymbol: false,
      lineStyle: { opacity: 0 },
      markPoint: {
        symbol: "circle",
        symbolSize: 8,
        data: markPoints,
      },
    });

    const sideText = data.side === "long" ? "LONG" : "SHORT";
    infoDiv.innerHTML =
      `<span class="badge ${data.side === "long" ? "badge-long" : "badge-short"}">${sideText}</span>` +
      `tag ${data.tag_index} &nbsp; ` +
      `pivot_type: ${data.pivot_type} &nbsp; ` +
      `net: ${data.net_return.toFixed(3)} &nbsp; ` +
      `raw: ${data.raw_return.toFixed(3)} &nbsp; ` +
      `MFE: ${data.MFE.toFixed(3)} &nbsp; ` +
      `MAE: ${data.MAE.toFixed(3)} &nbsp; ` +
      `stop_hit: ${data.stop_hit}`;

    return {
      backgroundColor: "#0b0c10",
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        formatter: params => {
          // params = [{seriesName, value:[ts, y]}, ...]
          const time = params[0].value[0];
          let lines = [time];

          const midItem = params.find(p => p.seriesName === "mid");
          if (midItem) {
            lines.push(`mid: ${midItem.value[1]}`);
          }

          const kalItem = params.find(p => p.seriesName === "kal");
          if (kalItem && kalItem.value[1] != null) {
            lines.push(`kal: ${kalItem.value[1]}`);
          }

          return lines.join("<br/>");
        },
      },
      grid: {
        left: 40,
        right: 20,
        top: 20,
        bottom: 30,
      },
      xAxis: { type: "time", boundaryGap: false },
      yAxis: { type: "value", scale: true },
      series,
    };
  }

  // ---------- Main load/render logic ---------------------------------------

  async function loadAndRender() {
    const symbol   = symbolInput.value.trim();
    const dataset  = datasetInput.value.trim();
    const tagIdx   = parseInt(tagInput.value, 10);
    const showTags = showTagsInput.checked;
    const showKal  = showKalInput.checked;

    if (!symbol || !dataset || !tagIdx) return;

    infoDiv.textContent = "Loading...";
    try {
      const rows = await loadCsvOnce(dataset);
      if (tagIdx < 1 || tagIdx > rows.length) {
        infoDiv.textContent = `Tag index out of range (1..${rows.length})`;
        return;
      }
      const row = rows[tagIdx - 1];

      const pivotTickId = Number(row["pivot_tick_id"]);
      const pivotTime   = new Date(row["pivot_time"]);

      const durL1   = Number(row["dur_pivot_to_L1_sec"]);
      const durH1   = Number(row["dur_pivot_to_H1_sec"]);
      const durConf = Number(row["dur_pivot_to_confirm_sec"]);
      const durExit = Number(row["dur_pivot_to_exit_sec"]);

      const L1Time      = new Date(pivotTime.getTime() + durL1   * 1000);
      const H1Time      = new Date(pivotTime.getTime() + durH1   * 1000);
      const confirmTime = new Date(pivotTime.getTime() + durConf * 1000);
      const exitTime    = new Date(pivotTime.getTime() + durExit * 1000);

      const fromId = Math.max(1, pivotTickId - WINDOW_BEFORE_TICKS);
      const toId   = pivotTickId + WINDOW_AFTER_TICKS;

      const ticks = await loadTicks(symbol, fromId, toId);
      if (!ticks.length) {
        infoDiv.textContent = "No ticks in this window.";
        return;
      }

      const pivotTick   = nearestTick(ticks, pivotTime);
      const L1Tick      = nearestTick(ticks, L1Time);
      const H1Tick      = nearestTick(ticks, H1Time);
      const confirmTick = nearestTick(ticks, confirmTime);
      const exitTick    = nearestTick(ticks, exitTime);

      const dataForChart = {
        symbol,
        tag_index: Number(row["tag_index"]),
        pivot_type: row["pivot_type"],
        side: row["side"],
        stop_price: Number(row["stop_price"]),
        raw_return: Number(row["raw_return"]),
        net_return: Number(row["net_return"]),
        MFE: Number(row["MFE"]),
        MAE: Number(row["MAE"]),
        stop_hit: row["stop_hit"] === "True" || row["stop_hit"] === "true" || row["stop_hit"] === "1",
        ticks,
        pivot:   pivotTick,
        L1:      L1Tick,
        H1:      H1Tick,
        confirm: confirmTick,
        exit:    exitTick,
      };

      const option = buildOption(dataForChart, showTags, showKal);
      chart.setOption(option, true);
    } catch (err) {
      console.error(err);
      infoDiv.textContent = `Error: ${err.message}`;
    }
  }

  loadBtn.addEventListener("click", loadAndRender);
  tagInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      loadAndRender();
    }
  });
  showTagsInput.addEventListener("change", loadAndRender);
  showKalInput.addEventListener("change", loadAndRender);

  loadAndRender();
})();
