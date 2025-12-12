// frontend/confirm.js
//
// Confirm lab viewer.
// - Lists *.csv in ../src/train/tags/ (directory listing HTML)
// - Loads selected CSV
// - Supports TWO schemas:
//    A) "new" schema: pivot_tick_id, pivot_time, dur_pivot_to_*_sec, ...
//    B) "current tags_XAUUSD_tags_1_600.csv" schema:
//       row,tag,id,L1,H1,conf,close,price_*,date,time,t_L1,t_H1,t_conf,t_close,net,gnet,side
// - Fetches ticks via /api/review/window (from_id, window)
// - Draws pivot/L1/H1/confirm/exit markers; overlays eval tags and kalman line.

(() => {
  // ---------- CONFIG --------------------------------------------------------

  const CSV_PATH_PREFIX = "../src/train/tags/";

  const WINDOW_BEFORE_TICKS = 500;
  const WINDOW_AFTER_TICKS  = 1000;

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
  const datasetSelect = document.getElementById("datasetSelect");
  const datasetInput  = document.getElementById("datasetInput");
  const tagInput      = document.getElementById("tagInput");
  const showTagsInput = document.getElementById("showTagsInput");
  const showKalInput  = document.getElementById("showKalInput");
  const loadBtn       = document.getElementById("loadBtn");
  const infoDiv       = document.getElementById("info");

  const chartDom = document.getElementById("chart-container");
  const chart = echarts.init(chartDom);

  // ---------- CSV listing ---------------------------------------------------

  async function listCsvFiles() {
    // Requires directory listing enabled at /src/train/tags/
    const resp = await fetch(CSV_PATH_PREFIX, { cache: "no-store" });
    if (!resp.ok) return [];
    const html = await resp.text();
    const doc = new DOMParser().parseFromString(html, "text/html");

    const files = Array.from(doc.querySelectorAll("a"))
      .map(a => (a.getAttribute("href") || "").trim())
      .filter(h => h.toLowerCase().endsWith(".csv"))
      .map(h => h.split("/").pop())
      .filter(Boolean);

    return Array.from(new Set(files)).sort((a, b) => a.localeCompare(b));
  }

  async function populateDatasetSelect() {
    if (!datasetSelect) return;

    datasetSelect.innerHTML = `<option value="">(loading...)</option>`;
    const files = await listCsvFiles();

    if (!files.length) {
      datasetSelect.innerHTML = `<option value="">(no csv found)</option>`;
      return;
    }

    datasetSelect.innerHTML = "";
    for (const f of files) {
      const opt = document.createElement("option");
      opt.value = f;
      opt.textContent = f;
      datasetSelect.appendChild(opt);
    }

    const typed = datasetInput.value.trim();
    if (typed && files.includes(typed)) {
      datasetSelect.value = typed;
    } else {
      datasetSelect.value = files[files.length - 1];
      datasetInput.value = datasetSelect.value;
    }
  }

  // ---------- CSV loading & parsing ----------------------------------------

  let csvRows = null;

  async function loadCsvOnce(datasetName) {
    if (csvRows && csvRows._datasetName === datasetName) return csvRows;

    const url = CSV_PATH_PREFIX + datasetName;
    const resp = await fetch(url, { cache: "no-store" });
    if (!resp.ok) throw new Error(`Failed to load CSV: ${resp.status} ${resp.statusText}`);

    const text = await resp.text();
    csvRows = parseCsv(text);
    csvRows._datasetName = datasetName;
    console.log(`Loaded CSV ${datasetName}, rows=${csvRows.length}`);
    return csvRows;
  }

  function parseCsv(text) {
    const lines = text.split(/\r?\n/).filter(line => line.trim().length > 0);
    if (lines.length === 0) return [];

    // Handle BOM if present
    const headerLine = lines[0].replace(/^\uFEFF/, "");
    const header = headerLine.split(",").map(h => h.trim());

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
    const resp = await fetch(url, { cache: "no-store" });
    if (!resp.ok) {
      throw new Error(`Failed to load ticks: ${resp.status} ${resp.statusText}`);
    }
    const data = await resp.json();

    let arr;
    if (Array.isArray(data)) arr = data;
    else if (Array.isArray(data.ticks)) arr = data.ticks;
    else throw new Error("Unexpected tick API format");

    return arr.map(normalizeTick).filter(t => t !== null);
  }

  function normalizeTick(raw) {
    const tick_id = raw.tick_id ?? raw.id;
    const tsRaw   = raw.ts ?? raw.timestamp ?? raw.time;
    const mid     = raw.mid ?? raw.price;
    const kal     = raw.kal ?? null;

    if (tick_id == null || tsRaw == null || mid == null) return null;

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

  function findTickById(ticks, id) {
    const nid = Number(id);
    if (!Number.isFinite(nid)) return null;
    return ticks.find(t => t.tick_id === nid) || null;
  }

  // ---------- Schema adapter -----------------------------------------------

  function parseTagRow(row, tagIdxFallback) {
    // Detect schema A (new)
    const hasNew = ("pivot_tick_id" in row) || ("pivot_time" in row) || ("dur_pivot_to_L1_sec" in row);

    if (hasNew) {
      const pivotTickId = Number(row["pivot_tick_id"]);
      const pivotTime   = new Date(row["pivot_time"]);

      const durL1   = Number(row["dur_pivot_to_L1_sec"]);
      const durH1   = Number(row["dur_pivot_to_H1_sec"]);
      const durConf = Number(row["dur_pivot_to_confirm_sec"]);
      const durExit = Number(row["dur_pivot_to_exit_sec"]);

      return {
        schema: "new",
        pivotTickId,
        pivotTime,
        // times derived from durations
        L1Time: new Date(pivotTime.getTime() + durL1   * 1000),
        H1Time: new Date(pivotTime.getTime() + durH1   * 1000),
        confirmTime: new Date(pivotTime.getTime() + durConf * 1000),
        exitTime: new Date(pivotTime.getTime() + durExit * 1000),

        tag_index: Number(row["tag_index"] || tagIdxFallback),
        pivot_type: row["pivot_type"] || "",
        side: row["side"] || "",
        net_return: Number(row["net_return"] || row["net"] || 0),
        raw_return: Number(row["raw_return"] || 0),
        MFE: Number(row["MFE"] || 0),
        MAE: Number(row["MAE"] || 0),
        stop_hit: String(row["stop_hit"] || "").toLowerCase() === "true" || String(row["stop_hit"]) === "1",
      };
    }

    // Schema B (your current CSV: row,tag,id,L1,H1,conf,close,...,date,time,t_*...)
    const pivotTickId = Number(row["id"]);
    const date = String(row["date"] || "").trim();
    const time = String(row["time"] || "").trim();

    // Build ISO-ish timestamp (assumes server/browser local is OK; your old code used Date() anyway)
    const pivotTime = new Date(`${date}T${time}`);

    const tL1   = Number(row["t_L1"]);
    const tH1   = Number(row["t_H1"]);
    const tConf = Number(row["t_conf"]);
    const tExit = Number(row["t_close"]); // close == exit

    return {
      schema: "legacy",
      pivotTickId,
      pivotTime,
      L1Time: new Date(pivotTime.getTime() + tL1   * 1000),
      H1Time: new Date(pivotTime.getTime() + tH1   * 1000),
      confirmTime: new Date(pivotTime.getTime() + tConf * 1000),
      exitTime: new Date(pivotTime.getTime() + tExit * 1000),

      // also have explicit tick ids for markers
      L1Id: Number(row["L1"]),
      H1Id: Number(row["H1"]),
      confirmId: Number(row["conf"]),
      exitId: Number(row["close"]),

      tag_index: Number(row["tag"] || row["row"] || tagIdxFallback),
      pivot_type: row["pivot_type"] || "",
      side: row["side"] || "",
      net_return: Number(row["net"] || 0),
      raw_return: Number(row["gnet"] || 0), // show gnet in "raw" slot to keep UI stable
      MFE: 0,
      MAE: 0,
      stop_hit: false,
    };
  }

  // ---------- Chart option builder -----------------------------------------

  function buildOption(data, showTags, showKal) {
    const ticks = data.ticks;
    const series = [];

    series.push({
      name: "mid",
      type: "line",
      data: ticks.map(t => [t.ts, t.mid]),
      showSymbol: false,
      smooth: false,
      lineStyle: { width: 1 },
    });

    if (showKal) {
      const hasKal = ticks.some(t => t.kal != null);
      if (hasKal) {
        series.push({
          name: "kal",
          type: "line",
          data: ticks.map(t => (t.kal != null ? [t.ts, t.kal] : [t.ts, null])),
          showSymbol: false,
          smooth: false,
          lineStyle: { width: 1 },
        });
      }
    }

    if (showTags) {
      const tagPoints = ticks
        .filter(t => t.eval_level >= 2)
        .map(t => ({ value: [t.ts, t.mid], eval_level: t.eval_level }));

      if (tagPoints.length > 0) {
        series.push({
          name: "L2+ tags",
          type: "scatter",
          data: tagPoints,
          symbolSize: 4,
        });
      }
    }

    function marker(name, tick, color) {
      if (!tick) return null;
      return {
        coord: [tick.ts, tick.mid],
        value: name,
        itemStyle: { color },
        label: { formatter: name, position: "top", fontSize: 10 },
      };
    }

    const markPoints = [
      marker("pivot", data.pivot,   "#ffaa00"),
      marker("L1",    data.L1,      "#00ffaa"),
      marker("H1",    data.H1,      "#ff00ff"),
      marker("conf",  data.confirm, "#00aaff"),
      marker("exit",  data.exit,    "#ffffff"),
    ].filter(Boolean);

    series.push({
      name: "markers",
      type: "line",
      data: ticks.map(t => [t.ts, t.mid]),
      showSymbol: false,
      lineStyle: { opacity: 0 },
      markPoint: { symbol: "circle", symbolSize: 8, data: markPoints },
    });

    const sideText = data.side === "long" ? "LONG" : "SHORT";
    infoDiv.innerHTML =
      `<span class="badge ${data.side === "long" ? "badge-long" : "badge-short"}">${sideText}</span>` +
      `tag ${data.tag_index} &nbsp; ` +
      (data.pivot_type ? `pivot_type: ${data.pivot_type} &nbsp; ` : "") +
      `net: ${Number(data.net_return || 0).toFixed(3)} &nbsp; ` +
      `raw: ${Number(data.raw_return || 0).toFixed(3)} &nbsp; ` +
      `MFE: ${Number(data.MFE || 0).toFixed(3)} &nbsp; ` +
      `MAE: ${Number(data.MAE || 0).toFixed(3)} &nbsp; ` +
      `stop_hit: ${data.stop_hit}`;

    return {
      backgroundColor: "#0b0c10",
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        formatter: params => {
          const time = params[0].value[0];
          const lines = [time];

          const midItem = params.find(p => p.seriesName === "mid");
          if (midItem) lines.push(`mid: ${midItem.value[1]}`);

          const kalItem = params.find(p => p.seriesName === "kal");
          if (kalItem && kalItem.value[1] != null) lines.push(`kal: ${kalItem.value[1]}`);

          return lines.join("<br/>");
        },
      },
      grid: { left: 40, right: 20, top: 20, bottom: 30 },
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
      const tag = parseTagRow(row, tagIdx);

      if (!Number.isFinite(tag.pivotTickId) || tag.pivotTickId <= 0) {
        infoDiv.textContent = `Invalid pivot_tick_id: ${tag.pivotTickId}`;
        return;
      }

      const fromId = Math.max(1, tag.pivotTickId - WINDOW_BEFORE_TICKS);
      const toId   = tag.pivotTickId + WINDOW_AFTER_TICKS;

      let ticks;
      try {
        ticks = await loadTicks(symbol, fromId, toId);
      } catch (e) {
        infoDiv.textContent = `Error: ${e.message}`;
        return;
      }

      if (!ticks.length) {
        infoDiv.textContent = "No ticks in this window.";
        return;
      }

      // Prefer explicit tick ids (schema B), else time-based nearest (schema A)
      const pivotTick   = findTickById(ticks, tag.pivotTickId) || nearestTick(ticks, tag.pivotTime);

      const L1Tick      =
        (tag.L1Id ? findTickById(ticks, tag.L1Id) : null) ||
        nearestTick(ticks, tag.L1Time);

      const H1Tick      =
        (tag.H1Id ? findTickById(ticks, tag.H1Id) : null) ||
        nearestTick(ticks, tag.H1Time);

      const confirmTick =
        (tag.confirmId ? findTickById(ticks, tag.confirmId) : null) ||
        nearestTick(ticks, tag.confirmTime);

      const exitTick    =
        (tag.exitId ? findTickById(ticks, tag.exitId) : null) ||
        nearestTick(ticks, tag.exitTime);

      const dataForChart = {
        symbol,
        tag_index: tag.tag_index,
        pivot_type: tag.pivot_type,
        side: tag.side,
        net_return: tag.net_return,
        raw_return: tag.raw_return,
        MFE: tag.MFE,
        MAE: tag.MAE,
        stop_hit: tag.stop_hit,
        ticks,
        pivot: pivotTick,
        L1: L1Tick,
        H1: H1Tick,
        confirm: confirmTick,
        exit: exitTick,
      };

      const option = buildOption(dataForChart, showTags, showKal);
      chart.setOption(option, true);
    } catch (err) {
      console.error(err);
      infoDiv.textContent = `Error: ${err.message}`;
    }
  }

  // ---------- Wiring --------------------------------------------------------

  loadBtn.addEventListener("click", loadAndRender);

  if (datasetSelect) {
    datasetSelect.addEventListener("change", () => {
      if (!datasetSelect.value) return;
      datasetInput.value = datasetSelect.value;
      csvRows = null; // force reload for new file
      loadAndRender();
    });
  }

  datasetInput.addEventListener("change", () => {
    csvRows = null;
  });

  tagInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") loadAndRender();
  });

  showTagsInput.addEventListener("change", loadAndRender);
  showKalInput.addEventListener("change", loadAndRender);

  // Boot
  (async () => {
    try {
      await populateDatasetSelect();
    } catch (e) {
      console.warn("populateDatasetSelect failed:", e);
    }
    loadAndRender();
  })();
})();
