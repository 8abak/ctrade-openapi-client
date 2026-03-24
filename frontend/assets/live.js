(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    window: 2000,
  };

  const state = {
    rows: [],
    currentMode: DEFAULTS.mode,
    currentRun: DEFAULTS.run,
    visibleSpanMs: null,
    source: null,
    chart: null,
  };

  const elements = {
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    tickId: document.getElementById("tickId"),
    windowSize: document.getElementById("windowSize"),
    applyButton: document.getElementById("applyButton"),
    statusLine: document.getElementById("statusLine"),
    liveMeta: document.getElementById("liveMeta"),
    chartHost: document.getElementById("liveChart"),
  };

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const mode = params.get("mode") === "review" ? "review" : DEFAULTS.mode;
    const run = params.get("run") === "stop" ? "stop" : DEFAULTS.run;
    const id = params.get("id") || DEFAULTS.id;
    const windowSize = Number.parseInt(params.get("window"), 10);
    return {
      mode,
      run,
      id,
      window: Number.isFinite(windowSize) && windowSize > 0 ? windowSize : DEFAULTS.window,
    };
  }

  function setSegment(container, value) {
    container.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", button.dataset.value === value);
    });
  }

  function syncControls(config) {
    state.currentMode = config.mode;
    state.currentRun = config.run;
    elements.tickId.value = config.id || "";
    elements.windowSize.value = String(config.window);
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
  }

  function currentConfig() {
    return {
      mode: state.currentMode,
      run: state.currentRun,
      id: elements.tickId.value.trim(),
      window: Math.max(1, Math.min(10000, Number.parseInt(elements.windowSize.value, 10) || DEFAULTS.window)),
    };
  }

  function writeQuery(config) {
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("window", String(config.window));
    if (config.id) {
      params.set("id", config.id);
    }
    history.replaceState(null, "", "/live?" + params.toString());
  }

  function status(text, isError) {
    elements.statusLine.textContent = text;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function ensureChart() {
    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      state.chart.on("datazoom", updateVisibleSpanFromChart);
      window.addEventListener("resize", () => state.chart.resize());
    }
    return state.chart;
  }

  function updateVisibleSpanFromChart() {
    if (!state.chart || !state.rows.length) {
      return;
    }
    const option = state.chart.getOption();
    const dataZoom = option.dataZoom && option.dataZoom[0];
    if (!dataZoom) {
      return;
    }
    if (typeof dataZoom.startValue === "number" && typeof dataZoom.endValue === "number" && dataZoom.endValue > dataZoom.startValue) {
      state.visibleSpanMs = dataZoom.endValue - dataZoom.startValue;
      return;
    }

    const xs = state.rows.map((row) => row.timestampMs);
    const min = xs[0];
    const max = xs[xs.length - 1];
    const startMs = min + (max - min) * ((dataZoom.start || 0) / 100);
    const endMs = min + (max - min) * ((dataZoom.end || 100) / 100);
    if (endMs > startMs) {
      state.visibleSpanMs = endMs - startMs;
    }
  }

  function gapAreas(rows) {
    const gaps = [];
    for (let index = 1; index < rows.length; index += 1) {
      const previous = rows[index - 1];
      const current = rows[index];
      if ((current.timestampMs - previous.timestampMs) >= 60000) {
        gaps.push([{ xAxis: previous.timestampMs }, { xAxis: current.timestampMs }]);
      }
    }
    return gaps;
  }

  function renderChart(forceWindow) {
    const chart = ensureChart();
    const rows = state.rows;
    const points = rows.map((row) => [row.timestampMs, row.price]);
    const firstTs = rows.length ? rows[0].timestampMs : Date.now() - 60000;
    const lastTs = rows.length ? rows[rows.length - 1].timestampMs : Date.now();
    const defaultSpan = lastTs > firstTs ? lastTs - firstTs : 60000;
    if (!state.visibleSpanMs || forceWindow) {
      state.visibleSpanMs = defaultSpan;
    }
    const span = Math.max(1000, state.visibleSpanMs || defaultSpan);
    const startValue = Math.max(firstTs, lastTs - span);
    const gaps = gapAreas(rows);

    chart.setOption({
      animation: false,
      backgroundColor: "transparent",
      grid: { left: 62, right: 28, top: 24, bottom: 110 },
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(6, 11, 20, 0.96)",
        borderColor: "rgba(109, 216, 255, 0.25)",
        textStyle: { color: "#f3f6fb" },
        axisPointer: {
          type: "cross",
          lineStyle: { color: "rgba(109, 216, 255, 0.35)" },
        },
        formatter(params) {
          if (!params.length) {
            return "";
          }
          const point = params[0];
          const date = new Date(point.value[0]);
          return [
            date.toLocaleString("en-AU", { hour12: false }),
            "Price: " + Number(point.value[1]).toFixed(2),
          ].join("<br>");
        },
      },
      xAxis: {
        type: "time",
        axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.28)" } },
        axisLabel: {
          color: "#9eadc5",
          formatter(value) {
            return new Date(value).toLocaleTimeString("en-AU", {
              hour12: false,
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            });
          },
        },
        splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: {
          color: "#9eadc5",
          formatter(value) {
            return Number(value).toFixed(2);
          },
        },
        axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.28)" } },
        splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
      },
      dataZoom: [
        {
          type: "inside",
          filterMode: "none",
          startValue,
          endValue: lastTs,
        },
        {
          type: "slider",
          height: 52,
          bottom: 28,
          filterMode: "none",
          startValue,
          endValue: lastTs,
          borderColor: "rgba(147, 181, 255, 0.16)",
          backgroundColor: "rgba(8, 13, 23, 0.94)",
          fillerColor: "rgba(109, 216, 255, 0.16)",
          dataBackground: {
            lineStyle: { color: "rgba(109, 216, 255, 0.55)" },
            areaStyle: { color: "rgba(109, 216, 255, 0.12)" },
          },
        },
      ],
      series: [
        {
          name: "XAUUSD",
          type: "line",
          showSymbol: false,
          smooth: false,
          data: points,
          lineStyle: { width: 2, color: "#6dd8ff" },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: "rgba(109, 216, 255, 0.22)" },
              { offset: 1, color: "rgba(109, 216, 255, 0.02)" },
            ]),
          },
          markArea: gaps.length ? {
            silent: true,
            itemStyle: { color: "rgba(255, 200, 87, 0.10)" },
            data: gaps,
          } : undefined,
        },
      ],
    }, true);

    updateVisibleSpanFromChart();
    if (rows.length) {
      const lastRow = rows[rows.length - 1];
      elements.liveMeta.textContent = [
        "Rows " + rows.length,
        "Last id " + lastRow.id,
        "Price " + Number(lastRow.price).toFixed(2),
        new Date(lastRow.timestampMs).toLocaleString("en-AU", { hour12: false }),
      ].join(" | ");
    } else {
      elements.liveMeta.textContent = "No rows returned.";
    }
  }

  function closeStream() {
    if (state.source) {
      state.source.close();
      state.source = null;
    }
  }

  function connectStream(lastId, windowSize) {
    closeStream();
    if (state.currentRun !== "run") {
      return;
    }

    const params = new URLSearchParams({
      afterId: String(lastId || 0),
      limit: String(Math.max(50, Math.min(500, windowSize))),
    });
    const source = new EventSource("/api/live/stream?" + params.toString());
    state.source = source;

    source.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (!payload.rows || !payload.rows.length) {
        return;
      }

      const seen = new Set(state.rows.map((row) => row.id));
      payload.rows.forEach((row) => {
        if (!seen.has(row.id)) {
          state.rows.push(row);
        }
      });

      const maxBuffer = Math.max(windowSize * 5, 5000);
      if (state.rows.length > maxBuffer) {
        state.rows = state.rows.slice(state.rows.length - maxBuffer);
      }

      renderChart(false);
      status("Streaming " + payload.rowCount + " new row(s).", false);
    };

    source.onerror = () => {
      status("Stream interrupted. Reconnecting...", true);
    };
  }

  async function loadData(forceWindow) {
    closeStream();
    const config = currentConfig();
    writeQuery(config);
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
    });
    if (config.mode === "review" && config.id) {
      params.set("id", config.id);
    }

    status("Loading chart...", false);
    try {
      const response = await fetch("/api/live/bootstrap?" + params.toString());
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Bootstrap request failed.");
      }

      state.rows = payload.rows || [];
      renderChart(forceWindow);
      status("Loaded " + payload.rowCount + " row(s).", false);
      if (config.run === "run") {
        connectStream(payload.lastId || 0, config.window);
      }
    } catch (error) {
      status(error.message || "Live bootstrap failed.", true);
    }
  }

  function bindSegment(container, handler) {
    container.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => handler(button.dataset.value));
    });
  }

  bindSegment(elements.modeToggle, (value) => {
    state.currentMode = value;
    setSegment(elements.modeToggle, value);
  });

  bindSegment(elements.runToggle, (value) => {
    state.currentRun = value;
    setSegment(elements.runToggle, value);
    if (value === "stop") {
      closeStream();
      status("Streaming stopped.", false);
    }
  });

  elements.applyButton.addEventListener("click", () => loadData(true));

  const initial = parseQuery();
  syncControls(initial);
  loadData(true);
}());
