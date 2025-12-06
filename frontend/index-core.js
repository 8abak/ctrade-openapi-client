// PATH: frontend/index-core.js
// Unified controller for segmeling index.html – Live/Review + Run/Stop

(function () {
  if (!window.ChartCore) {
    console.error("index-core: ChartCore not found");
    return;
  }

  const state = {
    dataMode: "live",           // "live" | "review"
    runState: "run",            // "run" | "stop"

    // live settings
    liveLimit: 400,
    liveIntervalMs: 1500,

    // review settings
    reviewWindow: 400,
    reviewStepTicks: 1,
    reviewStepMs: 1500,
    reviewTimer: null,
    reviewFromId: null,         // first tick id of current window in review mode

    // last known window meta from ChartCore
    lastCount: 0,
    lastFirstId: null,
    lastLastId: null,
  };

  const dom = {
    chartEl: null,
    modeSelect: null,
    runStopBtn: null,
    statusLine: null,
    layerCheckboxes: [],
  };

  function $(sel) {
    return document.querySelector(sel);
  }

  function initDom() {
    dom.chartEl = $("#segmeling-chart");
    dom.modeSelect = $("#mode-select");
    dom.runStopBtn = $("#btn-run-stop");
    dom.statusLine = $("#status-line");
    dom.layerCheckboxes = Array.from(
      document.querySelectorAll("input[data-layer-group]")
    );
  }

  function stopReviewPlayback() {
    if (state.reviewTimer) {
      clearInterval(state.reviewTimer);
      state.reviewTimer = null;
    }
  }

  function stopAllTimers() {
    try {
      ChartCore.stopLive();
    } catch (err) {
      console.error("index-core: stopLive error", err);
    }
    stopReviewPlayback();
  }

  function startReviewPlayback() {
    stopReviewPlayback();
    state.reviewTimer = setInterval(stepReviewForward, state.reviewStepMs);
  }

  function stepReviewForward() {
    if (state.dataMode !== "review" || state.runState !== "run") {
      stopReviewPlayback();
      return;
    }

    const baseFrom =
      state.reviewFromId != null
        ? state.reviewFromId
        : state.lastFirstId != null
        ? state.lastFirstId
        : 1;

    const nextFrom = baseFrom + state.reviewStepTicks;

    ChartCore.loadWindow(nextFrom, state.reviewWindow).catch((err) => {
      console.error("index-core: review step load error", err);
      // keep timer running; transient HTTP errors are acceptable
    });
  }

  function setDataMode(mode) {
    if (mode !== "live" && mode !== "review") {
      console.warn("index-core: invalid dataMode", mode);
      return;
    }
    if (state.dataMode === mode) return;

    stopAllTimers();
    state.dataMode = mode;

    // reflect in UI
    if (dom.modeSelect && dom.modeSelect.value !== mode) {
      dom.modeSelect.value = mode;
    }

    applyRunState();
    updateStatusLine();
  }

  function setRunState(runState) {
    if (runState !== "run" && runState !== "stop") {
      console.warn("index-core: invalid runState", runState);
      return;
    }
    if (state.runState === runState) return;

    state.runState = runState;
    applyRunState();
    updateRunStopButton();
    updateStatusLine();
  }

  function applyRunState() {
    stopAllTimers();

    if (state.dataMode === "live") {
      if (state.runState === "run") {
        // RUN + LIVE: follow the stream
        ChartCore.startLive({
          limit: state.liveLimit,
          intervalMs: state.liveIntervalMs,
        });
      } else {
        // STOP + LIVE: single snapshot, no polling
        ChartCore.loadLiveOnce({
          limit: state.liveLimit,
        }).catch((err) => console.error("index-core: loadLiveOnce error", err));
      }
      return;
    }

    // REVIEW mode
    let fromId = state.reviewFromId;
    if (fromId == null && state.lastLastId != null) {
      fromId = Math.max(1, state.lastLastId - state.reviewWindow + 1);
    }
    if (fromId == null) {
      fromId = 1;
    }

    ChartCore.loadWindow(fromId, state.reviewWindow).catch((err) =>
      console.error("index-core: initial review load error", err)
    );

    if (state.runState === "run") {
      startReviewPlayback();
    } else {
      stopReviewPlayback();
    }
  }

  function toggleLayer(group, on) {
    // mapping from UI names to ChartCore groups
    if (group === "pivots") {
      ChartCore.setVisibility("hipiv", on);
      ChartCore.setVisibility("lopiv", on);
      return;
    }
    if (group === "swings") {
      ChartCore.setVisibility("swings", on);
      return;
    }
    // direct mapping
    ChartCore.setVisibility(group, on);
  }

  function updateRunStopButton() {
    if (!dom.runStopBtn) return;
    const labelEl = dom.runStopBtn.querySelector(".label");
    if (!labelEl) return;

    if (state.runState === "run") {
      dom.runStopBtn.classList.remove("btn-stop");
      dom.runStopBtn.classList.add("btn-run");
      labelEl.textContent = "Run";
    } else {
      dom.runStopBtn.classList.remove("btn-run");
      dom.runStopBtn.classList.add("btn-stop");
      labelEl.textContent = "Stop";
    }
  }

  function updateStatusLine() {
    if (!dom.statusLine) return;

    const count = state.lastCount;
    const firstId = state.lastFirstId;
    const lastId = state.lastLastId;

    const modeText = state.dataMode.toUpperCase();
    const runText = state.runState.toUpperCase();

    if (!count || firstId == null || lastId == null) {
      dom.statusLine.textContent = `No data loaded yet (${modeText}, ${runText})`;
      return;
    }

    dom.statusLine.textContent = `${count} ticks from ${firstId} to ${lastId} (${modeText}, ${runText})`;
  }

  function wireEvents() {
    if (dom.modeSelect) {
      dom.modeSelect.addEventListener("change", (e) => {
        setDataMode(e.target.value);
      });
    }

    if (dom.runStopBtn) {
      dom.runStopBtn.addEventListener("click", () => {
        const next = state.runState === "run" ? "stop" : "run";
        setRunState(next);
      });
    }

    dom.layerCheckboxes.forEach((cb) => {
      cb.addEventListener("change", (e) => {
        const group = e.target.getAttribute("data-layer-group");
        const on = !!e.target.checked;
        if (!group) return;
        toggleLayer(group, on);
      });
    });
  }

  function initChartCore() {
    ChartCore.init("segmeling-chart");

    ChartCore.setWindowChangeHandler((meta) => {
      if (!meta) return;
      state.lastCount = meta.count || 0;
      state.lastFirstId = meta.firstId;
      state.lastLastId = meta.lastId;
      if (state.dataMode === "review" && meta.firstId != null) {
        state.reviewFromId = meta.firstId;
      }
      updateStatusLine();
    });
  }

  function init() {
    initDom();
    if (!dom.chartEl) {
      console.error("index-core: chart container not found");
      return;
    }

    initChartCore();
    wireEvents();
    updateRunStopButton();

    // ensure UI reflects initial state
    if (dom.modeSelect && dom.modeSelect.value !== state.dataMode) {
      dom.modeSelect.value = state.dataMode;
    }

    // kick off data based on initial state (LIVE + RUN)
    applyRunState();
    updateStatusLine();
  }

  document.addEventListener("DOMContentLoaded", init);

  // Expose for debugging/manual control
  window.SegmelingApp = {
    getState() {
      return { ...state };
    },
    setDataMode,
    setRunState,
    toggleLayer,
  };
})();
