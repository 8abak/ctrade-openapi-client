// frontend/index-core.js
// Unified index controller: ticks (mid/bid/ask/kal) + eval dots.

(function () {
  const MAX_EVAL_ROWS = 200000;
  const EVAL_DEBOUNCE_MS = 200;

  let modeSelect;
  let runStopBtn;
  let runStopLabel;
  let reviewFromIdInput;
  let reviewWindowInput;
  let reviewJumpBtn;
  let statusLine;
  let evalMinLevelSelect;
  let evalVisibleToggle;
  let layerCheckboxes;

  let isLiveRunning = false;
  let lastWindowInfo = null; // {count, firstId, lastId}
  let lastEvalInfo = null; // {minLevel, count, truncated, maxRows}

  let evalFetchTimer = null;

  function $(id) {
    return document.getElementById(id);
  }

  function setStatus(msg) {
    if (statusLine) statusLine.textContent = msg || "";
  }

  function setRunButton(isRunning) {
    isLiveRunning = !!isRunning;
    if (!runStopBtn || !runStopLabel) return;

    if (isLiveRunning) {
      runStopBtn.classList.remove("btn-run");
      runStopBtn.classList.add("btn-stop");
      runStopLabel.textContent = "Stop";
    } else {
      runStopBtn.classList.remove("btn-stop");
      runStopBtn.classList.add("btn-run");
      runStopLabel.textContent = "Run";
    }
  }

  function formatWindowPart(info) {
    if (!info || !info.count) return "Ticks: 0";
    return `Ticks: ${info.count} [id ${info.firstId}–${info.lastId}]`;
  }

  function formatEvalPart(ei) {
    if (!ei) return "";
    const trunc = ei.truncated ? " (truncated: yes)" : "";
    return ` | Evals (>= L${ei.minLevel}): ${ei.count}${trunc}`;
  }

  function updateStatusLine() {
    const base = formatWindowPart(lastWindowInfo);
    const evals = formatEvalPart(lastEvalInfo);
    setStatus(base + evals);
  }

  function getMinLevel() {
    const v = evalMinLevelSelect ? Number(evalMinLevelSelect.value) : 1;
    return Number.isFinite(v) ? v : 1;
  }

  function scheduleEvalFetch(info) {
    lastWindowInfo = info;
    updateStatusLine();

    if (evalFetchTimer) clearTimeout(evalFetchTimer);
    evalFetchTimer = setTimeout(() => {
      fetchAndAttachEvals(info);
    }, EVAL_DEBOUNCE_MS);
  }

  async function fetchAndAttachEvals(info) {
    const minLevel = getMinLevel();

    if (!info || !info.count || !info.firstId || !info.lastId) {
      ChartCore.setEvals([], minLevel);
      lastEvalInfo = { minLevel, count: 0, truncated: false, maxRows: MAX_EVAL_ROWS };
      updateStatusLine();
      return;
    }

    const tickFrom = Number(info.firstId);
    const tickTo = Number(info.lastId);
    if (!Number.isFinite(tickFrom) || !Number.isFinite(tickTo)) {
      ChartCore.setEvals([], minLevel);
      lastEvalInfo = { minLevel, count: 0, truncated: false, maxRows: MAX_EVAL_ROWS };
      updateStatusLine();
      return;
    }

    try {
      const url =
        `/api/evals/window?tick_from=${tickFrom}` +
        `&tick_to=${tickTo}` +
        `&min_level=${minLevel}` +
        `&max_rows=${MAX_EVAL_ROWS}`;

      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      const rows = Array.isArray(data.evals) ? data.evals : [];
      ChartCore.setEvals(rows, minLevel);

      lastEvalInfo = {
        minLevel,
        count: rows.length,
        truncated: !!data.truncated,
        maxRows: data.max_rows != null ? data.max_rows : MAX_EVAL_ROWS,
      };
      updateStatusLine();
    } catch (err) {
      console.error("index-core: eval fetch failed", err);
      ChartCore.setEvals([], minLevel);
      lastEvalInfo = { minLevel, count: 0, truncated: false, maxRows: MAX_EVAL_ROWS };
      updateStatusLine();
    }
  }

  function wireToggles() {
    if (!layerCheckboxes) return;

    layerCheckboxes.forEach((cb) => {
      cb.addEventListener("change", () => {
        const group = cb.getAttribute("data-layer-group");
        const checked = !!cb.checked;
        // now only mid/bid/ask/kal exist
        ChartCore.setVisibility(group, checked);
      });
    });
  }

  function applyInitialToggleStatesToChart() {
    if (!layerCheckboxes) return;
    layerCheckboxes.forEach((cb) => {
      const group = cb.getAttribute("data-layer-group");
      ChartCore.setVisibility(group, !!cb.checked);
    });
  }

  function wireEvalControls() {
    if (evalVisibleToggle) {
      evalVisibleToggle.addEventListener("change", () => {
        ChartCore.setEvalVisibility(!!evalVisibleToggle.checked);
      });
    }

    if (evalMinLevelSelect) {
      evalMinLevelSelect.addEventListener("change", () => {
        if (lastWindowInfo) scheduleEvalFetch(lastWindowInfo);
      });
    }
  }

  function wireReviewJump() {
    if (!reviewJumpBtn) return;

    reviewJumpBtn.addEventListener("click", async () => {
      const fromIdRaw = reviewFromIdInput ? reviewFromIdInput.value : "";
      const winRaw = reviewWindowInput ? reviewWindowInput.value : "";
      const windowSize = Math.max(1, Number(winRaw) || 2000);

      let fromId = Number(fromIdRaw);

      try {
        if (!Number.isFinite(fromId) || fromId <= 0) {
          const res = await fetch(`/api/live_window?limit=${windowSize}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const ticks = data.ticks || [];
          const last = ticks.length ? ticks[ticks.length - 1].id : null;
          if (!last) throw new Error("no ticks returned");
          fromId = Number(last);
          if (reviewFromIdInput) reviewFromIdInput.value = String(fromId);
        }

        await ChartCore.loadWindow(fromId, windowSize);
      } catch (err) {
        console.error("index-core: review jump failed", err);
        setStatus("Review load failed: " + err.message);
      }
    });
  }

  function wireModeAndRun() {
    if (!modeSelect || !runStopBtn) return;

    modeSelect.addEventListener("change", () => {
      const mode = modeSelect.value;
      if (mode === "review") {
        ChartCore.stopLive();
        setRunButton(false);
      } else {
        setRunButton(false);
      }
    });

    runStopBtn.addEventListener("click", async () => {
      const mode = modeSelect.value;

      if (mode === "live") {
        if (!isLiveRunning) {
          ChartCore.startLive({ limit: 2000, intervalMs: 2000 });
          setRunButton(true);
        } else {
          ChartCore.stopLive();
          setRunButton(false);
        }
        return;
      }

      setRunButton(false);
      if (reviewJumpBtn) reviewJumpBtn.click();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    modeSelect = $("mode-select");
    runStopBtn = $("btn-run-stop");
    runStopLabel = runStopBtn ? runStopBtn.querySelector(".label") : null;
    reviewFromIdInput = $("review-from-id");
    reviewWindowInput = $("review-window");
    reviewJumpBtn = $("btn-review-jump");
    statusLine = $("status-line");
    evalMinLevelSelect = $("eval-min-level");
    evalVisibleToggle = $("eval-visible-toggle");
    layerCheckboxes = Array.from(document.querySelectorAll("[data-layer-group]"));

    ChartCore.init("segmeling-chart");

    ChartCore.setWindowChangeHandler((info) => {
      scheduleEvalFetch(info);
    });

    wireModeAndRun();
    wireReviewJump();
    wireToggles();
    wireEvalControls();

    // defaults
    if (reviewWindowInput) reviewWindowInput.value = 2000;
    if (modeSelect) modeSelect.value = "live";

    // ensure chart matches initial checkbox states immediately
    applyInitialToggleStatesToChart();

    // eval overlay toggle
    if (evalVisibleToggle) ChartCore.setEvalVisibility(!!evalVisibleToggle.checked);

    setRunButton(false);
    setStatus("Idle – select mode and press Run.");
  });
})();
