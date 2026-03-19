// frontend/index-core.js
// Unified index controller: ticks (mid/bid/ask/kal/k2/piv).

(function () {
  let modeSelect;
  let runStopBtn;
  let runStopLabel;
  let reviewFromIdInput;
  let reviewWindowInput;
  let reviewJumpBtn;
  let statusLine;
  let pivotVisibleToggle;
  let pivotLevelSelect;
  let layerCheckboxes;

  let isLiveRunning = false;

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
    return `Ticks: ${info.count} [id ${info.firstId}-${info.lastId}]`;
  }

  function getWindowSize(forLive) {
    const raw = reviewWindowInput ? Number(reviewWindowInput.value) : 2000;
    const fallback = Number.isFinite(raw) && raw > 0 ? raw : 2000;
    return forLive ? Math.max(500, fallback) : Math.max(1, fallback);
  }

  function wireToggles() {
    if (!layerCheckboxes) return;

    layerCheckboxes.forEach((cb) => {
      cb.addEventListener("change", () => {
        const group = cb.getAttribute("data-layer-group");
        ChartCore.setVisibility(group, !!cb.checked);
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

  function wirePivotControls() {
    if (pivotVisibleToggle) {
      pivotVisibleToggle.addEventListener("change", () => {
        ChartCore.setVisibility("piv", !!pivotVisibleToggle.checked);
      });
    }

    if (pivotLevelSelect) {
      pivotLevelSelect.addEventListener("change", () => {
        ChartCore.setPivotLevel(Number(pivotLevelSelect.value) || 1);
      });
    }
  }

  function wireReviewJump() {
    if (!reviewJumpBtn) return;

    reviewJumpBtn.addEventListener("click", async () => {
      const fromIdRaw = reviewFromIdInput ? reviewFromIdInput.value : "";
      const windowSize = getWindowSize(false);
      let fromId = Number(fromIdRaw);

      try {
        if (!Number.isFinite(fromId) || fromId <= 0) {
          const res = await fetch(`/api/live_window?limit=${Math.max(500, windowSize)}`);
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
      if (modeSelect.value === "review") {
        ChartCore.stopLive();
      }
      setRunButton(false);
    });

    runStopBtn.addEventListener("click", async () => {
      if (modeSelect.value === "live") {
        if (!isLiveRunning) {
          ChartCore.startLive({ limit: getWindowSize(true), intervalMs: 2000 });
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
    pivotVisibleToggle = $("piv-visible-toggle");
    pivotLevelSelect = $("piv-level-select");
    layerCheckboxes = Array.from(document.querySelectorAll("[data-layer-group]"));

    ChartCore.init("segmeling-chart");
    ChartCore.setWindowChangeHandler((info) => {
      setStatus(formatWindowPart(info));
    });

    wireModeAndRun();
    wireReviewJump();
    wireToggles();
    wirePivotControls();

    if (reviewWindowInput) {
      reviewWindowInput.addEventListener("change", () => {
        if (modeSelect && modeSelect.value === "live" && isLiveRunning) {
          ChartCore.startLive({ limit: getWindowSize(true), intervalMs: 2000 });
        }
      });
    }

    if (reviewWindowInput) reviewWindowInput.value = 2000;
    if (modeSelect) modeSelect.value = "live";

    applyInitialToggleStatesToChart();
    if (pivotLevelSelect) ChartCore.setPivotLevel(Number(pivotLevelSelect.value) || 1);
    if (pivotVisibleToggle) ChartCore.setVisibility("piv", !!pivotVisibleToggle.checked);

    setRunButton(false);
    setStatus("Idle - select mode and press Run.");
  });
})();
