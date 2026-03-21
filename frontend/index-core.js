// frontend/index-core.js
// Unified index controller: ticks (mid/bid/ask/kal/k2/piv).

(function () {
  const LIVE_INTERVAL_MS = 2000;
  const REVIEW_MIN_WINDOW = 100;
  const REVIEW_PREFETCH_MIN = 500;
  const REVIEW_BASE_FRAME_MS = 500;
  const REVIEW_MAX_FRAME_MS = 25;

  let modeSelect;
  let runStopBtn;
  let runStopLabel;
  let reviewFromIdInput;
  let reviewWindowInput;
  let reviewJumpBtn;
  let reviewPlayPauseBtn;
  let reviewStepBackBtn;
  let reviewStepForwardBtn;
  let reviewResetBtn;
  let reviewSpeedSelect;
  let reviewStepSizeSelect;
  let statusLine;
  let pivotVisibleToggle;
  let pivotLevelSelect;
  let layerCheckboxes;

  let isLiveRunning = false;
  let lastWindowInfo = null;

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

  function setPlayPauseButton(isPlaying) {
    if (!reviewPlayPauseBtn) return;
    reviewPlayPauseBtn.textContent = isPlaying ? "Pause" : "Play";
  }

  function formatWindowPart(info) {
    if (!info || !info.count) return "Ticks: 0";
    return `Ticks: ${info.count} [id ${info.firstId}-${info.lastId}]`;
  }

  function getWindowSize(forLive) {
    const raw = reviewWindowInput ? Number(reviewWindowInput.value) : 2000;
    const fallback = Number.isFinite(raw) && raw > 0 ? raw : 2000;
    return forLive ? Math.max(500, fallback) : Math.max(REVIEW_MIN_WINDOW, fallback);
  }

  function getTicksPerStep() {
    const raw = reviewStepSizeSelect ? Number(reviewStepSizeSelect.value) : 1;
    return Number.isFinite(raw) && raw > 0 ? raw : 1;
  }

  function getFrameMs() {
    const raw = reviewSpeedSelect ? reviewSpeedSelect.value : "1";
    if (raw === "max") return REVIEW_MAX_FRAME_MS;

    const speed = Number(raw);
    if (!Number.isFinite(speed) || speed <= 0) return REVIEW_BASE_FRAME_MS;
    return Math.max(REVIEW_MAX_FRAME_MS, Math.round(REVIEW_BASE_FRAME_MS / speed));
  }

  function getSelectedSpeedLabel() {
    if (!reviewSpeedSelect) return "1x";
    const selected = reviewSpeedSelect.options[reviewSpeedSelect.selectedIndex];
    return selected ? selected.textContent : "1x";
  }

  function updateStatusLine() {
    const reviewMode = modeSelect && modeSelect.value === "review";
    const windowPart = formatWindowPart(lastWindowInfo);

    if (reviewMode) {
      const prefix = reviewPlayback.getStatusPrefix();
      setStatus(prefix ? `${prefix} | ${windowPart}` : windowPart);
      return;
    }

    const livePrefix = isLiveRunning ? "Live running" : "Live idle";
    setStatus(`${livePrefix} | ${windowPart}`);
  }

  async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.json();
  }

  async function resolveReviewFromId() {
    let fromId = reviewFromIdInput ? Number(reviewFromIdInput.value) : NaN;
    if (Number.isFinite(fromId) && fromId > 0) return fromId;

    const data = await fetchJSON(`/api/live_window?limit=${Math.max(500, getWindowSize(false))}`);
    const ticks = Array.isArray(data.ticks) ? data.ticks : [];
    const last = ticks.length ? Number(ticks[ticks.length - 1].id) : NaN;
    if (!Number.isFinite(last) || last <= 0) {
      throw new Error("No ticks returned for review start.");
    }

    if (reviewFromIdInput) reviewFromIdInput.value = String(last);
    return last;
  }

  async function fetchReviewWindow(fromId, windowSize) {
    const url =
      `/api/review/window?from_id=${encodeURIComponent(fromId)}` +
      `&window=${encodeURIComponent(windowSize)}`;
    return fetchJSON(url);
  }

  async function fetchReviewChunkAfter(afterId, limit) {
    if (!Number.isFinite(afterId) || afterId <= 0) return { ticks: [], pivots: [] };
    const url =
      `/api/live_window?after_id=${encodeURIComponent(afterId)}` +
      `&limit=${encodeURIComponent(limit)}`;
    return fetchJSON(url);
  }

  async function fetchReviewChunkBefore(beforeId, limit) {
    if (!Number.isFinite(beforeId) || beforeId <= 0) return { ticks: [], pivots: [] };
    const url =
      `/api/live_window?before_id=${encodeURIComponent(beforeId)}` +
      `&limit=${encodeURIComponent(limit)}`;
    return fetchJSON(url);
  }

  function syncReviewControls() {
    const inReviewMode = !!(modeSelect && modeSelect.value === "review");
    const busy = reviewPlayback.busy;
    const loaded = reviewPlayback.loaded;

    if (reviewJumpBtn) reviewJumpBtn.disabled = !inReviewMode || busy;
    if (reviewPlayPauseBtn) reviewPlayPauseBtn.disabled = !inReviewMode || busy;
    if (reviewStepBackBtn) reviewStepBackBtn.disabled = !inReviewMode || busy || !loaded;
    if (reviewStepForwardBtn) reviewStepForwardBtn.disabled = !inReviewMode || busy || !loaded;
    if (reviewResetBtn) reviewResetBtn.disabled = !inReviewMode || busy;
    if (reviewSpeedSelect) reviewSpeedSelect.disabled = !inReviewMode;
    if (reviewStepSizeSelect) reviewStepSizeSelect.disabled = !inReviewMode;
    if (reviewFromIdInput) reviewFromIdInput.disabled = busy;
    if (reviewWindowInput) reviewWindowInput.disabled = busy;

    setPlayPauseButton(reviewPlayback.playing);
  }

  const reviewPlayback = {
    loaded: false,
    playing: false,
    busy: false,
    timer: null,
    requestedStartId: null,
    initialStartId: null,
    visibleCount: 0,
    visibleStartIndex: 0,
    bufferTicks: [],
    bufferPivots: [],
    pivotKeys: new Set(),

    getStatusPrefix() {
      if (!this.loaded) return "Review idle";
      const stateLabel = this.playing ? "Review playing" : "Review paused";
      const ticksPerStep = getTicksPerStep();
      return `${stateLabel} | ${getSelectedSpeedLabel()} | ${ticksPerStep} tick${ticksPerStep === 1 ? "" : "s"}/step`;
    },

    clearTimer() {
      if (!this.timer) return;
      clearTimeout(this.timer);
      this.timer = null;
    },

    pause() {
      this.playing = false;
      this.clearTimer();
      setPlayPauseButton(false);
      syncReviewControls();
      updateStatusLine();
    },

    clearLoadedState() {
      this.loaded = false;
      this.playing = false;
      this.visibleCount = 0;
      this.visibleStartIndex = 0;
      this.bufferTicks = [];
      this.bufferPivots = [];
      this.pivotKeys = new Set();
    },

    pivotKey(pivot) {
      if (pivot && pivot.id != null) return `id:${pivot.id}`;
      return [
        pivot && pivot.tickid,
        pivot && pivot.ts,
        pivot && pivot.level,
        pivot && pivot.ptype,
        pivot && pivot.px,
      ].join("|");
    },

    mergePivots(pivots) {
      const rows = Array.isArray(pivots) ? pivots : [];
      for (const pivot of rows) {
        const key = this.pivotKey(pivot);
        if (this.pivotKeys.has(key)) continue;
        this.pivotKeys.add(key);
        this.bufferPivots.push(pivot);
      }
    },

    appendTicks(ticks) {
      const rows = Array.isArray(ticks) ? ticks : [];
      if (!rows.length) return 0;

      const lastId = this.bufferTicks.length ? Number(this.bufferTicks[this.bufferTicks.length - 1].id) : null;
      const nextRows = lastId == null ? rows.slice() : rows.filter((row) => Number(row && row.id) > lastId);
      if (!nextRows.length) return 0;

      this.bufferTicks.push(...nextRows);
      return nextRows.length;
    },

    prependTicks(ticks) {
      const rows = Array.isArray(ticks) ? ticks : [];
      if (!rows.length) return 0;

      const firstId = this.bufferTicks.length ? Number(this.bufferTicks[0].id) : null;
      const nextRows = firstId == null ? rows.slice() : rows.filter((row) => Number(row && row.id) < firstId);
      if (!nextRows.length) return 0;

      this.bufferTicks = nextRows.concat(this.bufferTicks);
      this.visibleStartIndex += nextRows.length;
      return nextRows.length;
    },

    getVisibleTicks() {
      if (!this.loaded || !this.visibleCount) return [];
      return this.bufferTicks.slice(this.visibleStartIndex, this.visibleStartIndex + this.visibleCount);
    },

    getVisiblePivots(firstId, lastId) {
      if (!Number.isFinite(firstId) || !Number.isFinite(lastId)) return [];
      return this.bufferPivots.filter((pivot) => {
        const tickId = Number(pivot && pivot.tickid);
        return Number.isFinite(tickId) && tickId >= firstId && tickId <= lastId;
      });
    },

    renderVisible(resetZoom) {
      const visibleTicks = this.getVisibleTicks();
      const firstId = visibleTicks.length ? Number(visibleTicks[0].id) : null;
      const lastId = visibleTicks.length ? Number(visibleTicks[visibleTicks.length - 1].id) : null;
      const pivots = this.getVisiblePivots(firstId, lastId);

      ChartCore.setReviewData({ ticks: visibleTicks, pivots });
      if (resetZoom) ChartCore.resetZoom();
    },

    rebuildPivotIndex() {
      this.pivotKeys = new Set(this.bufferPivots.map((pivot) => this.pivotKey(pivot)));
    },

    pruneCache() {
      const maxCache = Math.max(this.visibleCount * 6, REVIEW_PREFETCH_MIN * 2);
      if (!this.bufferTicks.length || this.bufferTicks.length <= maxCache) return;

      const keepBefore = Math.max(this.visibleCount * 2, REVIEW_PREFETCH_MIN);
      const keepAfter = Math.max(this.visibleCount * 3, REVIEW_PREFETCH_MIN);
      const trimStart = Math.max(0, this.visibleStartIndex - keepBefore);
      const trimEnd = Math.min(
        this.bufferTicks.length,
        this.visibleStartIndex + this.visibleCount + keepAfter
      );

      if (trimStart === 0 && trimEnd === this.bufferTicks.length) return;

      const keptTicks = this.bufferTicks.slice(trimStart, trimEnd);
      this.bufferTicks = keptTicks;
      this.visibleStartIndex -= trimStart;

      const firstKeptId = keptTicks.length ? Number(keptTicks[0].id) : null;
      const lastKeptId = keptTicks.length ? Number(keptTicks[keptTicks.length - 1].id) : null;
      if (!Number.isFinite(firstKeptId) || !Number.isFinite(lastKeptId)) {
        this.bufferPivots = [];
        this.rebuildPivotIndex();
        return;
      }

      this.bufferPivots = this.bufferPivots.filter((pivot) => {
        const tickId = Number(pivot && pivot.tickid);
        return Number.isFinite(tickId) && tickId >= firstKeptId && tickId <= lastKeptId;
      });
      this.rebuildPivotIndex();
    },

    async ensureForwardBuffer(stepCount) {
      const desired = this.visibleStartIndex + this.visibleCount + stepCount;
      while (this.bufferTicks.length < desired) {
        const lastTick = this.bufferTicks[this.bufferTicks.length - 1];
        const lastId = lastTick ? Number(lastTick.id) : NaN;
        if (!Number.isFinite(lastId) || lastId <= 0) break;

        const chunk = await fetchReviewChunkAfter(
          lastId + 1,
          Math.max(REVIEW_PREFETCH_MIN, getWindowSize(false), stepCount, getTicksPerStep())
        );
        const added = this.appendTicks(chunk.ticks);
        this.mergePivots(chunk.pivots);
        if (!added) break;
      }

      return Math.max(0, this.bufferTicks.length - (this.visibleStartIndex + this.visibleCount));
    },

    async ensureBackwardBuffer(stepCount) {
      while (this.visibleStartIndex < stepCount) {
        const firstTick = this.bufferTicks[0];
        const firstId = firstTick ? Number(firstTick.id) : NaN;
        if (!Number.isFinite(firstId) || firstId <= 1) break;

        const chunk = await fetchReviewChunkBefore(
          firstId - 1,
          Math.max(REVIEW_PREFETCH_MIN, getWindowSize(false), stepCount, getTicksPerStep())
        );
        const added = this.prependTicks(chunk.ticks);
        this.mergePivots(chunk.pivots);
        if (!added) break;
      }

      return this.visibleStartIndex;
    },

    async moveForward(stepCount) {
      const available = await this.ensureForwardBuffer(stepCount);
      const moveBy = Math.min(stepCount, available);
      if (moveBy <= 0) return 0;

      this.visibleStartIndex += moveBy;
      this.pruneCache();
      return moveBy;
    },

    async moveBackward(stepCount) {
      await this.ensureBackwardBuffer(stepCount);
      const moveBy = Math.min(stepCount, this.visibleStartIndex);
      if (moveBy <= 0) return 0;

      this.visibleStartIndex -= moveBy;
      this.pruneCache();
      return moveBy;
    },

    async shiftBy(delta) {
      if (this.busy) return 0;
      if (!this.loaded) return 0;

      this.busy = true;
      syncReviewControls();

      try {
        const moved = delta >= 0
          ? await this.moveForward(delta)
          : await this.moveBackward(Math.abs(delta));

        if (moved > 0) this.renderVisible(false);
        updateStatusLine();
        return moved;
      } finally {
        this.busy = false;
        syncReviewControls();
      }
    },

    scheduleNext() {
      this.clearTimer();
      if (!this.playing) return;

      this.timer = setTimeout(async () => {
        try {
          const moved = await this.shiftBy(getTicksPerStep());
          if (moved <= 0) {
            this.pause();
            return;
          }
          this.scheduleNext();
        } catch (err) {
          console.error("index-core: review playback step failed", err);
          this.pause();
          setStatus("Review playback failed: " + err.message);
        }
      }, getFrameMs());
    },

    play() {
      if (!this.loaded) return;
      this.playing = true;
      setPlayPauseButton(true);
      syncReviewControls();
      updateStatusLine();
      this.scheduleNext();
    },

    async loadWindow(fromId, autoPlay) {
      this.pause();
      this.busy = true;
      syncReviewControls();

      try {
        const payload = await fetchReviewWindow(fromId, getWindowSize(false));
        const ticks = Array.isArray(payload.ticks) ? payload.ticks : [];

        this.clearLoadedState();
        this.requestedStartId = fromId;
        this.initialStartId = fromId;
        this.mergePivots(payload.pivots);

        if (!ticks.length) {
          ChartCore.setReviewData({ ticks: [], pivots: [] });
          updateStatusLine();
          return;
        }

        this.loaded = true;
        this.bufferTicks = ticks.slice();
        this.visibleCount = ticks.length;
        this.visibleStartIndex = 0;
        this.renderVisible(true);

        if (autoPlay) this.play();
        else updateStatusLine();
      } finally {
        this.busy = false;
        syncReviewControls();
      }
    },

    async runFromInputs() {
      const fromId = await resolveReviewFromId();
      await this.loadWindow(fromId, true);
    },

    async jumpFromInputs() {
      const fromId = await resolveReviewFromId();
      await this.loadWindow(fromId, false);
    },

    async playOrPause() {
      if (this.playing) {
        this.pause();
        return;
      }

      if (!this.loaded) {
        await this.runFromInputs();
        return;
      }

      this.play();
    },

    async resetFromInputs() {
      const fromId = await resolveReviewFromId();
      await this.loadWindow(fromId, false);
    },

    refreshCurrentWindow() {
      if (!this.loaded) return;
      this.renderVisible(false);
      updateStatusLine();
    },

    rescheduleIfPlaying() {
      if (this.playing) this.scheduleNext();
      updateStatusLine();
    },
  };

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

  function wireReviewControls() {
    if (reviewJumpBtn) {
      reviewJumpBtn.addEventListener("click", async () => {
        try {
          await reviewPlayback.jumpFromInputs();
        } catch (err) {
          console.error("index-core: review jump failed", err);
          reviewPlayback.pause();
          setStatus("Review load failed: " + err.message);
        }
      });
    }

    if (reviewPlayPauseBtn) {
      reviewPlayPauseBtn.addEventListener("click", async () => {
        try {
          await reviewPlayback.playOrPause();
        } catch (err) {
          console.error("index-core: review play/pause failed", err);
          reviewPlayback.pause();
          setStatus("Review playback failed: " + err.message);
        }
      });
    }

    if (reviewStepForwardBtn) {
      reviewStepForwardBtn.addEventListener("click", async () => {
        try {
          reviewPlayback.pause();
          await reviewPlayback.shiftBy(getTicksPerStep());
        } catch (err) {
          console.error("index-core: review step forward failed", err);
          setStatus("Review step failed: " + err.message);
        }
      });
    }

    if (reviewStepBackBtn) {
      reviewStepBackBtn.addEventListener("click", async () => {
        try {
          reviewPlayback.pause();
          await reviewPlayback.shiftBy(-getTicksPerStep());
        } catch (err) {
          console.error("index-core: review step backward failed", err);
          setStatus("Review step failed: " + err.message);
        }
      });
    }

    if (reviewResetBtn) {
      reviewResetBtn.addEventListener("click", async () => {
        try {
          await reviewPlayback.resetFromInputs();
        } catch (err) {
          console.error("index-core: review reset failed", err);
          reviewPlayback.pause();
          setStatus("Review reset failed: " + err.message);
        }
      });
    }

    if (reviewSpeedSelect) {
      reviewSpeedSelect.addEventListener("change", () => {
        reviewPlayback.rescheduleIfPlaying();
      });
    }

    if (reviewStepSizeSelect) {
      reviewStepSizeSelect.addEventListener("change", () => {
        updateStatusLine();
      });
    }
  }

  function wireModeAndRun() {
    if (!modeSelect || !runStopBtn) return;

    modeSelect.addEventListener("change", () => {
      if (modeSelect.value === "review") {
        ChartCore.stopLive();
        setRunButton(false);
        if (reviewPlayback.loaded) {
          reviewPlayback.refreshCurrentWindow();
        } else {
          lastWindowInfo = { count: 0, firstId: null, lastId: null };
        }
      } else {
        reviewPlayback.pause();
      }

      syncReviewControls();
      updateStatusLine();
    });

    runStopBtn.addEventListener("click", async () => {
      if (modeSelect.value === "live") {
        if (!isLiveRunning) {
          reviewPlayback.pause();
          ChartCore.startLive({ limit: getWindowSize(true), intervalMs: LIVE_INTERVAL_MS });
          setRunButton(true);
        } else {
          ChartCore.stopLive();
          setRunButton(false);
        }

        updateStatusLine();
        return;
      }

      try {
        await reviewPlayback.runFromInputs();
      } catch (err) {
        console.error("index-core: review run failed", err);
        reviewPlayback.pause();
        setStatus("Review run failed: " + err.message);
      }
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    modeSelect = $("mode-select");
    runStopBtn = $("btn-run-stop");
    runStopLabel = runStopBtn ? runStopBtn.querySelector(".label") : null;
    reviewFromIdInput = $("review-from-id");
    reviewWindowInput = $("review-window");
    reviewJumpBtn = $("btn-review-jump");
    reviewPlayPauseBtn = $("btn-review-play-pause");
    reviewStepBackBtn = $("btn-review-step-back");
    reviewStepForwardBtn = $("btn-review-step-forward");
    reviewResetBtn = $("btn-review-reset");
    reviewSpeedSelect = $("review-speed");
    reviewStepSizeSelect = $("review-step-size");
    statusLine = $("status-line");
    pivotVisibleToggle = $("piv-visible-toggle");
    pivotLevelSelect = $("piv-level-select");
    layerCheckboxes = Array.from(document.querySelectorAll("[data-layer-group]"));

    ChartCore.init("segmeling-chart");
    ChartCore.setWindowChangeHandler((info) => {
      lastWindowInfo = info;
      updateStatusLine();
    });

    wireModeAndRun();
    wireReviewControls();
    wireToggles();
    wirePivotControls();

    if (reviewWindowInput) {
      reviewWindowInput.addEventListener("change", () => {
        if (modeSelect && modeSelect.value === "live" && isLiveRunning) {
          ChartCore.startLive({ limit: getWindowSize(true), intervalMs: LIVE_INTERVAL_MS });
        }
      });
    }

    if (reviewWindowInput) reviewWindowInput.value = 2000;
    if (reviewSpeedSelect) reviewSpeedSelect.value = "1";
    if (reviewStepSizeSelect) reviewStepSizeSelect.value = "1";
    if (modeSelect) modeSelect.value = "live";

    applyInitialToggleStatesToChart();
    if (pivotLevelSelect) ChartCore.setPivotLevel(Number(pivotLevelSelect.value) || 1);
    if (pivotVisibleToggle) ChartCore.setVisibility("piv", !!pivotVisibleToggle.checked);

    setRunButton(false);
    syncReviewControls();
    lastWindowInfo = { count: 0, firstId: null, lastId: null };
    updateStatusLine();
  });
})();
