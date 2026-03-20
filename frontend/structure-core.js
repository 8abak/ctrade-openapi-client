(function () {
  const state = {
    days: [],
    lastPayload: null,
    lastIncludeTicks: false,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function setStatus(msg) {
    const el = $("structure-status");
    if (el) el.textContent = msg || "";
  }

  function selectedMode() {
    return (($("structure-mode") && $("structure-mode").value) || "day").trim() || "day";
  }

  function selectedDayId() {
    const raw = $("structure-day") ? Number($("structure-day").value) : null;
    return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : null;
  }

  function includeTicks() {
    return !!($("toggle-ticks") && $("toggle-ticks").checked);
  }

  function applyVisibility() {
    ChartCore.setVisibility("mid", includeTicks());
    ChartCore.setVisibility("kal", !!($("toggle-kal") && $("toggle-kal").checked));
    ChartCore.setVisibility("piv", !!($("toggle-pivots") && $("toggle-pivots").checked));
    ChartCore.setVisibility("tpiv", !!($("toggle-tpivots") && $("toggle-tpivots").checked));
    ChartCore.setVisibility("tzone", !!($("toggle-zones") && $("toggle-zones").checked));
    ChartCore.setVisibility("tepisode", !!($("toggle-episodes") && $("toggle-episodes").checked));
  }

  function setDetail(obj) {
    const el = $("detail-body");
    if (!el) return;
    el.textContent = obj ? JSON.stringify(obj, null, 2) : "Click a structure object to inspect it.";
  }

  async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}\n${txt}`);
    }
    return res.json();
  }

  function fillDayOptions(days) {
    const sel = $("structure-day");
    if (!sel) return;
    const current = sel.value;
    sel.innerHTML = "";
    for (const day of days) {
      const opt = document.createElement("option");
      opt.value = String(day.id);
      opt.textContent = `${day.daydate || String(day.startts || "").slice(0, 10)}  (#${day.id})`;
      sel.appendChild(opt);
    }
    if (current && Array.from(sel.options).some((o) => o.value === current)) {
      sel.value = current;
    } else if (sel.options.length) {
      sel.selectedIndex = 0;
    }
  }

  async function loadDays() {
    const data = await fetchJSON("/api/structure/days?limit=90");
    state.days = Array.isArray(data.days) ? data.days : [];
    fillDayOptions(state.days);
    return state.days;
  }

  function syncModeUi() {
    const mode = selectedMode();
    const daySel = $("structure-day");
    const dayBtn = $("structure-refresh-days");
    const fromId = $("structure-from-id");
    const win = $("structure-window");

    const dayMode = mode === "day";
    if (daySel) daySel.disabled = !dayMode;
    if (dayBtn) dayBtn.disabled = !dayMode;
    if (fromId) fromId.disabled = dayMode;
    if (win) win.disabled = dayMode;
  }

  function formatSummary(data) {
    const ticksLoaded = Array.isArray(data.ticks) ? data.ticks.length : 0;
    const pivots = Array.isArray(data.pivots) ? data.pivots.length : 0;
    const tpivots = Array.isArray(data.tpivots) ? data.tpivots.length : 0;
    const zones = Array.isArray(data.tzone) ? data.tzone.length : 0;
    const episodes = Array.isArray(data.tepisode) ? data.tepisode.length : 0;

    if (data.mode === "day" && data.day) {
      return `day #${data.day.id} ${data.day.daydate || ""} | ticks=${ticksLoaded} pivots=${pivots} tpivots=${tpivots} zones=${zones} episodes=${episodes}`;
    }

    const range = data.range || {};
    return `window id ${range.startid || "?"}-${range.endid || "?"} | ticks=${ticksLoaded} pivots=${pivots} tpivots=${tpivots} zones=${zones} episodes=${episodes}`;
  }

  async function loadNow() {
    const mode = selectedMode();
    const wantsTicks = includeTicks();

    setStatus("Loading...");
    setDetail(null);

    let data;
    if (mode === "day") {
      const dayId = selectedDayId();
      if (!dayId) {
        setStatus("No day selected.");
        return;
      }
      data = await fetchJSON(`/api/structure/day?day_id=${encodeURIComponent(dayId)}&include_ticks=${wantsTicks ? "true" : "false"}`);
    } else {
      const fromIdRaw = $("structure-from-id") ? Number($("structure-from-id").value) : null;
      const windowRaw = $("structure-window") ? Number($("structure-window").value) : 20000;
      const fromId = Number.isFinite(fromIdRaw) && fromIdRaw > 0 ? Math.floor(fromIdRaw) : 1;
      const windowSize = Number.isFinite(windowRaw) && windowRaw >= 100 ? Math.floor(windowRaw) : 20000;
      data = await fetchJSON(
        `/api/structure/window?from_id=${encodeURIComponent(fromId)}&window=${encodeURIComponent(windowSize)}&include_ticks=${wantsTicks ? "true" : "false"}`
      );
    }

    state.lastPayload = data;
    state.lastIncludeTicks = wantsTicks;
    ChartCore.setStructureData(data);
    applyVisibility();
    setStatus(formatSummary(data));
  }

  function wireUi() {
    const mode = $("structure-mode");
    const daySel = $("structure-day");
    const refreshDaysBtn = $("structure-refresh-days");
    const loadBtn = $("structure-load");
    const resetZoomBtn = $("structure-reset-zoom");

    if (mode) {
      mode.addEventListener("change", () => {
        syncModeUi();
      });
    }

    if (refreshDaysBtn) {
      refreshDaysBtn.addEventListener("click", async () => {
        try {
          setStatus("Refreshing days...");
          await loadDays();
          setStatus("Days refreshed.");
        } catch (err) {
          console.error("structure-core days refresh failed", err);
          setStatus(`Days refresh failed: ${err.message || err}`);
        }
      });
    }

    if (daySel) {
      daySel.addEventListener("change", () => {
        if (selectedMode() === "day") loadNow().catch(onError);
      });
    }

    if (loadBtn) {
      loadBtn.addEventListener("click", () => {
        loadNow().catch(onError);
      });
    }

    if (resetZoomBtn) {
      resetZoomBtn.addEventListener("click", () => {
        ChartCore.resetZoom();
      });
    }

    ["toggle-pivots", "toggle-tpivots", "toggle-zones", "toggle-episodes", "toggle-kal"].forEach((id) => {
      const el = $(id);
      if (el) el.addEventListener("change", applyVisibility);
    });

    const tickToggle = $("toggle-ticks");
    if (tickToggle) {
      tickToggle.addEventListener("change", () => {
        if (includeTicks() !== state.lastIncludeTicks) {
          loadNow().catch(onError);
          return;
        }
        applyVisibility();
      });
    }
  }

  function onError(err) {
    console.error("structure-core load failed", err);
    setStatus(`Load failed: ${err.message || err}`);
  }

  document.addEventListener("DOMContentLoaded", async () => {
    ChartCore.init("structure-chart");
    ChartCore.setClickHandler((info) => {
      setDetail({
        seriesId: info.seriesId,
        seriesName: info.seriesName,
        payload: info.payload,
      });
    });

    wireUi();
    syncModeUi();
    applyVisibility();

    try {
      await loadDays();
      if (selectedMode() === "day" && !selectedDayId() && state.days.length) {
        $("structure-day").value = String(state.days[0].id);
      }
      await loadNow();
    } catch (err) {
      onError(err);
    }
  });
})();
