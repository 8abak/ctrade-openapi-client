(function () {
  const state = {
    days: [],
    rawPayload: null,
    viewPayload: null,
    derived: null,
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

  function toNum(value) {
    if (value === null || value === undefined || value === "") return null;
    const num = typeof value === "number" ? value : Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function toBool(value) {
    if (typeof value === "boolean") return value;
    if (value === null || value === undefined) return false;
    return ["1", "t", "true", "y", "yes"].includes(String(value).trim().toLowerCase());
  }

  function formatNum(value, digits) {
    const num = toNum(value);
    return num == null ? "" : num.toFixed(digits);
  }

  function formatValue(value) {
    if (value === null || value === undefined || value === "") return "";
    if (typeof value === "boolean") return value ? "true" : "false";
    return String(value);
  }

  function applyVisibility() {
    ChartCore.setVisibility("mid", includeTicks());
    ChartCore.setVisibility("kal", !!($("toggle-kal") && $("toggle-kal").checked));
    ChartCore.setVisibility("piv", !!($("toggle-pivots") && $("toggle-pivots").checked));
    ChartCore.setVisibility("tpiv", false);
    ChartCore.setVisibility("tzone", !!($("toggle-zones") && $("toggle-zones").checked));
    ChartCore.setVisibility("tepisode", !!($("toggle-episodes") && $("toggle-episodes").checked));
    ChartCore.setVisibility("tconfirm", !!($("toggle-confirms") && $("toggle-confirms").checked));
    ChartCore.setVisibility("tscore", !!($("toggle-scores") && $("toggle-scores").checked));
  }

  function defaultDetailText() {
    return "Click a score, confirm, zone, or episode to inspect it.";
  }

  function setDetailText(text) {
    const el = $("detail-body");
    if (!el) return;
    el.textContent = text || defaultDetailText();
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

  function pickStrictB(ruleRows) {
    if (!Array.isArray(ruleRows) || !ruleRows.length) return null;
    for (const row of ruleRows) {
      if (String((row && row.rulename) || "").trim().toLowerCase() === "strictb") return row;
    }
    return ruleRows[0] || null;
  }

  function decorateConfirmRow(confirmRow, bestScore, strictB) {
    return {
      ...confirmRow,
      tconfirmid: confirmRow.tconfirmid != null ? confirmRow.tconfirmid : confirmRow.id,
      tepisodeid:
        confirmRow.tepisodeid != null
          ? confirmRow.tepisodeid
          : confirmRow.episodeid != null
            ? confirmRow.episodeid
            : null,
      structurescore: bestScore ? bestScore.structurescore : null,
      contextscore: bestScore ? bestScore.contextscore : null,
      truthscore: bestScore ? bestScore.truthscore : null,
      penaltyscore: bestScore ? bestScore.penaltyscore : null,
      totalscore: bestScore ? bestScore.totalscore : null,
      scoregrade: bestScore ? bestScore.scoregrade : null,
      reason: bestScore ? bestScore.reason : null,
      strictb_ishit: strictB ? strictB.ishit : null,
      strictb_reason: strictB ? strictB.reason : null,
      strictb_score: strictB ? strictB.score : null,
    };
  }

  function decorateScoreRow(scoreRow, confirmRow, strictB) {
    return {
      ...scoreRow,
      tconfirmid:
        scoreRow.tconfirmid != null
          ? scoreRow.tconfirmid
          : scoreRow.confirmid != null
            ? scoreRow.confirmid
            : confirmRow && confirmRow.id != null
              ? confirmRow.id
              : null,
      tepisodeid:
        scoreRow.tepisodeid != null
          ? scoreRow.tepisodeid
          : scoreRow.episodeid != null
            ? scoreRow.episodeid
            : confirmRow && confirmRow.tepisodeid != null
              ? confirmRow.tepisodeid
              : confirmRow && confirmRow.episodeid != null
                ? confirmRow.episodeid
                : null,
      dayid: scoreRow.dayid != null ? scoreRow.dayid : confirmRow ? confirmRow.dayid : null,
      dir: scoreRow.dir != null ? scoreRow.dir : confirmRow ? confirmRow.dir : null,
      anchorts: confirmRow ? confirmRow.anchorts : null,
      anchorprice: confirmRow ? confirmRow.anchorprice : null,
      anchorpivotid: confirmRow ? confirmRow.anchorpivotid : null,
      confirmstate: confirmRow ? confirmRow.confirmstate : null,
      microreversalts: confirmRow ? confirmRow.microreversalts : null,
      microreversalticks: confirmRow ? confirmRow.microreversalticks : null,
      lowerhights: confirmRow ? confirmRow.lowerhights : null,
      lowerhighticks: confirmRow ? confirmRow.lowerhighticks : null,
      lowerlowts: confirmRow ? confirmRow.lowerlowts : null,
      lowerlowticks: confirmRow ? confirmRow.lowerlowticks : null,
      breakhights: confirmRow ? confirmRow.breakhights : null,
      breakhighticks: confirmRow ? confirmRow.breakhighticks : null,
      invalidated: confirmRow ? confirmRow.invalidated : null,
      invalidationreason: confirmRow ? confirmRow.invalidationreason : null,
      zonepos: confirmRow ? confirmRow.zonepos : null,
      inzone: confirmRow ? confirmRow.inzone : null,
      truthmatch: confirmRow ? confirmRow.truthmatch : null,
      strictb_ishit: strictB ? strictB.ishit : null,
      strictb_reason: strictB ? strictB.reason : null,
      strictb_score: strictB ? strictB.score : null,
    };
  }

  function buildDerived(raw) {
    const confirms = Array.isArray(raw && raw.tconfirm) ? raw.tconfirm : [];
    const scores = Array.isArray(raw && raw.tscore) ? raw.tscore : [];
    const rules = Array.isArray(raw && raw.trulehit) ? raw.trulehit : [];

    const rulesByConfirmId = new Map();
    for (const row of rules) {
      const confirmId = toNum(row && (row.tconfirmid != null ? row.tconfirmid : row.confirmid));
      if (confirmId == null) continue;
      const bucket = rulesByConfirmId.get(confirmId) || [];
      bucket.push(row);
      rulesByConfirmId.set(confirmId, bucket);
    }

    const bestScoreByConfirmId = new Map();
    for (const row of scores) {
      const confirmId = toNum(row && (row.tconfirmid != null ? row.tconfirmid : row.confirmid));
      if (confirmId == null) continue;
      const current = bestScoreByConfirmId.get(confirmId);
      const currentScore = current ? toNum(current.totalscore) : null;
      const nextScore = toNum(row.totalscore);
      if (current == null || (nextScore != null && (currentScore == null || nextScore > currentScore))) {
        bestScoreByConfirmId.set(confirmId, row);
      }
    }

    const confirmsById = new Map();
    const decoratedConfirms = confirms.map((row) => {
      const confirmId = toNum(row && row.id);
      const strictB = pickStrictB(rulesByConfirmId.get(confirmId));
      const bestScore = bestScoreByConfirmId.get(confirmId);
      const decorated = decorateConfirmRow(row, bestScore, strictB);
      if (confirmId != null) confirmsById.set(confirmId, decorated);
      return decorated;
    });

    const decoratedScores = scores.map((row) => {
      const confirmId = toNum(row && (row.tconfirmid != null ? row.tconfirmid : row.confirmid));
      const confirmRow = confirmId != null ? confirmsById.get(confirmId) : null;
      const strictB = pickStrictB(rulesByConfirmId.get(confirmId));
      return decorateScoreRow(row, confirmRow, strictB);
    });

    return {
      confirms: decoratedConfirms,
      scores: decoratedScores,
      rulesByConfirmId,
    };
  }

  function selectedGrades() {
    const grades = [];
    ["A", "B", "C", "D", "F"].forEach((grade) => {
      const el = $(`grade-${grade}`);
      if (el && el.checked) grades.push(grade);
    });
    return grades;
  }

  function readFilters() {
    const rawMin = $("filter-min-score") ? $("filter-min-score").value : "";
    const minScore = rawMin === "" ? 0 : Number(rawMin);
    return {
      minScore: Number.isFinite(minScore) ? Math.max(0, minScore) : 0,
      grades: selectedGrades(),
      onlyTruthmatched: !!($("filter-truthmatched") && $("filter-truthmatched").checked),
      onlyConfirmed: !!($("filter-confirmed") && $("filter-confirmed").checked),
      onlyInvalidated: !!($("filter-invalidated") && $("filter-invalidated").checked),
    };
  }

  function rowMatchesFilters(row, filters) {
    const confirmState = String((row && row.confirmstate) || "").trim().toLowerCase();

    if (!filters.grades.length) return false;

    if (filters.onlyTruthmatched && !toBool(row && row.truthmatch)) return false;
    if (filters.onlyConfirmed && confirmState !== "confirmed") return false;
    if (filters.onlyInvalidated && confirmState !== "invalidated") return false;

    const totalScore = toNum(row && row.totalscore);
    if (filters.minScore > 0 && (totalScore == null || totalScore < filters.minScore)) return false;

    const grade = String((row && row.scoregrade) || "").trim().toUpperCase();
    if (filters.grades.length && !filters.grades.includes(grade)) return false;

    return true;
  }

  function buildViewPayload() {
    const raw = state.rawPayload || {};
    const derived = state.derived || buildDerived(raw);
    const filters = readFilters();

    const filteredScores = derived.scores.filter((row) => rowMatchesFilters(row, filters));
    const visibleConfirmIdsFromScores = new Set(
      filteredScores
        .map((row) => toNum(row && row.tconfirmid))
        .filter((value) => value != null)
    );

    const filteredConfirms = derived.confirms.filter((row) => {
      const confirmId = toNum(row && row.tconfirmid);
      const baseMatch = rowMatchesFilters(row, filters);
      if (!baseMatch) return false;
      if (filters.minScore > 0 || filters.grades.length < 5) {
        return confirmId != null && visibleConfirmIdsFromScores.has(confirmId);
      }
      return true;
    });

    const visibleConfirmIds = new Set(
      filteredConfirms
        .map((row) => toNum(row && row.tconfirmid))
        .filter((value) => value != null)
    );

    const filteredRulehits = (Array.isArray(raw.trulehit) ? raw.trulehit : []).filter((row) => {
      const confirmId = toNum(row && (row.tconfirmid != null ? row.tconfirmid : row.confirmid));
      return confirmId != null && visibleConfirmIds.has(confirmId);
    });

    return {
      ...raw,
      tconfirm: filteredConfirms,
      tscore: filteredScores.filter((row) => {
        const confirmId = toNum(row && row.tconfirmid);
        return confirmId != null && visibleConfirmIds.has(confirmId);
      }),
      trulehit: filteredRulehits,
    };
  }

  function formatSummary(raw, view) {
    const ticksLoaded = Array.isArray(view && view.ticks) ? view.ticks.length : 0;
    const pivots = Array.isArray(view && view.pivots) ? view.pivots.length : 0;
    const zones = Array.isArray(view && view.tzone) ? view.tzone.length : 0;
    const episodes = Array.isArray(view && view.tepisode) ? view.tepisode.length : 0;
    const confirmsVisible = Array.isArray(view && view.tconfirm) ? view.tconfirm.length : 0;
    const confirmsTotal = Array.isArray(raw && raw.tconfirm) ? raw.tconfirm.length : 0;
    const scoresVisible = Array.isArray(view && view.tscore) ? view.tscore.length : 0;
    const scoresTotal = Array.isArray(raw && raw.tscore) ? raw.tscore.length : 0;

    if (view && view.mode === "day" && view.day) {
      return `day #${view.day.id} ${view.day.daydate || ""} | ticks=${ticksLoaded} pivots=${pivots} zones=${zones} episodes=${episodes} confirms=${confirmsVisible}/${confirmsTotal} scores=${scoresVisible}/${scoresTotal}`;
    }

    const range = (view && view.range) || {};
    return `window id ${range.startid || "?"}-${range.endid || "?"} | ticks=${ticksLoaded} pivots=${pivots} zones=${zones} episodes=${episodes} confirms=${confirmsVisible}/${confirmsTotal} scores=${scoresVisible}/${scoresTotal}`;
  }

  function buildStructureDetail(info) {
    const payload = info && info.payload ? info.payload : null;
    if (!payload) return defaultDetailText();

    const hasStructuralIdentity =
      payload.tconfirmid != null ||
      payload.anchorpivotid != null ||
      payload.confirmstate != null ||
      payload.totalscore != null;

    if (!hasStructuralIdentity) {
      return JSON.stringify(
        {
          seriesId: info.seriesId,
          seriesName: info.seriesName,
          payload,
        },
        null,
        2
      );
    }

    return [
      `${info.seriesName || "Structure Detail"}`,
      "",
      "Identity",
      `dayid: ${formatValue(payload.dayid)}`,
      `tepisodeid: ${formatValue(payload.tepisodeid != null ? payload.tepisodeid : payload.episodeid)}`,
      `tconfirmid: ${formatValue(payload.tconfirmid)}`,
      `dir: ${formatValue(payload.dir)}`,
      "",
      "Anchor",
      `anchorts: ${formatValue(payload.anchorts)}`,
      `anchorprice: ${formatNum(payload.anchorprice, 2)}`,
      `anchorpivotid: ${formatValue(payload.anchorpivotid)}`,
      "",
      "Confirmation",
      `confirmstate: ${formatValue(payload.confirmstate)}`,
      `microreversalticks: ${formatValue(payload.microreversalticks)}`,
      `lowerhighticks: ${formatValue(payload.lowerhighticks)}`,
      `lowerlowticks: ${formatValue(payload.lowerlowticks)}`,
      `breakhighticks: ${formatValue(payload.breakhighticks)}`,
      `invalidated: ${formatValue(payload.invalidated)}`,
      "",
      "Truth",
      `zonepos: ${formatValue(payload.zonepos)}`,
      `inzone: ${formatValue(payload.inzone)}`,
      `truthmatch: ${formatValue(payload.truthmatch)}`,
      "",
      "Score",
      `structurescore: ${formatNum(payload.structurescore, 1)}`,
      `contextscore: ${formatNum(payload.contextscore, 1)}`,
      `truthscore: ${formatNum(payload.truthscore, 1)}`,
      `penaltyscore: ${formatNum(payload.penaltyscore, 1)}`,
      `totalscore: ${formatNum(payload.totalscore, 1)}`,
      `scoregrade: ${formatValue(payload.scoregrade)}`,
      `reason: ${formatValue(payload.reason)}`,
      "",
      "Rule",
      `StrictB ishit: ${formatValue(payload.strictb_ishit)}`,
      `StrictB reason: ${formatValue(payload.strictb_reason)}`,
    ].join("\n");
  }

  function applyDataView() {
    if (!state.rawPayload) return;
    state.viewPayload = buildViewPayload();
    ChartCore.setStructureData(state.viewPayload);
    applyVisibility();
    setStatus(formatSummary(state.rawPayload, state.viewPayload));
  }

  async function loadNow() {
    const mode = selectedMode();
    const wantsTicks = includeTicks();

    setStatus("Loading...");
    setDetailText("");

    let data;
    if (mode === "day") {
      const dayId = selectedDayId();
      if (!dayId) {
        setStatus("No day selected.");
        return;
      }
      data = await fetchJSON(
        `/api/structure/day?day_id=${encodeURIComponent(dayId)}&include_ticks=${wantsTicks ? "true" : "false"}&include_rulehits=true`
      );
    } else {
      const fromIdRaw = $("structure-from-id") ? Number($("structure-from-id").value) : null;
      const windowRaw = $("structure-window") ? Number($("structure-window").value) : 80000;
      const fromId = Number.isFinite(fromIdRaw) && fromIdRaw > 0 ? Math.floor(fromIdRaw) : 1;
      const windowSize = Number.isFinite(windowRaw) && windowRaw >= 100 ? Math.floor(windowRaw) : 80000;
      data = await fetchJSON(
        `/api/structure/window?from_id=${encodeURIComponent(fromId)}&window=${encodeURIComponent(windowSize)}&include_ticks=${wantsTicks ? "true" : "false"}&include_rulehits=true`
      );
    }

    state.rawPayload = data;
    state.derived = buildDerived(data);
    state.lastIncludeTicks = wantsTicks;
    applyDataView();
  }

  function setGrades(grades) {
    ["A", "B", "C", "D", "F"].forEach((grade) => {
      const el = $(`grade-${grade}`);
      if (el) el.checked = grades.includes(grade);
    });
  }

  function resetFilters() {
    if ($("filter-min-score")) $("filter-min-score").value = "0";
    setGrades(["A", "B", "C", "D", "F"]);
    if ($("filter-truthmatched")) $("filter-truthmatched").checked = false;
    if ($("filter-confirmed")) $("filter-confirmed").checked = false;
    if ($("filter-invalidated")) $("filter-invalidated").checked = false;
  }

  function applyPreset(kind) {
    if (kind === "clear") {
      resetFilters();
    } else if (kind === "ab") {
      if ($("filter-min-score")) $("filter-min-score").value = "0";
      setGrades(["A", "B"]);
      if ($("filter-truthmatched")) $("filter-truthmatched").checked = false;
      if ($("filter-confirmed")) $("filter-confirmed").checked = false;
      if ($("filter-invalidated")) $("filter-invalidated").checked = false;
    } else if (kind === "confirmed60") {
      if ($("filter-min-score")) $("filter-min-score").value = "60";
      setGrades(["A", "B", "C", "D", "F"]);
      if ($("filter-truthmatched")) $("filter-truthmatched").checked = false;
      if ($("filter-confirmed")) $("filter-confirmed").checked = true;
      if ($("filter-invalidated")) $("filter-invalidated").checked = false;
    } else if (kind === "truth60") {
      if ($("filter-min-score")) $("filter-min-score").value = "60";
      setGrades(["A", "B", "C", "D", "F"]);
      if ($("filter-truthmatched")) $("filter-truthmatched").checked = true;
      if ($("filter-confirmed")) $("filter-confirmed").checked = false;
      if ($("filter-invalidated")) $("filter-invalidated").checked = false;
    }
    applyDataView();
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

    ["toggle-kal", "toggle-pivots", "toggle-zones", "toggle-episodes", "toggle-confirms", "toggle-scores"].forEach((id) => {
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

    [
      "filter-min-score",
      "grade-A",
      "grade-B",
      "grade-C",
      "grade-D",
      "grade-F",
      "filter-truthmatched",
      "filter-confirmed",
      "filter-invalidated",
    ].forEach((id) => {
      const el = $(id);
      if (!el) return;
      el.addEventListener("change", applyDataView);
      if (id === "filter-min-score") {
        el.addEventListener("input", applyDataView);
      }
    });

    ["preset-clear", "preset-ab", "preset-confirmed60", "preset-truth60"].forEach((id) => {
      const el = $(id);
      if (!el) return;
      el.addEventListener("click", () => applyPreset(String(el.dataset.preset || "")));
    });
  }

  function onError(err) {
    console.error("structure-core load failed", err);
    setStatus(`Load failed: ${err.message || err}`);
  }

  document.addEventListener("DOMContentLoaded", async () => {
    ChartCore.init("structure-chart");
    ChartCore.setClickHandler((info) => {
      setDetailText(buildStructureDetail(info));
    });

    wireUi();
    syncModeUi();
    resetFilters();
    applyVisibility();
    setDetailText("");

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
