(function () {
  const sections = [
    { key: "overview", label: "Overview", path: "/control", subtitle: "Operational overview" },
    { key: "mission", label: "Mission", path: "/control/mission", subtitle: "Supervisor guidance" },
    { key: "research", label: "Research", path: "/control/research", subtitle: "Loop status and jobs" },
    { key: "incidents", label: "Incidents", path: "/control/incidents", subtitle: "Engineering incidents" },
    { key: "candidates", label: "Candidates", path: "/control/candidates", subtitle: "Setup library" },
    { key: "day-review", label: "Day Review", path: "/control/day-review", subtitle: "Entry review" },
    { key: "journals", label: "Journals", path: "/control/journals", subtitle: "Unified journals" },
    { key: "settings", label: "Settings", path: "/control/settings", subtitle: "Bounded policy" }
  ];

  const state = {
    currentSection: sectionFromPath(location.pathname),
    overview: null,
    mission: null,
    settings: null,
    research: null,
    incidents: null,
    candidates: null,
    journals: null,
    dayReview: null,
    pollTimer: null,
    loading: false,
    candidateFilters: { day: "", side: "", family: "", promotedStatus: "", spreadRegime: "", sessionBucket: "" },
    journalFilters: { component: "", level: "", eventType: "" },
    dayReviewFilters: { day: "", runId: "", setupFingerprint: "" },
    journalLimit: 20,
    dayReviewLimit: 20
  };

  const elements = {
    nav: document.getElementById("controlNav"),
    content: document.getElementById("controlContent"),
    status: document.getElementById("controlStatusLine"),
    pageEyebrow: document.getElementById("pageEyebrow"),
    pageTitle: document.getElementById("pageTitle"),
    sidebarMeta: document.getElementById("controlSidebarMeta"),
    refresh: document.getElementById("refreshButton")
  };

  function sectionFromPath(pathname) {
    const normalized = String(pathname || "/control").replace(/\/+$/, "") || "/control";
    const match = sections.find((section) => section.path === normalized);
    return match ? match.key : "overview";
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function setStatus(message, tone) {
    elements.status.textContent = message;
    elements.status.classList.remove("error", "success");
    if (tone) {
      elements.status.classList.add(tone);
    }
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(function () { return {}; });
    if (!response.ok) {
      throw payload;
    }
    return payload;
  }

  function humanNumber(value, digits) {
    if (value == null || value === "") {
      return "n/a";
    }
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return String(value);
    }
    return number.toFixed(typeof digits === "number" ? digits : 2);
  }

  function humanWhen(value) {
    if (!value) {
      return "n/a";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    return date.toLocaleString();
  }

  function badge(text, tone) {
    return "<span class=\"control-badge" + (tone ? " is-" + tone : "") + "\">" + escapeHtml(text) + "</span>";
  }

  function renderNav() {
    elements.nav.innerHTML = sections.map(function (section) {
      const active = section.key === state.currentSection;
      return [
        "<button class=\"control-nav-button", active ? " active" : "", "\" type=\"button\" data-section=\"", escapeHtml(section.key), "\">",
        "<span class=\"control-nav-label\">", escapeHtml(section.label), "</span>",
        "<small>", escapeHtml(section.subtitle), "</small>",
        "</button>"
      ].join("");
    }).join("");
  }

  function card(title, body, options) {
    const extra = options && options.extra ? options.extra : "";
    return [
      "<section class=\"panel control-card\">",
      "<div class=\"control-card-head\"><div class=\"sql-label\">", escapeHtml(title), "</div>", extra, "</div>",
      "<div class=\"control-card-body\">", body, "</div>",
      "</section>"
    ].join("");
  }

  function kvGrid(items) {
    return [
      "<div class=\"control-kv-grid\">",
      items.map(function (item) {
        return [
          "<div class=\"control-kv-item\">",
          "<span>", escapeHtml(item.label), "</span>",
          "<strong>", escapeHtml(item.value == null ? "n/a" : item.value), "</strong>",
          "</div>"
        ].join("");
      }).join(""),
      "</div>"
    ].join("");
  }

  function simpleTable(columns, rows) {
    if (!rows.length) {
      return "<div class=\"sql-empty\">No rows available.</div>";
    }
    return [
      "<div class=\"control-table-wrap\"><table class=\"control-table\"><thead><tr>",
      columns.map(function (column) { return "<th>" + escapeHtml(column.label) + "</th>"; }).join(""),
      "</tr></thead><tbody>",
      rows.map(function (row) {
        return "<tr>" + columns.map(function (column) {
          const value = typeof column.render === "function" ? column.render(row) : row[column.key];
          return "<td>" + (value == null ? "" : value) + "</td>";
        }).join("") + "</tr>";
      }).join(""),
      "</tbody></table></div>"
    ].join("");
  }

  function actionButtons(items) {
    return [
      "<div class=\"control-action-row\">",
      items.map(function (item) {
        return "<button class=\"" + escapeHtml(item.kind || "ghost-button") + " compact-button\" type=\"button\" data-action=\"" + escapeHtml(item.action) + "\">" + escapeHtml(item.label) + "</button>";
      }).join(""),
      "</div>"
    ].join("");
  }

  function humanDuration(value) {
    const seconds = Number(value);
    if (!Number.isFinite(seconds) || seconds < 0) {
      return "n/a";
    }
    if (seconds < 60) {
      return seconds + "s";
    }
    if (seconds < 3600) {
      return Math.floor(seconds / 60) + "m " + (seconds % 60) + "s";
    }
    return Math.floor(seconds / 3600) + "h " + Math.floor((seconds % 3600) / 60) + "m";
  }

  function renderStoryCard(story) {
    if (!story) {
      return "<div class=\"sql-empty\">No control story available yet.</div>";
    }
    return [
      "<div class=\"control-list\">",
      "<div class=\"control-list-item\"><strong>What Just Happened</strong><div class=\"sql-muted\">", escapeHtml(story.whatJustHappened || "n/a"), "</div></div>",
      "<div class=\"control-list-item\"><strong>What Is Running Now</strong><div class=\"sql-muted\">", escapeHtml(story.runningNow || "n/a"), "</div></div>",
      "<div class=\"control-list-item\"><strong>What Is Blocked Now</strong><div class=\"sql-muted\">", escapeHtml(story.blockedNow || "none"), "</div></div>",
      "<div class=\"control-list-item\"><strong>What The Machine Wants Next</strong><div class=\"sql-muted\">", escapeHtml(story.machineNext || "n/a"), "</div></div>",
      "<div class=\"control-list-item\"><strong>What The Latest Verdict Means</strong><div class=\"sql-muted\">", escapeHtml(story.latestVerdictMeans || "n/a"), "</div></div>",
      "<div class=\"control-list-item\"><strong>Recommended Action</strong><div class=\"sql-muted\">", escapeHtml(story.recommendedAction || "No action required."), "</div></div>",
      "</div>"
    ].join("");
  }

  function renderSectionStory(overview) {
    return card("Control Story", renderStoryCard((overview || {}).story || {}));
  }

  function renderCurrentBest(candidate) {
    const latestCandidate = candidate || {};
    const latestMetrics = latestCandidate.metrics || {};
    const divergenceDetail = latestCandidate.family === "divergence_sweep"
      ? "<div class=\"control-callout\"><div><span class=\"sql-label\">Divergence Detail</span><p class=\"control-copy\">" +
        escapeHtml([
          latestCandidate.eventSubtype || "n/a",
          latestCandidate.indicator || "n/a",
          latestCandidate.signalStyle || "n/a"
        ].join(" | ")) +
        "</p></div></div>"
      : "";
    return [
      kvGrid([
        { label: "Setup", value: latestCandidate.candidateName || "n/a" },
        { label: "Family", value: latestCandidate.family || "n/a" },
        { label: "Side", value: latestCandidate.side || "n/a" },
        { label: "Broker Day", value: latestCandidate.brokerday || "n/a" }
      ]),
      divergenceDetail,
      "<div class=\"control-metric-row\">",
      metricPill("Clean Precision", humanNumber(latestMetrics.cleanPrecision, 3)),
      metricPill("Entries/Day", humanNumber(latestMetrics.entriesPerDay, 2)),
      metricPill("Median Hit Sec", humanNumber(latestMetrics.medianHitSeconds, 1)),
      metricPill("Walk Range", humanNumber(latestMetrics.walkForwardRange, 3)),
      "</div>"
    ].join("");
  }

  function renderLastCompletedResult(result) {
    if (!result || !result.runId) {
      return "<div class=\"sql-empty\">No completed run result is available yet.</div>";
    }
    const selectedCandidate = result.selectedCandidate || {};
    const divergenceDetail = selectedCandidate.eventSubtype || selectedCandidate.indicator || selectedCandidate.signalStyle
      ? "<div class=\"control-callout\"><div><span class=\"sql-label\">Selected Divergence</span><p class=\"control-copy\">" +
        escapeHtml([
          selectedCandidate.candidateName || "n/a",
          selectedCandidate.eventSubtype || "n/a",
          selectedCandidate.indicator || "n/a",
          selectedCandidate.signalStyle || "n/a"
        ].join(" | ")) +
        "</p></div></div>"
      : "";
    return [
      kvGrid([
        { label: "Run", value: result.runId || "n/a" },
        { label: "Broker Day", value: result.brokerday || "n/a" },
        { label: "Family", value: result.family || "n/a" },
        { label: "Fingerprint", value: result.fingerprint || "n/a" },
        { label: "Verdict", value: result.verdict || "n/a" },
        { label: "Candidates", value: String(result.candidateCount == null ? 0 : result.candidateCount) },
        { label: "Best Improved", value: truthLabel(result.bestImproved) },
        { label: "Finished", value: humanWhen(result.finishedAt) }
      ]),
      "<div class=\"control-callout\">",
      "<div><span class=\"sql-label\">Headline</span><p class=\"control-copy\">", escapeHtml(result.headline || "No summary headline recorded."), "</p></div>",
      "</div>",
      divergenceDetail,
      "<div class=\"control-metric-row\">",
      metricPill("Clean Precision", humanNumber((result.metrics || {}).cleanPrecision, 3)),
      metricPill("Entries/Day", humanNumber((result.metrics || {}).entriesPerDay, 2)),
      metricPill("Walk Range", humanNumber((result.metrics || {}).walkForwardRange, 3)),
      metricPill("Signals", humanNumber((result.metrics || {}).signalCount, 0)),
      "</div>"
    ].join("");
  }

  function renderOverview(data) {
    const research = data.research || {};
    const engineering = data.engineering || {};
    return [
      "<div class=\"control-grid control-grid-overview\">",
      card("Mission", [
        "<h3 class=\"control-headline\">", escapeHtml((data.mission || {}).missionTitle || "Mission"), "</h3>",
        "<p class=\"control-copy\">", escapeHtml((data.mission || {}).mainObjective || "No mission configured."), "</p>",
        kvGrid([
          { label: "Selected Study Day", value: data.selectedStudyDay || "latest available" },
          { label: "Latest Run Day", value: data.brokerday || "unknown" },
          { label: "Research", value: research.state || "unknown" },
          { label: "Engineering", value: engineering.state || "unknown" },
          { label: "Queue Depth", value: String(((research.activity || {}).queueDepth) == null ? 0 : ((research.activity || {}).queueDepth)) }
        ]),
        actionButtons([
          { action: "pauseResearch", label: "Pause Research", kind: "ghost-button" },
          { action: "resumeResearch", label: "Resume Research", kind: "ghost-button" },
          { action: "pauseEngineering", label: "Pause Engineering", kind: "ghost-button" },
          { action: "resumeEngineering", label: "Resume Engineering", kind: "ghost-button" }
        ])
      ].join("")),
      card("Study Day", renderStudyDayControl(research)),
      card("Operating Story", renderStoryCard(data.story || {})),
      card("Live Research", renderResearchActivity(research.activity || {})),
      card("Last Completed Result", renderLastCompletedResult(research.lastCompletedResult || {})),
      card("Current Best", renderCurrentBest(research.bestCandidate || {})),
      card("Latest Incident", renderIncidentSummary(engineering.activeIncident)),
      card("Latest Engineering Action", renderActionSummary(engineering.latestAction)),
      card("Safe Controls", actionButtons([
        { action: "restartResearch", label: "Restart Research Services", kind: "primary-button" },
        { action: "restartControl", label: "Restart Control Services", kind: "ghost-button" },
        { action: "requeueResearch", label: "Requeue Latest Failed Job", kind: "ghost-button" },
        { action: "runSmoke", label: "Run Smoke Tests", kind: "ghost-button" }
      ])),
      "</div>"
    ].join("");
  }

  function metricPill(label, value) {
    return "<div class=\"control-metric-pill\"><span>" + escapeHtml(label) + "</span><strong>" + escapeHtml(value) + "</strong></div>";
  }

  function truthLabel(value) {
    return value ? "yes" : "no";
  }

  function renderIncidentSummary(incident) {
    if (!incident || !incident.id) {
      return "<div class=\"sql-empty\">No active incident.</div>";
    }
    return [
      "<h3 class=\"control-headline\">#", escapeHtml(incident.id), " ", escapeHtml(incident.summary || "Incident"), "</h3>",
      kvGrid([
        { label: "Status", value: incident.status || "n/a" },
        { label: "Class", value: incident.incident_type || "n/a" },
        { label: "Fingerprint", value: incident.fingerprint || "n/a" },
        { label: "Retries", value: String(incident.retry_count || 0) + "/" + String(incident.max_retries || 0) }
      ]),
      "<div class=\"control-callout\">",
      "<div><span class=\"sql-label\">Root Cause</span><p class=\"control-copy\">", escapeHtml(incident.rootCause || incident.summary || "n/a"), "</p></div>",
      "</div>"
    ].join("");
  }

  function renderActionSummary(action) {
    if (!action || !action.id) {
      return "<div class=\"sql-empty\">No recent engineering action.</div>";
    }
    return [
      kvGrid([
        { label: "Action", value: action.action_type || "n/a" },
        { label: "Status", value: action.status || "n/a" },
        { label: "Requested By", value: action.requested_by || "n/a" },
        { label: "Finished", value: humanWhen(action.finished_at || action.finishedAt) }
      ]),
      action.error_text ? "<div class=\"control-callout\"><div><span class=\"sql-label\">Failure</span><p class=\"control-copy\">" + escapeHtml(action.error_text) + "</p></div></div>" : ""
    ].join("");
  }

  function renderResearchNarrative(summary) {
    if (!summary) {
      return "<div class=\"sql-empty\">No research summary available.</div>";
    }
    return [
      kvGrid([
        { label: "Last Completed", value: summary.lastCompletedRun || "n/a" },
        { label: "Selected Study Day", value: summary.selectedStudyDay || "latest available" },
        { label: "Last Family", value: summary.lastFamilyTried || "n/a" },
        { label: "Last Verdict", value: summary.lastVerdict || "n/a" },
        { label: "Queue Depth", value: String(summary.queueDepth == null ? 0 : summary.queueDepth) },
        { label: "Best Current", value: summary.bestCandidate || "n/a" }
      ]),
      "<div class=\"control-callout\">",
      "<div><span class=\"sql-label\">Current Blocker</span><p class=\"control-copy\">", escapeHtml(summary.currentBlocker || "none"), "</p></div>",
      "<div><span class=\"sql-label\">Recommended Action</span><p class=\"control-copy\">", escapeHtml(summary.recommendedAction || "No action required."), "</p></div>",
      "<div><span class=\"sql-label\">What The Verdict Means</span><p class=\"control-copy\">", escapeHtml(summary.latestVerdictMeaning || "n/a"), "</p></div>",
      "</div>"
    ].join("");
  }

  function renderStudyDayControl(research) {
    const studyDay = (research || {}).studyDay || {};
    const selected = studyDay.selectedStudyDay || "";
    const available = Array.isArray(studyDay.availableBrokerdays) ? studyDay.availableBrokerdays.slice() : [];
    if (selected && available.indexOf(selected) === -1) {
      available.unshift(selected);
    }
    return [
      "<form class=\"control-form\" data-role=\"studyDayForm\">",
      "<div class=\"control-form-grid\">",
      "<label class=\"control-field\">",
      "<span>Study Broker Day</span>",
      "<select name=\"brokerday\">",
      "<option value=\"\">Latest available</option>",
      available.map(function (day) {
        const isSelected = String(day) === String(selected) ? " selected" : "";
        return "<option value=\"" + escapeHtml(day) + "\"" + isSelected + ">" + escapeHtml(day) + "</option>";
      }).join(""),
      "</select>",
      "</label>",
      "</div>",
      kvGrid([
        { label: "Selected", value: studyDay.selectedStudyDay || "latest available" },
        { label: "Effective", value: studyDay.effectiveStudyDay || "n/a" },
        { label: "Latest Available", value: studyDay.latestAvailableBrokerday || "n/a" },
        { label: "Recent Days", value: String(available.length) }
      ]),
      "<div class=\"control-action-row\">",
      "<button class=\"ghost-button compact-button\" type=\"submit\">Save Study Day</button>",
      "<button class=\"primary-button compact-button\" type=\"button\" data-action=\"seedDivergenceSweep\">Run Divergence Sweep</button>",
      "</div>",
      "</form>"
    ].join("");
  }

  function renderResearchActivity(activity) {
    if (!activity) {
      return "<div class=\"sql-empty\">No runtime activity available.</div>";
    }
    return [
      kvGrid([
        { label: "Loop State", value: activity.loopState || "unknown" },
        { label: "Current Run", value: activity.currentRunId || "none" },
        { label: "Current Job", value: activity.currentJobId || "none" },
        { label: "Phase", value: activity.currentPhase || "unknown" },
        { label: "Step", value: activity.currentStep || "unknown" },
        { label: "Queue Depth", value: String(activity.queueDepth == null ? 0 : activity.queueDepth) },
        { label: "Family", value: activity.currentFamily || "unknown" },
        { label: "Fingerprint", value: activity.currentFingerprint || "unknown" },
        { label: "Proposal Source", value: activity.currentProposalSource || "unknown" },
        { label: "Worker Consuming", value: truthLabel(activity.workerActivelyConsuming) },
        { label: "Worker Service", value: activity.workerServiceState || "unknown" },
        { label: "Worker Heartbeat", value: humanWhen(activity.workerHeartbeatAt) },
        { label: "Run Elapsed", value: humanDuration(activity.activeRunElapsedSeconds) },
        { label: "Supervisor Service", value: activity.supervisorServiceState || "unknown" },
        { label: "Orchestrator Active", value: truthLabel(activity.orchestratorActive) },
        { label: "Orchestrator Service", value: activity.orchestratorServiceState || "unknown" },
        { label: "Engineering", value: activity.engineeringState || "idle" },
        { label: "Engineering Blocking", value: truthLabel(activity.engineeringBlocking) }
      ]),
      "<div class=\"control-mini-list\">",
      "<div class=\"control-list-item\"><strong>Worker last claimed job</strong><div class=\"sql-muted\">",
      escapeHtml(activity.workerLastClaimedJobId ? ("Job " + activity.workerLastClaimedJobId + " at " + humanWhen(activity.workerLastClaimedAt)) : "No claim event recorded."),
      "</div></div>",
      "<div class=\"control-list-item\"><strong>Latest worker event</strong><div class=\"sql-muted\">",
      escapeHtml(activity.workerLastEventType ? (activity.workerLastEventType + " at " + humanWhen(activity.workerLastEventAt) + " | " + (activity.workerLastEventMessage || "")) : "No worker event recorded."),
      "</div></div>",
      "<div class=\"control-list-item\"><strong>Orchestrator last event</strong><div class=\"sql-muted\">",
      escapeHtml(activity.orchestratorLastEventAt ? humanWhen(activity.orchestratorLastEventAt) : "No recent orchestrator event recorded."),
      "</div></div>",
      "<div class=\"control-list-item\"><strong>Latest event anywhere</strong><div class=\"sql-muted\">",
      escapeHtml(activity.latestEventType ? ((activity.latestEventSource || "system") + " | " + activity.latestEventType + " at " + humanWhen(activity.latestEventAt)) : "No journal event recorded."),
      "</div></div>",
      "</div>"
    ].join("");
  }

  function proposalKindLabel(proposal) {
    if (!proposal) {
      return "n/a";
    }
    if (!proposal.proposalDerived && proposal.proposalKind === "seed_next_job") {
      return "manual seed";
    }
    return proposal.proposalKind || "proposal";
  }

  function renderMissionForm(data) {
    return [
      "<form class=\"control-form\" id=\"missionForm\">",
      field("Mission Title", "missionTitle", data.missionTitle),
      textareaField("Main Objective", "mainObjective", data.mainObjective, 4),
      textareaField("Definition Of Tradable", "tradableDefinition", data.tradableDefinition, 4),
      field("Scoring Priority", "scoringPriority", data.scoringPriority),
      field("Current Phase", "currentPhase", data.currentPhase),
      textareaField("Allowed Directions", "allowedDirections", (data.allowedDirections || []).join("\n"), 4),
      textareaField("Forbidden Directions", "forbiddenDirections", (data.forbiddenDirections || []).join("\n"), 4),
      "<div class=\"control-form-grid\">",
      field("Minimum Runs Before Stop", "minimumRunsBeforeStop", data.minimumRunsBeforeStop, "number"),
      selectField("Preferred Side Lock", "preferredSideLock", data.preferredSideLock, ["both", "long", "short"]),
      checkboxField("Require Same-Day Holdout", "sameDayHoldoutRequired", data.sameDayHoldoutRequired),
      checkboxField("Require Prior-Day Validation", "priorDayValidationRequired", data.priorDayValidationRequired),
      "</div>",
      textareaField("Operator Guidance", "guidanceNotes", data.guidanceNotes, 5),
      "<div class=\"control-action-row\"><button class=\"primary-button compact-button\" type=\"submit\">Save Mission</button></div>",
      "</form>"
    ].join("");
  }

  function renderResearch(data) {
    const latestRun = data.latestRun || {};
    const lastCompletedRun = data.lastCompletedRun || {};
    const currentRun = data.currentRun || {};
    const currentJob = data.currentJob || {};
    const focusRun = currentRun.id ? currentRun : (lastCompletedRun.id ? lastCompletedRun : latestRun);
    return [
      "<div class=\"control-grid control-grid-overview\">",
      renderSectionStory(state.overview),
      card("What Just Happened", renderResearchNarrative(data.summary || {})),
      card("Active State", renderResearchActivity(data.activity || {})),
      card("Current Run / Job", [
        kvGrid([
          { label: "Run", value: currentRun.id || "none" },
          { label: "Job", value: currentJob.id || "none" },
          { label: "Selected Study Day", value: (data.selectedStudyDay) || "latest available" },
          { label: "Broker Day", value: ((data.activity || {}).currentBrokerday) || "unknown" },
          { label: "Target Study Day", value: ((data.activity || {}).currentStudyDayTarget) || ((data.activity || {}).nextStudyDayTarget) || "n/a" },
          { label: "Family", value: ((data.activity || {}).currentFamily) || "unknown" },
          { label: "Fingerprint", value: ((data.activity || {}).currentFingerprint) || "unknown" },
          { label: "Proposal", value: ((data.activity || {}).currentProposalKind) || "unknown" },
          { label: "Proposal Source", value: ((data.activity || {}).currentProposalSource) || "unknown" },
          { label: "Elapsed", value: humanDuration((data.activity || {}).activeRunElapsedSeconds) }
        ]),
        "<div class=\"control-callout\">",
        "<div><span class=\"sql-label\">Mutation Note</span><p class=\"control-copy\">", escapeHtml(((data.activity || {}).currentMutationNote) || "No mutation note recorded for the active job."), "</p></div>",
        "</div>"
      ].join("")),
      card("Research State", [
        kvGrid([
          { label: "Loop State", value: data.state || "unknown" },
          { label: "Last Completed Run", value: (data.summary || {}).lastCompletedRun || "n/a" },
          { label: "Selected Study Day", value: data.selectedStudyDay || "latest available" },
          { label: "Current Run", value: currentRun.id || "none" },
          { label: "Queue Depth", value: String((((data.activity || {}).queueDepth) == null ? 0 : ((data.activity || {}).queueDepth))) },
          { label: "Fingerprint", value: (((focusRun.config || {}).config_fingerprint) || "n/a") },
          { label: "Recommended Action", value: data.recommendedAction || "No action required." }
        ]),
        actionButtons([
          { action: "pauseResearch", label: "Pause", kind: "ghost-button" },
          { action: "resumeResearch", label: "Resume", kind: "ghost-button" },
          { action: "seedDivergenceSweep", label: "Run Divergence Sweep", kind: "primary-button" },
          { action: "seedResearch", label: "Seed Next Job", kind: "ghost-button" },
          { action: "resetResearch", label: "Reset State", kind: "ghost-button" }
        ])
      ].join("")),
      card("Study Day Control", renderStudyDayControl(data)),
      card("Last Completed Result", renderLastCompletedResult(data.lastCompletedResult || {})),
      card("Current Config", kvGrid([
        { label: "Study Day", value: ((focusRun.config || {}).study_brokerday) || "n/a" },
        { label: "Slice Rows", value: ((focusRun.config || {}).slice_rows) || "n/a" },
        { label: "Family", value: ((focusRun.config || {}).candidate_family) || "n/a" },
        { label: "Side Lock", value: ((focusRun.config || {}).side_lock) || "n/a" },
        { label: "Label Variant", value: ((focusRun.config || {}).label_variant) || "n/a" }
      ])),
      card("Queued Jobs", simpleTable([
        { label: "ID", render: function (row) { return escapeHtml(row.id); } },
        { label: "Status", render: function (row) { return escapeHtml(row.status); } },
        { label: "Requested By", render: function (row) { return escapeHtml(row.requested_by || "n/a"); } },
        { label: "Study Day", render: function (row) { return escapeHtml(((row.config || {}).study_brokerday) || "n/a"); } },
        { label: "Proposal", render: function (row) { return escapeHtml(proposalKindLabel(row.proposal || {})); } },
        { label: "Proposal Source", render: function (row) { return escapeHtml(((row.proposal || {}).proposalSource) || "n/a"); } },
        { label: "Family", render: function (row) { return escapeHtml(((row.proposal || {}).family) || ((row.config || {}).candidate_family) || "n/a"); } },
        { label: "Fingerprint", render: function (row) { return "<div class=\"control-wrap\">" + escapeHtml(((row.proposal || {}).fingerprint) || ((row.config || {}).config_fingerprint) || "n/a") + "</div>"; } },
        { label: "Lineage", render: function (row) {
          const proposal = row.proposal || {};
          const parts = [];
          if (proposal.derivedFromRunId) { parts.push("run " + proposal.derivedFromRunId); }
          if (row.sourceDecisionId) { parts.push("decision " + row.sourceDecisionId); }
          if (row.sourceJobId) { parts.push("job " + row.sourceJobId); }
          return "<div class=\"control-wrap\">" + escapeHtml(parts.join(" | ") || "n/a") + "</div>";
        } },
        { label: "Seed Rule", render: function (row) { return "<div class=\"control-wrap\">" + escapeHtml(((row.proposal || {}).seedRuleRef) || "n/a") + "</div>"; } },
        { label: "Mutation Note", render: function (row) { return "<div class=\"control-wrap\">" + escapeHtml(((row.proposal || {}).mutationNote) || "n/a") + "</div>"; } }
      ], (data.jobs || []).filter(function (row) { return row.status === "pending"; }).slice(0, 8))),
      card("Recent Runs", simpleTable([
        { label: "Run", render: function (row) { return escapeHtml(row.id); } },
        { label: "Broker Day", render: function (row) { return escapeHtml(row.brokerday || "n/a"); } },
        { label: "Study Day", render: function (row) { return escapeHtml(((row.config || {}).study_brokerday) || "n/a"); } },
        { label: "Family", render: function (row) { return escapeHtml(((row.config || {}).candidate_family) || "n/a"); } },
        { label: "Status", render: function (row) { return escapeHtml(row.status); } },
        { label: "Verdict", render: function (row) { return escapeHtml(row.verdict_hint || "n/a"); } },
        { label: "Precision", render: function (row) { return escapeHtml(humanNumber((row.metrics || {}).cleanPrecision, 3)); } },
        { label: "Candidates", render: function (row) { return escapeHtml(String(row.candidateCount == null ? 0 : row.candidateCount)); } }
      ], data.runs || [])),
      card("Next Proposals", renderProposalList(data.nextProposals || [])),
      card("Failed Jobs", simpleTable([
        { label: "Job", render: function (row) { return escapeHtml(row.id); } },
        { label: "Error", render: function (row) { return "<div class=\"control-wrap\">" + escapeHtml(row.error_text || row.error || "n/a") + "</div>"; } },
        { label: "Finished", render: function (row) { return escapeHtml(humanWhen(row.finished_at || row.created_at)); } }
      ], (data.jobs || []).filter(function (row) { return row.status === "failed"; }).slice(0, 8))),
      "</div>"
      ].join("");
  }

  function renderProposalList(items) {
    if (!items.length) {
      return "<div class=\"sql-empty\">No pending mutation proposals.</div>";
    }
    return "<div class=\"control-list\">" + items.map(function (item) {
      return [
        "<div class=\"control-list-item\">",
        "<strong>", escapeHtml((item.action || "proposal") + " -> " + (item.family || "n/a")), "</strong>",
        "<div class=\"sql-muted\">", escapeHtml(item.reason || ""), "</div>",
        "<small>", escapeHtml((item.mutatedFields || []).join(", ") || "bounded mutation"), "</small>",
        "<div class=\"control-wrap\">", escapeHtml(item.configFingerprint || "n/a"), "</div>",
        "<div class=\"sql-muted\">", escapeHtml(((item.proposalSource || item.source) || "unknown source") + (item.sourceRunId ? (" | from run " + item.sourceRunId) : "") + (item.seedRuleRef ? (" | " + item.seedRuleRef) : "")), "</div>",
        "</div>"
      ].join("");
    }).join("") + "</div>";
  }

  function renderIncidents(data) {
    return [
      "<div class=\"control-grid control-grid-overview\">",
      renderSectionStory(state.overview),
      card("Active Incident", [
        renderIncidentSummary(data.current || {}),
        actionButtons([
          { action: "pauseEngineering", label: "Pause Engineering", kind: "ghost-button" },
          { action: "resumeEngineering", label: "Resume Engineering", kind: "ghost-button" },
          { action: "retryIncident", label: "Retry Incident", kind: "ghost-button" },
          { action: "manualTakeover", label: "Manual Takeover", kind: "primary-button" },
          { action: "ackIncident", label: "Acknowledge Escalation", kind: "ghost-button" }
        ])
      ].join("")),
      card("Repair Trail", renderRepairTrail(data.current || {})),
      card("Recent Incidents", simpleTable([
        { label: "ID", render: function (row) { return escapeHtml(row.id); } },
        { label: "Status", render: function (row) { return escapeHtml(row.status); } },
        { label: "Type", render: function (row) { return escapeHtml(row.incident_type); } },
        { label: "Summary", render: function (row) { return "<div class=\"control-wrap\">" + escapeHtml(row.summary || "") + "</div>"; } },
        { label: "Root Cause", render: function (row) { return "<div class=\"control-wrap\">" + escapeHtml(row.rootCause || "n/a") + "</div>"; } }
      ], data.rows || [])),
      "</div>"
      ].join("");
  }

  function renderRepairTrail(incident) {
    const actions = incident.actions || [];
    const patches = incident.patches || [];
    const smokes = incident.smokeTests || [];
    return [
      "<div class=\"control-list-block\">",
      "<div class=\"sql-label\">Actions</div>",
      actions.length ? "<div class=\"control-list\">" + actions.map(function (item) {
        return "<div class=\"control-list-item\"><strong>" + escapeHtml(item.action_type || "action") + "</strong><div class=\"sql-muted\">" + escapeHtml(item.status || "") + " | " + escapeHtml(item.rationale || "") + "</div></div>";
      }).join("") + "</div>" : "<div class=\"sql-empty\">No actions yet.</div>",
      "</div>",
      "<div class=\"control-list-block\">",
      "<div class=\"sql-label\">Patches</div>",
      patches.length ? "<div class=\"control-list\">" + patches.map(function (item) {
        return "<div class=\"control-list-item\"><strong>" + escapeHtml(item.patch_type || "patch") + "</strong><div class=\"sql-muted\">" + escapeHtml((item.target_files || []).join(", ")) + "</div></div>";
      }).join("") + "</div>" : "<div class=\"sql-empty\">No patches yet.</div>",
      "</div>",
      "<div class=\"control-list-block\">",
      "<div class=\"sql-label\">Smoke Tests</div>",
      smokes.length ? "<div class=\"control-list\">" + smokes.map(function (item) {
        return "<div class=\"control-list-item\"><strong>" + escapeHtml(item.test_name || item.name || "smoke") + "</strong><div class=\"sql-muted\">" + escapeHtml(item.status || "") + " | " + escapeHtml(item.detail || "") + "</div></div>";
      }).join("") + "</div>" : "<div class=\"sql-empty\">No smoke tests yet.</div>",
      "</div>"
    ].join("");
  }

  function renderCandidates(data) {
    return [
      renderSectionStory(state.overview),
      card("Filters", [
        "<form class=\"control-form\" id=\"candidateFilterForm\">",
        "<div class=\"control-form-grid\">",
        field("Day", "day", state.candidateFilters.day),
        field("Side", "side", state.candidateFilters.side),
        field("Family", "family", state.candidateFilters.family),
        field("Status", "promotedStatus", state.candidateFilters.promotedStatus),
        field("Spread Regime", "spreadRegime", state.candidateFilters.spreadRegime),
        field("Session Bucket", "sessionBucket", state.candidateFilters.sessionBucket),
        "</div>",
        "<div class=\"control-action-row\"><button class=\"ghost-button compact-button\" type=\"submit\">Apply Filters</button></div>",
        "</form>"
      ].join("")),
      card("Setup Library", renderCandidateRows(data || []))
    ].join("");
  }

  function renderCandidateRows(rows) {
    if (!rows.length) {
      return "<div class=\"sql-empty\">No candidate setups found for the current filters.</div>";
    }
    return rows.map(function (row) {
      return [
        "<article class=\"control-candidate-card\">",
        "<div class=\"control-candidate-head\">",
        "<div><strong>", escapeHtml(row.candidateName || row.setupFingerprint), "</strong><div class=\"sql-muted\">", escapeHtml(row.setupFingerprint), "</div></div>",
        "<div class=\"control-candidate-badges\">",
        badge(row.status || "active", row.status === "promoted" ? "good" : (row.status === "archived" ? "danger" : "")),
        badge(row.side || "n/a"),
        badge(row.family || "n/a"),
        row.eventSubtype ? badge(row.eventSubtype) : "",
        row.indicator ? badge(row.indicator) : "",
        row.signalStyle ? badge(row.signalStyle) : "",
        "</div></div>",
        "<div class=\"control-metric-row\">",
        metricPill("Clean Precision", humanNumber((row.validationMetrics || {}).cleanPrecision, 3)),
        metricPill("Entries/Day", humanNumber(row.entriesPerDay, 2)),
        metricPill("Hit Speed", humanNumber(row.medianHitSeconds, 1)),
        metricPill("Walk Range", humanNumber((row.validationMetrics || {}).walkForwardRange, 3)),
        metricPill("Avg Adv", humanNumber(row.avgMaxAdverse, 4)),
        metricPill("Days", String(row.daysPassed || 0) + "/" + String(row.daysSeen || 0)),
        "</div>",
        row.family === "divergence_sweep"
          ? "<div class=\"control-callout\"><div><span class=\"sql-label\">Divergence Pack</span><p class=\"control-copy\">" + escapeHtml([
              row.eventSubtype || "n/a",
              row.indicator || "n/a",
              row.signalStyle || "n/a",
              row.spreadRegime || "n/a",
              row.sessionBucket || "n/a"
            ].join(" | ")) + "</p></div></div>"
          : "",
        "<div class=\"control-wrap\"><code>", escapeHtml(JSON.stringify((row.rule || {}).predicates || [])), "</code></div>",
        "<form class=\"control-candidate-form\" data-fingerprint=\"", escapeHtml(row.setupFingerprint), "\">",
        "<div class=\"control-form-grid\">",
        selectField("Status", "status", row.status, ["promoted", "active", "archived"]),
        textareaField("Operator Notes", "operatorNotes", row.operatorNotes || "", 2),
        "</div>",
        "<div class=\"control-action-row\"><button class=\"ghost-button compact-button\" type=\"submit\">Save Setup State</button></div>",
        "</form>",
        "</article>"
      ].join("");
    }).join("");
  }

  function renderDayReview(data) {
    const page = (data || {}).entriesPage || {};
    return [
      renderSectionStory(state.overview),
      card("Review Scope", [
        "<form class=\"control-form\" id=\"dayReviewForm\">",
        "<div class=\"control-form-grid\">",
        field("Broker Day", "day", state.dayReviewFilters.day || data.selectedStudyDay || data.brokerday || ""),
        field("Run ID", "runId", state.dayReviewFilters.runId || ((data.run || {}).id || "")),
        field("Setup Fingerprint", "setupFingerprint", state.dayReviewFilters.setupFingerprint || ""),
        "</div>",
        "<div class=\"control-action-row\"><button class=\"ghost-button compact-button\" type=\"submit\">Load Review</button></div>",
        "</form>"
      ].join("")),
      card("Review Run", renderLastCompletedResult({
        runId: (data.run || {}).id,
        brokerday: data.brokerday,
        family: (((data.run || {}).config || {}).candidate_family),
        fingerprint: (((data.run || {}).config || {}).config_fingerprint),
        verdict: (data.run || {}).verdict_hint,
        metrics: (data.run || {}).metrics,
        finishedAt: (data.run || {}).finished_at,
        headline: (data.run || {}).headline,
        candidateCount: (data.candidates || []).length
      })),
      card("Review Detail", [
        kvGrid([
          { label: "Selected Study Day", value: data.selectedStudyDay || "latest available" },
          { label: "Visible Entries", value: String((data.entries || []).length) },
          { label: "Total Entries", value: String(page.totalCount == null ? 0 : page.totalCount) },
          { label: "Has More", value: page.hasMore ? "yes" : "no" }
        ]),
        "<div class=\"control-callout\"><div><span class=\"sql-label\">Raw Detail</span><p class=\"control-copy\">Use SQL view for full raw detail when you need the complete entry set beyond the default page.</p></div></div>",
        page.hasMore ? "<div class=\"control-action-row\"><button class=\"ghost-button compact-button\" type=\"button\" data-action=\"loadMoreDayReview\">Load More</button></div>" : ""
      ].join("")),
      card("Chart", renderDayChart((data.chart || {}).ticks || [], (data.chart || {}).markers || [])),
      card("Matched Entries", simpleTable([
        { label: "Time", render: function (row) { return escapeHtml(humanWhen(row.timestamp)); } },
        { label: "Subtype", render: function (row) { return escapeHtml(row.eventSubtype || "n/a"); } },
        { label: "Indicator", render: function (row) { return escapeHtml(row.indicator || "n/a"); } },
        { label: "Style", render: function (row) { return escapeHtml(row.signalStyle || "n/a"); } },
        { label: "Side", render: function (row) { return escapeHtml(row.side); } },
        { label: "Spread", render: function (row) { return escapeHtml(humanNumber(row.spread, 4)); } },
        { label: "2x Spread", render: function (row) { return escapeHtml(humanNumber(row.targetAmount, 4)); } },
        { label: "Target Hit", render: function (row) { return escapeHtml(row.targetHit ? "yes" : "no"); } },
        { label: "First Hit", render: function (row) { return escapeHtml(row.firstSideHit || "n/a"); } },
        { label: "Hit Sec", render: function (row) { return escapeHtml(humanNumber(row.hitSeconds, 1)); } },
        { label: "Hit Ticks", render: function (row) { return escapeHtml(humanNumber(row.hitTicks, 0)); } },
        { label: "Max Adv", render: function (row) { return escapeHtml(humanNumber(row.maxAdverse, 4)); } },
        { label: "Max Fav", render: function (row) { return escapeHtml(humanNumber(row.maxFavorable, 4)); } },
        { label: "Scalp", render: function (row) { return escapeHtml(row.scalpQualified ? "yes" : "no"); } },
        { label: "Candidate", render: function (row) { return escapeHtml(row.candidate); } }
      ], data.entries || []))
    ].join("");
  }

  function renderDayChart(ticks, markers) {
    if (!ticks.length) {
      return "<div class=\"sql-empty\">No chart data available for this broker day.</div>";
    }
    const width = 1100;
    const height = 320;
    const prices = ticks.map(function (tick) { return Number(tick.mid); });
    const minPrice = Math.min.apply(null, prices);
    const maxPrice = Math.max.apply(null, prices);
    const priceSpan = Math.max(0.0001, maxPrice - minPrice);
    const indexById = {};
    ticks.forEach(function (tick, index) {
      indexById[tick.id] = index;
    });
    const path = ticks.map(function (tick, index) {
      const x = (index / Math.max(1, ticks.length - 1)) * width;
      const y = height - (((Number(tick.mid) - minPrice) / priceSpan) * height);
      return (index ? "L" : "M") + x.toFixed(2) + " " + y.toFixed(2);
    }).join(" ");
    const markerSvg = markers.slice(0, 250).map(function (marker) {
      const idx = indexById[marker.tickId];
      if (idx == null) {
        return "";
      }
      const x = (idx / Math.max(1, ticks.length - 1)) * width;
      const y = height - (((Number(marker.price) - minPrice) / priceSpan) * height);
      const color = marker.targetHit ? "#7ef0c7" : "#ff6b88";
      return "<circle cx=\"" + x.toFixed(2) + "\" cy=\"" + y.toFixed(2) + "\" r=\"4\" fill=\"" + color + "\" opacity=\"0.9\"></circle>";
    }).join("");
    return [
      "<svg class=\"control-day-chart\" viewBox=\"0 0 ", width, " ", height, "\" role=\"img\">",
      "<path d=\"", path, "\" fill=\"none\" stroke=\"#6dd8ff\" stroke-width=\"2\"></path>",
      markerSvg,
      "</svg>"
    ].join("");
  }

  function renderJournals(data) {
    const rows = (data || {}).rows || [];
    return [
      renderSectionStory(state.overview),
      card("Filters", [
        "<form class=\"control-form\" id=\"journalFilterForm\">",
        "<div class=\"control-form-grid\">",
        field("Component", "component", state.journalFilters.component),
        field("Level", "level", state.journalFilters.level),
        field("Event Type", "eventType", state.journalFilters.eventType),
        "</div>",
        "<div class=\"control-action-row\"><button class=\"ghost-button compact-button\" type=\"submit\">Apply Filters</button></div>",
        "</form>"
      ].join("")),
      card("Journal Detail", [
        kvGrid([
          { label: "Visible Events", value: String(rows.length) },
          { label: "Total Events", value: String((data || {}).totalCount == null ? 0 : (data || {}).totalCount) },
          { label: "Current Limit", value: String((data || {}).limit == null ? 0 : (data || {}).limit) },
          { label: "Has More", value: (data || {}).hasMore ? "yes" : "no" }
        ]),
        "<div class=\"control-callout\"><div><span class=\"sql-label\">Raw Detail</span><p class=\"control-copy\">Use SQL view for full raw detail when you need the full journal stream beyond the default page.</p></div></div>",
        (data || {}).hasMore ? "<div class=\"control-action-row\"><button class=\"ghost-button compact-button\" type=\"button\" data-action=\"loadMoreJournals\">Load More</button></div>" : ""
      ].join("")),
      card("Journal Stream", simpleTable([
        { label: "Time", render: function (row) { return escapeHtml(humanWhen(row.created_at)); } },
        { label: "Source", render: function (row) { return escapeHtml(row.source || "n/a"); } },
        { label: "Component", render: function (row) { return escapeHtml(row.component || "n/a"); } },
        { label: "Level", render: function (row) { return escapeHtml(row.level || "n/a"); } },
        { label: "Event", render: function (row) { return escapeHtml(row.event_type || "n/a"); } },
        { label: "Message", render: function (row) { return "<div class=\"control-wrap\">" + escapeHtml(row.message || "") + "</div>"; } }
      ], rows || []))
    ].join("");
  }

  function renderSettings(data) {
    return [
      "<form class=\"control-form\" id=\"settingsForm\">",
      "<div class=\"control-form-grid\">",
      checkboxField("Research Loop Enabled", "researchLoopEnabled", data.researchLoopEnabled),
      checkboxField("Engineering Loop Enabled", "engineeringLoopEnabled", data.engineeringLoopEnabled),
      field("Max Retries / Incident", "maxRetriesPerIncident", data.maxRetriesPerIncident, "number"),
      field("Max Next Jobs", "maxNextJobs", data.maxNextJobs, "number"),
      field("Max Patch Files", "maxPatchFiles", data.maxPatchFiles, "number"),
      field("Max Patch Line Changes", "maxPatchLineChanges", data.maxPatchLineChanges, "number"),
      field("Max Patch Bytes", "maxPatchBytes", data.maxPatchBytes, "number"),
      field("Restart Limit / Hour", "restartRateLimitPerHour", data.restartRateLimitPerHour, "number"),
      field("Failed Direction Stop Count", "failedDirectionStopCount", data.failedDirectionStopCount, "number"),
      field("Iteration Budget", "iterationBudget", data.iterationBudget, "number"),
      field("Approved Slice Ladder", "approvedSliceLadder", (data.approvedSliceLadder || []).join(",")),
      field("Research Model Override", "researchModelOverride", data.researchModelOverride || ""),
      field("Engineering Model Override", "engineeringModelOverride", data.engineeringModelOverride || ""),
      "</div>",
      "<div class=\"control-toggle-grid\">",
      familyToggleList(data.approvedCandidateFamilies || {}),
      "</div>",
      "<div class=\"control-action-row\"><button class=\"primary-button compact-button\" type=\"submit\">Save Settings</button></div>",
      "</form>"
    ].join("");
  }

  function familyToggleList(families) {
    return Object.keys(families).map(function (family) {
      const checked = families[family] ? " checked" : "";
      return "<label class=\"compact-check\"><input type=\"checkbox\" name=\"family:" + escapeHtml(family) + "\"" + checked + "><span>" + escapeHtml(family) + "</span></label>";
    }).join("");
  }

  function field(label, name, value, type) {
    return [
      "<label class=\"control-field\">",
      "<span>", escapeHtml(label), "</span>",
      "<input type=\"", escapeHtml(type || "text"), "\" name=\"", escapeHtml(name), "\" value=\"", escapeHtml(value == null ? "" : value), "\">",
      "</label>"
    ].join("");
  }

  function textareaField(label, name, value, rows) {
    return [
      "<label class=\"control-field control-field-wide\">",
      "<span>", escapeHtml(label), "</span>",
      "<textarea name=\"", escapeHtml(name), "\" rows=\"", escapeHtml(rows || 3), "\">", escapeHtml(value == null ? "" : value), "</textarea>",
      "</label>"
    ].join("");
  }

  function selectField(label, name, value, options) {
    return [
      "<label class=\"control-field\">",
      "<span>", escapeHtml(label), "</span>",
      "<select name=\"", escapeHtml(name), "\">",
      options.map(function (option) {
        const selected = String(option) === String(value) ? " selected" : "";
        return "<option value=\"" + escapeHtml(option) + "\"" + selected + ">" + escapeHtml(option) + "</option>";
      }).join(""),
      "</select></label>"
    ].join("");
  }

  function checkboxField(label, name, checked) {
    return [
      "<label class=\"compact-check\">",
      "<input type=\"checkbox\" name=\"", escapeHtml(name), "\"", checked ? " checked" : "", ">",
      "<span>", escapeHtml(label), "</span>",
      "</label>"
    ].join("");
  }

  async function loadOverview() {
    const overview = await fetchJson("/api/control/overview");
    state.overview = overview;
    state.research = state.research || {};
    state.incidents = state.incidents || {};
    elements.sidebarMeta.textContent = (overview.mission || {}).missionTitle || "Private control plane";
    return overview;
  }

  async function loadSection() {
    if (state.loading) {
      return;
    }
    state.loading = true;
    try {
      await loadOverview();
      const section = sections.find(function (item) { return item.key === state.currentSection; }) || sections[0];
      elements.pageEyebrow.textContent = section.label;
      elements.pageTitle.textContent = section.subtitle;
      if (state.currentSection === "overview") {
        elements.content.innerHTML = renderOverview(state.overview);
      } else if (state.currentSection === "mission") {
        state.mission = await fetchJson("/api/control/mission");
        elements.content.innerHTML = card("Mission Policy", renderMissionForm(state.mission));
      } else if (state.currentSection === "research") {
        const data = await Promise.all([
          fetchJson("/api/control/research/status"),
          fetchJson("/api/control/research/runs?limit=12"),
          fetchJson("/api/control/research/jobs?limit=40")
        ]);
        state.research = Object.assign({}, state.overview.research || {}, data[0]);
        state.research.runs = data[1];
        state.research.jobs = data[2];
        elements.content.innerHTML = renderResearch(state.research);
      } else if (state.currentSection === "incidents") {
        const data = await Promise.all([
          fetchJson("/api/control/incidents/current"),
          fetchJson("/api/control/incidents?limit=20")
        ]);
        state.incidents = { current: data[0], rows: data[1] };
        elements.content.innerHTML = renderIncidents(state.incidents);
      } else if (state.currentSection === "candidates") {
        state.candidates = await fetchJson("/api/control/candidates?" + new URLSearchParams(state.candidateFilters).toString());
        elements.content.innerHTML = renderCandidates(state.candidates);
      } else if (state.currentSection === "day-review") {
        const query = new URLSearchParams(cleanObject(state.dayReviewFilters));
        query.set("entryLimit", String(state.dayReviewLimit));
        state.dayReview = await fetchJson("/api/control/day-review?" + query.toString());
        elements.content.innerHTML = renderDayReview(state.dayReview);
      } else if (state.currentSection === "journals") {
        const query = new URLSearchParams(cleanObject(state.journalFilters));
        query.set("limit", String(state.journalLimit));
        state.journals = await fetchJson("/api/control/journals?" + query.toString());
        elements.content.innerHTML = renderJournals(state.journals);
      } else if (state.currentSection === "settings") {
        state.settings = await fetchJson("/api/control/settings");
        elements.content.innerHTML = card("Panel Settings", renderSettings(state.settings));
      }
      setStatus("Control data refreshed.", "success");
    } catch (error) {
      const message = error && error.detail ? error.detail : "Control API request failed.";
      elements.content.innerHTML = "<div class=\"panel control-loading\"><div class=\"sql-label\">Error</div><p class=\"sql-muted\">" + escapeHtml(message) + "</p></div>";
      setStatus(message, "error");
    } finally {
      state.loading = false;
    }
  }

  function cleanObject(payload) {
    const result = {};
    Object.keys(payload || {}).forEach(function (key) {
      if (payload[key] != null && String(payload[key]).trim() !== "") {
        result[key] = payload[key];
      }
    });
    return result;
  }

  async function postAction(url, body, successMessage) {
    const payload = await fetchJson(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {})
    });
    await loadSection();
    setStatus((payload && payload.message) || successMessage || "Action completed.", "success");
    return payload;
  }

  function navigate(sectionKey) {
    const section = sections.find(function (item) { return item.key === sectionKey; }) || sections[0];
    state.currentSection = section.key;
    history.pushState({}, "", section.path);
    renderNav();
    loadSection();
  }

  function actionReason(defaultText) {
    return { reason: defaultText };
  }

  async function handleShellAction(action) {
    if (action === "pauseResearch") {
      return postAction("/api/control/research/pause", actionReason("paused from control panel"), "Research loop paused.");
    }
    if (action === "resumeResearch") {
      return postAction("/api/control/research/resume", actionReason("resumed from control panel"), "Research loop resumed.");
    }
    if (action === "pauseEngineering") {
      return postAction("/api/control/engineering/pause", actionReason("paused from control panel"), "Engineering loop paused.");
    }
    if (action === "resumeEngineering") {
      return postAction("/api/control/engineering/resume", actionReason("resumed from control panel"), "Engineering loop resumed.");
    }
    if (action === "restartResearch") {
      return postAction("/api/control/research/restart", { services: [] }, "Research services restarted.");
    }
    if (action === "restartControl") {
      return postAction("/api/control/services/restart", { services: [] }, "Control services restarted.");
    }
    if (action === "requeueResearch") {
      return postAction("/api/control/research/requeue", { reason: "requeue latest failed research item" }, "Latest failed research job requeued.");
    }
    if (action === "runSmoke") {
      return postAction("/api/control/repair/run-smoke-tests", { tests: [] }, "Smoke tests started.");
    }
    if (action === "loadMoreJournals") {
      state.journalLimit += 20;
      return loadSection();
    }
    if (action === "loadMoreDayReview") {
      state.dayReviewLimit += 20;
      return loadSection();
    }
    if (action === "seedResearch") {
      return postAction("/api/control/research/seed-next", actionReason("seeded next job from control panel"), "Seeded next research job.");
    }
    if (action === "seedDivergenceSweep") {
      return postAction("/api/control/research/seed-divergence", actionReason("seeded divergence_sweep from control panel"), "Seeded divergence_sweep research job.");
    }
    if (action === "resetResearch") {
      return postAction("/api/control/research/reset", { mode: "soft", reason: "soft reset from control panel" }, "Research state reset.");
    }
    if (action === "retryIncident") {
      return postAction("/api/control/engineering/retry", actionReason("retry incident from control panel"), "Incident requeued for engineering retry.");
    }
    if (action === "manualTakeover") {
      return postAction("/api/control/engineering/manual-takeover", actionReason("manual takeover from control panel"), "Engineering loop marked as manual takeover.");
    }
    if (action === "ackIncident") {
      return postAction("/api/control/engineering/acknowledge", actionReason("incident acknowledged from control panel"), "Incident acknowledged.");
    }
  }

  elements.nav.addEventListener("click", function (event) {
    const button = event.target.closest("[data-section]");
    if (!button) {
      return;
    }
    navigate(button.dataset.section);
  });

  elements.refresh.addEventListener("click", function () {
    loadSection();
  });

  elements.content.addEventListener("click", function (event) {
    const button = event.target.closest("[data-action]");
    if (!button) {
      return;
    }
    handleShellAction(button.dataset.action).catch(function (error) {
      setStatus((error && error.detail) || "Control action failed.", "error");
    });
  });

  elements.content.addEventListener("submit", function (event) {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    event.preventDefault();
    if (form.id === "missionForm") {
      const payload = formToObject(form);
      payload.allowedDirections = splitTextarea(payload.allowedDirections);
      payload.forbiddenDirections = splitTextarea(payload.forbiddenDirections);
      payload.sameDayHoldoutRequired = Boolean(payload.sameDayHoldoutRequired);
      payload.priorDayValidationRequired = Boolean(payload.priorDayValidationRequired);
      fetchJson("/api/control/mission", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }).then(function () {
        loadSection().then(function () {
          setStatus("Mission policy saved.", "success");
        });
      }).catch(function (error) {
        setStatus((error && error.detail) || "Mission save failed.", "error");
      });
      return;
    }
    if (form.id === "settingsForm") {
      const payload = formToObject(form);
      payload.approvedSliceLadder = splitComma(payload.approvedSliceLadder).map(function (item) { return Number(item); }).filter(Number.isFinite);
      payload.approvedCandidateFamilies = {};
      Array.from(form.querySelectorAll("input[name^='family:']")).forEach(function (input) {
        payload.approvedCandidateFamilies[input.name.replace("family:", "")] = input.checked;
      });
      fetchJson("/api/control/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }).then(function () {
        loadSection().then(function () {
          setStatus("Settings saved.", "success");
        });
      }).catch(function (error) {
        setStatus((error && error.detail) || "Settings save failed.", "error");
      });
      return;
    }
    if (form.id === "candidateFilterForm") {
      state.candidateFilters = formToObject(form);
      loadSection();
      return;
    }
    if (form.dataset.role === "studyDayForm") {
      const payload = formToObject(form);
      fetchJson("/api/control/research/study-day", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }).then(function () {
        loadSection().then(function () {
          setStatus("Study day saved.", "success");
        });
      }).catch(function (error) {
        setStatus((error && error.detail) || "Study day save failed.", "error");
      });
      return;
    }
    if (form.id === "journalFilterForm") {
      state.journalFilters = formToObject(form);
      state.journalLimit = 20;
      loadSection();
      return;
    }
    if (form.id === "dayReviewForm") {
      state.dayReviewFilters = formToObject(form);
      state.dayReviewLimit = 20;
      loadSection();
      return;
    }
    if (form.classList.contains("control-candidate-form")) {
      const payload = formToObject(form);
      fetchJson("/api/control/candidates/" + encodeURIComponent(form.dataset.fingerprint), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      }).then(function () {
        loadSection().then(function () {
          setStatus("Candidate library state saved.", "success");
        });
      }).catch(function (error) {
        setStatus((error && error.detail) || "Candidate save failed.", "error");
      });
    }
  });

  window.addEventListener("popstate", function () {
    state.currentSection = sectionFromPath(location.pathname);
    renderNav();
    loadSection();
  });

  function formToObject(form) {
    const payload = {};
    Array.from(form.elements).forEach(function (element) {
      if (!element.name || element.disabled) {
        return;
      }
      if (element.type === "checkbox" && !element.name.startsWith("family:")) {
        payload[element.name] = element.checked;
        return;
      }
      payload[element.name] = element.value;
    });
    return payload;
  }

  function splitTextarea(value) {
    return String(value || "")
      .split(/\n+/)
      .map(function (item) { return item.trim(); })
      .filter(Boolean);
  }

  function splitComma(value) {
    return String(value || "")
      .split(",")
      .map(function (item) { return item.trim(); })
      .filter(Boolean);
  }

  function startPolling() {
    stopPolling();
    state.pollTimer = window.setInterval(function () {
      if (state.currentSection === "mission" || state.currentSection === "settings") {
        loadOverview().catch(function () {
          setStatus("Background refresh failed.", "error");
        });
        return;
      }
      loadSection().catch(function () {
        setStatus("Background refresh failed.", "error");
      });
    }, 15000);
  }

  function stopPolling() {
    if (state.pollTimer) {
      window.clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
  }

  renderNav();
  loadSection().catch(function (error) {
    setStatus((error && error.detail) || "Failed to initialize control panel.", "error");
  });
  startPolling();
}());
