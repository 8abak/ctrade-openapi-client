(function () {
  const LIMIT = 40;
  const POLL_MS = 3000;

  function $(id) {
    return document.getElementById(id);
  }

  function fmtNum(value, digits = 2) {
    const n = Number(value);
    return Number.isFinite(n) ? n.toFixed(digits) : "-";
  }

  function fmtTime(value) {
    if (!value) return "-";
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) return String(value);
    return dt.toISOString().replace("T", " ").slice(0, 19);
  }

  function pill(label, tone) {
    return `<span class="pill ${tone || ""}">${label}</span>`;
  }

  function renderRows(targetId, rows, emptyText, mapper, colspan) {
    const body = $(targetId);
    if (!body) return;
    if (!Array.isArray(rows) || !rows.length) {
      body.innerHTML = `<tr><td colspan="${colspan || 1}" class="muted">${emptyText}</td></tr>`;
      return;
    }
    body.innerHTML = rows.map(mapper).join("");
  }

  function render(data) {
    const state = data && data.state ? data.state : null;
    const trade = data && data.opentrade ? data.opentrade : null;
    const candidates = Array.isArray(data && data.candidates) ? data.candidates : [];
    const outcomes = Array.isArray(data && data.outcomes) ? data.outcomes : [];
    const trades = Array.isArray(data && data.trades) ? data.trades : [];
    const stats = data && data.candidatestats ? data.candidatestats : null;

    $("statusline").textContent = state
      ? `Last processed tick ${state.tickid || "-"} | mode ${state.mode || "-"} | status ${state.status || "-"} | updated ${fmtTime(state.updated)}`
      : "UNITY state row not found yet.";

    $("state-value").textContent = state ? `${state.mode || "live"} / ${state.status || "idle"}` : "offline";
    $("state-sub").textContent = state
      ? `State row updated ${fmtTime(state.updated)}`
      : "Run the UNITY live service after the migration.";

    $("candidate-total").textContent = stats && stats.total != null ? String(stats.total) : "0";
    $("candidate-sub").textContent = stats
      ? `eligible ${stats.eligible || 0} | opened ${stats.tradeopened || 0} | favored ${stats.favored || 0}`
      : "No candidate stats yet.";

    $("trade-value").textContent = trade ? `${trade.side || "-"} @ ${fmtNum(trade.openprice)}` : "none";
    $("trade-sub").textContent = trade
      ? `tick ${trade.opentick || "-"} | stop ${fmtNum(trade.stopprice)} | target ${fmtNum(trade.targetprice)}`
      : "No open paper trade.";

    const latest = candidates[0] || null;
    $("latest-value").textContent = latest ? `${latest.side || "-"} ${latest.regimeto || "-"}` : "-";
    $("latest-sub").textContent = latest
      ? `tick ${latest.signaltickid || "-"} | score ${fmtNum(latest.score)} | ${latest.signalstatus || "-"}`
      : "No candidates yet.";

    renderRows("candidate-body", candidates, "No candidates yet.", (row) => {
      const sideTone = row.side === "long" ? "green" : "red";
      const elig = row.eligible ? pill("eligible", "green") : pill(row.eligibilityreason || "ineligible", "red");
      const statusTone = row.signalstatus === "opened" ? "green" : row.signalstatus === "skipped" ? "gold" : "";
      const tradePill = row.tradeopened ? pill("opened", "green") : pill("shadow", "gold");
      return `
        <tr>
          <td>${row.signaltickid || "-"}</td>
          <td>${fmtTime(row.time)}</td>
          <td>${pill(row.side || "-", sideTone)}</td>
          <td>${fmtNum(row.score)}</td>
          <td>${elig}</td>
          <td>${pill(row.signalstatus || "-", statusTone)}</td>
          <td>${tradePill}</td>
          <td title="${row.reason || ""}">${row.reason || "-"}</td>
        </tr>
      `;
    }, 8);

    renderRows("outcome-body", outcomes, "No outcomes yet.", (row) => {
      const firstHit = row.firsthit === "tp" ? pill("tp", "green") : row.firsthit === "sl" ? pill("sl", "red") : pill(row.firsthit || "-", "gold");
      return `
        <tr>
          <td>${row.candidateid || "-"}</td>
          <td>${row.status || "-"}</td>
          <td>${firstHit}</td>
          <td>${fmtNum(row.pnl)}</td>
          <td>${row.resolveseconds != null ? row.resolveseconds : "-"}</td>
        </tr>
      `;
    }, 5);

    renderRows("trade-body", trades, "No paper trades yet.", (row) => {
      const tone = row.side === "long" ? "green" : "red";
      return `
        <tr>
          <td>${row.opentick || "-"}</td>
          <td>${pill(row.side || "-", tone)}</td>
          <td>${row.status || "-"}</td>
          <td>${fmtNum(row.pnl)}</td>
          <td>${row.exitreason || "-"}</td>
        </tr>
      `;
    }, 5);
  }

  async function poll() {
    try {
      const res = await fetch(`/api/unity/recent?symbol=XAUUSD&limit=${LIMIT}`);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const data = await res.json();
      render(data);
    } catch (err) {
      $("statusline").textContent = `UNITY dashboard load failed: ${err.message}`;
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    poll();
    setInterval(poll, POLL_MS);
  });
})();
