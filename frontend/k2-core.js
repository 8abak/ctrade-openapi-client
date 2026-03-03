// frontend/k2-core.js
// K2 candles page controller. Chart behavior stays in chart-core.js.

(function () {
  const POLL_MS = 3000;
  let pollTimer = null;

  function $(id) {
    return document.getElementById(id);
  }

  function setStatus(msg) {
    const el = $("k2-status");
    if (el) el.textContent = msg || "";
  }

  function readInputs() {
    const symbol = (($("k2-symbol") && $("k2-symbol").value) || "XAUUSD").trim() || "XAUUSD";
    const limitRaw = $("k2-limit") ? Number($("k2-limit").value) : 500;
    const limit = Math.max(1, Math.min(5000, Number.isFinite(limitRaw) ? limitRaw : 500));

    const fromRaw = $("k2-from-id") ? $("k2-from-id").value.trim() : "";
    const parsedFrom = fromRaw ? Number(fromRaw) : null;
    const fromId = Number.isFinite(parsedFrom) && parsedFrom > 0 ? Math.floor(parsedFrom) : null;
    return { symbol, limit, fromId };
  }

  async function refreshNow() {
    const { symbol, limit, fromId } = readInputs();
    const q =
      `symbol=${encodeURIComponent(symbol)}` +
      `&limit=${encodeURIComponent(limit)}` +
      (fromId != null ? `&from_id=${encodeURIComponent(fromId)}` : "");

    setStatus("Loading...");
    try {
      const res = await fetch(`/api/k2candles/window?${q}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const candles = Array.isArray(data.candles) ? data.candles : [];
      ChartCore.setK2Candles(candles);

      let msg = `symbol=${data.symbol || symbol} candles=${candles.length}`;
      if (candles.length) {
        msg += ` [id ${candles[0].id}..${candles[candles.length - 1].id}]`;
      }
      setStatus(msg);
    } catch (err) {
      console.error("k2-core refresh failed", err);
      setStatus(`Load failed: ${err.message || err}`);
    }
  }

  function syncPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
    const enabled = $("k2-poll") ? $("k2-poll").checked : false;
    if (!enabled) return;
    pollTimer = setInterval(() => {
      refreshNow().catch(() => {});
    }, POLL_MS);
  }

  function wireUi() {
    const refreshBtn = $("k2-refresh");
    const pollToggle = $("k2-poll");
    const fromInput = $("k2-from-id");
    const symbolInput = $("k2-symbol");
    const limitInput = $("k2-limit");

    if (refreshBtn) {
      refreshBtn.addEventListener("click", () => {
        refreshNow();
      });
    }
    if (pollToggle) {
      pollToggle.addEventListener("change", () => {
        syncPolling();
      });
    }
    if (fromInput) {
      fromInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") refreshNow();
      });
    }
    if (symbolInput) {
      symbolInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") refreshNow();
      });
    }
    if (limitInput) {
      limitInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") refreshNow();
      });
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    ChartCore.init("k2-chart");
    wireUi();
    syncPolling();
    refreshNow();
  });
})();
