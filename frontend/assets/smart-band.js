(function (root) {
  "use strict";

  // Browser port of HLRTACD smart-band-causal-v1-2026-09-19.
  // Each quote is checked against the previous outer band before updating it.
  const CONFIG = Object.freeze({
    fastSeconds: 8, slowSeconds: 55, residualStride: 4, residualWindow: 240,
    quantileRefreshTicks: 40, trendSeconds: 45, minimumBandDollars: .12,
    gapResetSeconds: 15 * 60, testMinTicks: 8, testMinSeconds: 3,
  });

  function quantile(sorted, fraction) {
    const position = (sorted.length - 1) * fraction;
    const lower = Math.floor(position), upper = Math.ceil(position);
    return sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower);
  }

  class SmartBandTracker {
    constructor(maxStored = 12000) {
      this.maxStored = maxStored;
      this.points = [];
      this.residuals = [];
      this.recentCenters = [];
      this.offsets = [-.24, -.12, .12, .24];
      this.count = 0;
      this.segment = -1;
      this.lastId = null;
      this.lastTimestampMs = null;
      this.fast = null;
      this.slow = null;
      this.testSide = null;
      this.testStartMs = null;
      this.testTicks = 0;
      this.boundaryState = "INSIDE";
      this.lastRejectionMs = null;
      this.lastRejectionSide = null;
      this.phaseState = "WARMUP";
      this.pendingPhase = null;
      this.pendingPhaseTicks = 0;
    }

    center() { return .35 * this.fast + .65 * this.slow; }

    keep(point) {
      this.points.push(point);
      if (this.points.length > this.maxStored) this.points.splice(0, this.points.length - this.maxStored);
      this.count += 1;
      this.lastId = point.tickId;
      this.lastTimestampMs = point.timestampMs;
      return point;
    }

    reset(tickId, timestampMs, mid, spread) {
      this.segment += 1;
      this.fast = mid;
      this.slow = mid;
      this.residuals = [];
      this.recentCenters = [[timestampMs, mid]];
      this.testSide = null;
      this.testStartMs = null;
      this.testTicks = 0;
      this.boundaryState = "INSIDE";
      this.phaseState = "WARMUP";
      this.pendingPhase = null;
      this.pendingPhaseTicks = 0;
      const floor = Math.max(CONFIG.minimumBandDollars, spread);
      this.offsets = [-2 * floor, -floor, floor, 2 * floor];
      return this.keep({tickId, timestampMs, segment: this.segment, center: mid,
        innerLower: mid - floor, innerUpper: mid + floor,
        outerLower: mid - 2 * floor, outerUpper: mid + 2 * floor,
        phase: "WARMUP", boundaryState: "INSIDE", trendStrength: 0, width: 4 * floor});
    }

    observeBoundary(mid, timestampMs, lower, upper) {
      const side = mid > upper ? "UP" : mid < lower ? "DOWN" : null;
      if (side == null) {
        if (this.testSide && this.boundaryState.startsWith("TESTING")) {
          this.lastRejectionMs = timestampMs;
          this.lastRejectionSide = this.testSide;
        }
        this.testSide = null;
        this.testStartMs = null;
        this.testTicks = 0;
        this.boundaryState = this.lastRejectionMs != null && timestampMs - this.lastRejectionMs <= 8000
          ? "REJECTED_" + this.lastRejectionSide : "INSIDE";
        return;
      }
      if (side !== this.testSide) {
        this.testSide = side;
        this.testStartMs = timestampMs;
        this.testTicks = 1;
      } else {
        this.testTicks += 1;
      }
      const duration = (timestampMs - this.testStartMs) / 1000;
      this.boundaryState = (this.testTicks >= CONFIG.testMinTicks && duration >= CONFIG.testMinSeconds
        ? "ACCEPTED_" : "TESTING_") + side;
    }

    refreshOffsets(spread) {
      const sorted = this.residuals.slice().sort((a, b) => a - b);
      const desired = [
        Math.min(-2 * Math.max(CONFIG.minimumBandDollars, spread), quantile(sorted, .04)),
        Math.min(-Math.max(CONFIG.minimumBandDollars, spread), quantile(sorted, .20)),
        Math.max(Math.max(CONFIG.minimumBandDollars, spread), quantile(sorted, .80)),
        Math.max(2 * Math.max(CONFIG.minimumBandDollars, spread), quantile(sorted, .96)),
      ];
      this.offsets = this.offsets.map((previous, index) => {
        const target = desired[index];
        return previous + (Math.abs(target) > Math.abs(previous) ? .35 : .12) * (target - previous);
      });
    }

    stablePhase(observed) {
      if (observed === this.phaseState) {
        this.pendingPhase = null;
        this.pendingPhaseTicks = 0;
        return this.phaseState;
      }
      if (observed !== this.pendingPhase) {
        this.pendingPhase = observed;
        this.pendingPhaseTicks = 1;
      } else {
        this.pendingPhaseTicks += 1;
      }
      if (this.pendingPhaseTicks >= 10) {
        this.phaseState = observed;
        this.pendingPhase = null;
        this.pendingPhaseTicks = 0;
      }
      return this.phaseState;
    }

    process(row) {
      const tickId = Number(row.id ?? row.tickId);
      const timestampMs = row.timestampMs != null && Number.isFinite(Number(row.timestampMs))
        ? Number(row.timestampMs) : Date.parse(row.timestamp);
      const mid = Number(row.mid), bid = Number(row.bid), ask = Number(row.ask);
      if (![tickId, timestampMs, mid].every(Number.isFinite)) return null;
      if (this.lastId != null && tickId <= this.lastId) return null;
      const spread = Number.isFinite(bid) && Number.isFinite(ask) ? Math.max(0, ask - bid) : 0;
      const elapsed = this.lastTimestampMs == null ? null : (timestampMs - this.lastTimestampMs) / 1000;
      if (this.fast == null || elapsed == null || elapsed >= CONFIG.gapResetSeconds || elapsed < 0)
        return this.reset(tickId, timestampMs, mid, spread);

      const dt = Math.max(0, Math.min(elapsed, 10));
      const previousCenter = this.center();
      this.observeBoundary(mid, timestampMs, previousCenter + this.offsets[0], previousCenter + this.offsets[3]);
      const innovation = mid - previousCenter;
      const limit = Math.max(.30, 3 * (this.offsets[3] - this.offsets[0]), 2 * spread);
      const boundedPrice = previousCenter + Math.max(-limit, Math.min(limit, innovation));
      this.fast += (1 - Math.exp(-dt / CONFIG.fastSeconds)) * (boundedPrice - this.fast);
      this.slow += (1 - Math.exp(-dt / CONFIG.slowSeconds)) * (boundedPrice - this.slow);
      const center = this.center();
      const count = this.count + 1;
      if (count % CONFIG.residualStride === 0) {
        this.residuals.push(mid - center);
        if (this.residuals.length > CONFIG.residualWindow) this.residuals.shift();
      }
      if (count % CONFIG.quantileRefreshTicks === 0 && this.residuals.length >= 20)
        this.refreshOffsets(spread);

      this.recentCenters.push([timestampMs, center]);
      while (this.recentCenters.length > 1
        && timestampMs - this.recentCenters[1][0] > CONFIG.trendSeconds * 1000)
        this.recentCenters.shift();
      const width = Math.max(CONFIG.minimumBandDollars * 2, this.offsets[3] - this.offsets[0]);
      const strength = (center - this.recentCenters[0][1]) / width;
      const observed = strength >= .30 ? "RISING" : strength <= -.30 ? "FALLING"
        : Math.abs(strength) <= .10 ? "RANGING" : "TRANSITION";
      const phase = this.stablePhase(observed);
      return this.keep({tickId, timestampMs, segment: this.segment, center,
        innerLower: center + this.offsets[1], innerUpper: center + this.offsets[2],
        outerLower: center + this.offsets[0], outerUpper: center + this.offsets[3],
        phase, boundaryState: this.boundaryState, trendStrength: strength, width});
    }

    visible(firstId, lastId) {
      return this.points.filter((point) => point.tickId >= firstId && point.tickId <= lastId);
    }
  }

  const api = {SmartBandTracker, sourceVersion: "smart-band-causal-v1-2026-09-19"};
  root.DatavisSmartBand = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
