(function () {
  if (window.DatavisCharting) {
    return;
  }

  const DEFAULT_AXIS_LABEL_COLOR = "#9eadc5";
  const DEFAULT_SPLIT_LINE_COLOR = "rgba(147,181,255,0.10)";
  const DEBUG_FLAG = "__DATAVIS_CHART_DEBUG__";

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function normalizeOrderedValues(values) {
    const normalized = [];
    let lastValue = null;
    values.forEach(function (value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return;
      }
      if (!normalized.length || numeric !== lastValue) {
        normalized.push(numeric);
        lastValue = numeric;
      }
    });
    return normalized;
  }

  function lowerBound(values, target) {
    let low = 0;
    let high = values.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (values[middle] < target) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  function upperBound(values, target) {
    let low = 0;
    let high = values.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (values[middle] <= target) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  function debugEnabled() {
    try {
      if (typeof window === "undefined") {
        return false;
      }
      if (Boolean(window[DEBUG_FLAG])) {
        return true;
      }
      return window.localStorage?.getItem(DEBUG_FLAG) === "1";
    } catch (error) {
      return false;
    }
  }

  function debugLog() {
    if (!debugEnabled() || typeof console === "undefined" || typeof console.log !== "function") {
      return;
    }
    console.log.apply(console, arguments);
  }

  function coerceFiniteNumber(value) {
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index += 1) {
        const nested = coerceFiniteNumber(value[index]);
        if (Number.isFinite(nested)) {
          return nested;
        }
      }
      return NaN;
    }
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : NaN;
  }

  function normalizeZoomCandidate(candidate) {
    if (!candidate || typeof candidate !== "object") {
      return null;
    }
    const start = coerceFiniteNumber(candidate.start);
    const end = coerceFiniteNumber(candidate.end);
    const startValue = coerceFiniteNumber(candidate.startValue);
    const endValue = coerceFiniteNumber(candidate.endValue);
    if (![start, end, startValue, endValue].some(Number.isFinite)) {
      return null;
    }
    return {
      start: start,
      end: end,
      startValue: startValue,
      endValue: endValue,
    };
  }

  function zoomCandidateScore(candidate) {
    if (!candidate) {
      return -1;
    }
    let score = 0;
    if (Number.isFinite(candidate.startValue) && Number.isFinite(candidate.endValue)) {
      score += 4;
    }
    if (Number.isFinite(candidate.start) && Number.isFinite(candidate.end)) {
      score += 2;
      if (Math.abs(candidate.end - candidate.start) < 99.999) {
        score += 1;
      }
    }
    return score;
  }

  function chooseBestZoomCandidate(candidates) {
    let best = null;
    let bestScore = -1;
    candidates.forEach(function (candidate) {
      const normalized = normalizeZoomCandidate(candidate);
      const score = zoomCandidateScore(normalized);
      if (score > bestScore) {
        best = normalized;
        bestScore = score;
      }
    });
    return best;
  }

  function readChartDataZoom(chart, eventPayload) {
    const eventCandidates = [];
    if (eventPayload && typeof eventPayload === "object") {
      if (Array.isArray(eventPayload.batch)) {
        eventPayload.batch.forEach(function (entry) {
          eventCandidates.push(entry);
        });
      }
      eventCandidates.push(eventPayload);
    }
    const eventChoice = chooseBestZoomCandidate(eventCandidates);
    if (eventChoice) {
      return eventChoice;
    }
    const candidates = [];
    const optionEntries = chart?.getOption?.()?.dataZoom;
    if (Array.isArray(optionEntries)) {
      optionEntries.forEach(function (entry) {
        candidates.push(entry);
      });
    }
    return chooseBestZoomCandidate(candidates);
  }

  function resolveRangeFromZoom(zoom, values) {
    if (!zoom || !values.length) {
      return null;
    }
    const domainStart = Number(values[0]);
    const domainEnd = Number(values[values.length - 1]);
    let startValue = coerceFiniteNumber(zoom.startValue);
    let endValue = coerceFiniteNumber(zoom.endValue);
    if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) {
      const startPercent = Number.isFinite(coerceFiniteNumber(zoom.start)) ? coerceFiniteNumber(zoom.start) / 100 : 0;
      const endPercent = Number.isFinite(coerceFiniteNumber(zoom.end)) ? coerceFiniteNumber(zoom.end) / 100 : 1;
      const span = domainEnd - domainStart;
      startValue = domainStart + (span * clamp(startPercent, 0, 1));
      endValue = domainStart + (span * clamp(endPercent, 0, 1));
    }
    if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) {
      return null;
    }
    const minValue = Math.min(startValue, endValue);
    const maxValue = Math.max(startValue, endValue);
    let startIndex = lowerBound(values, minValue);
    let endIndex = upperBound(values, maxValue) - 1;
    startIndex = clamp(startIndex, 0, values.length - 1);
    endIndex = clamp(endIndex, 0, values.length - 1);
    if (endIndex < startIndex) {
      endIndex = startIndex;
    }
    return {
      startIndex: startIndex,
      endIndex: endIndex,
      startValue: values[startIndex],
      endValue: values[endIndex],
      visibleCount: (endIndex - startIndex) + 1,
    };
  }

  function createViewportModel(options) {
    const toleranceItems = clamp(Math.round(Number(options?.rightEdgeToleranceItems ?? 1)), 0, 32);
    const debugName = String(options?.debugName || "chart");
    const model = {
      startIndex: 0,
      endIndex: 0,
      startValue: null,
      endValue: null,
      visibleCount: null,
      followRightEdge: true,
      initialized: false,
      userHasInteracted: false,
      applyingProgrammaticViewport: false,
      datasetLength: 0,
    };

    function log() {
      if (!debugEnabled()) {
        return;
      }
      const args = Array.from(arguments);
      args.unshift("[chart viewport][" + debugName + "]");
      debugLog.apply(null, args);
    }

    function applyWindow(windowState) {
      model.startIndex = windowState.startIndex;
      model.endIndex = windowState.endIndex;
      model.startValue = windowState.startValue;
      model.endValue = windowState.endValue;
      model.visibleCount = windowState.visibleCount;
      model.followRightEdge = Boolean(windowState.followRightEdge);
      model.userHasInteracted = Boolean(windowState.userHasInteracted);
      model.applyingProgrammaticViewport = Boolean(windowState.applyingProgrammaticViewport);
      model.initialized = true;
      model.datasetLength = Number(windowState.datasetLength || model.datasetLength || 0);
      return snapshot();
    }

    function stateFromIndices(values, startIndex, endIndex, overrides) {
      const resolvedStartIndex = clamp(Math.round(Number(startIndex || 0)), 0, values.length - 1);
      const resolvedEndIndex = clamp(Math.round(Number(endIndex || resolvedStartIndex)), resolvedStartIndex, values.length - 1);
      return {
        startIndex: resolvedStartIndex,
        endIndex: resolvedEndIndex,
        startValue: values[resolvedStartIndex],
        endValue: values[resolvedEndIndex],
        visibleCount: (resolvedEndIndex - resolvedStartIndex) + 1,
        followRightEdge: Boolean(overrides?.followRightEdge),
        userHasInteracted: Boolean(overrides?.userHasInteracted),
        applyingProgrammaticViewport: Boolean(overrides?.applyingProgrammaticViewport),
        datasetLength: values.length,
      };
    }

    function defaultWindow(values) {
      return stateFromIndices(values, 0, values.length - 1, {
        followRightEdge: true,
        userHasInteracted: false,
      });
    }

    function latestWindow(values, visibleCount, overrides) {
      const endIndex = values.length - 1;
      const startIndex = Math.max(0, endIndex - clamp(Number(visibleCount || values.length), 1, values.length) + 1);
      return stateFromIndices(values, startIndex, endIndex, {
        followRightEdge: true,
        userHasInteracted: Boolean(overrides?.userHasInteracted),
        applyingProgrammaticViewport: Boolean(overrides?.applyingProgrammaticViewport),
      });
    }

    function historyWindow(values, startIndex, visibleCount, overrides) {
      const boundedVisibleCount = clamp(Number(visibleCount || values.length), 1, values.length);
      let resolvedStartIndex = clamp(Math.round(Number(startIndex || 0)), 0, values.length - 1);
      let resolvedEndIndex = Math.min(values.length - 1, resolvedStartIndex + boundedVisibleCount - 1);
      if ((resolvedEndIndex - resolvedStartIndex + 1) < boundedVisibleCount) {
        resolvedStartIndex = Math.max(0, resolvedEndIndex - boundedVisibleCount + 1);
      }
      return stateFromIndices(values, resolvedStartIndex, resolvedEndIndex, {
        followRightEdge: false,
        userHasInteracted: Boolean(overrides?.userHasInteracted),
        applyingProgrammaticViewport: Boolean(overrides?.applyingProgrammaticViewport),
      });
    }

    function loudFallback(reason, values) {
      log("FULL-RANGE FALLBACK", reason, {
        datasetLength: values.length,
        viewport: snapshot(),
      });
      return {
        reason: reason,
        state: defaultWindow(values),
      };
    }

    function snapshot() {
      return {
        startIndex: model.startIndex,
        endIndex: model.endIndex,
        startValue: model.startValue,
        endValue: model.endValue,
        visibleCount: model.visibleCount,
        followRightEdge: model.followRightEdge,
        initialized: model.initialized,
        userHasInteracted: model.userHasInteracted,
        applyingProgrammaticViewport: model.applyingProgrammaticViewport,
        datasetLength: model.datasetLength,
      };
    }

    function reset() {
      model.startIndex = 0;
      model.endIndex = 0;
      model.startValue = null;
      model.endValue = null;
      model.visibleCount = null;
      model.followRightEdge = true;
      model.initialized = false;
      model.userHasInteracted = false;
      model.applyingProgrammaticViewport = false;
      model.datasetLength = 0;
    }

    function currentWindow() {
      return model.initialized ? snapshot() : null;
    }

    function setApplyingProgrammaticViewport(active) {
      model.applyingProgrammaticViewport = Boolean(active);
      return snapshot();
    }

    function initialize(xValues, options) {
      const values = normalizeOrderedValues(xValues || []);
      if (!values.length) {
        reset();
        return null;
      }
      const state = defaultWindow(values);
      log("initialize", {
        datasetLength: values.length,
        viewport: state,
      });
      return applyWindow({
        ...state,
        applyingProgrammaticViewport: Boolean(options?.applyingProgrammaticViewport),
      });
    }

    function projectWindow(xValues, options) {
      const values = normalizeOrderedValues(xValues || []);
      if (!values.length) {
        reset();
        return null;
      }
      if (options?.reset || !model.initialized) {
        const initializedState = defaultWindow(values);
        log("before update", {
          datasetLength: values.length,
          viewport: snapshot(),
          reason: options?.reset ? "reset" : "initialize",
        });
        return applyWindow({
          ...initializedState,
          applyingProgrammaticViewport: Boolean(options?.applyingProgrammaticViewport),
        });
      }
      const visibleCount = clamp(Number(model.visibleCount) || Number(model.endIndex - model.startIndex + 1) || values.length, 1, values.length);
      log("before update", {
        datasetLength: values.length,
        viewport: snapshot(),
        updateMeta: options?.updateMeta || null,
      });
      if (model.followRightEdge) {
        const state = latestWindow(values, visibleCount, {
          userHasInteracted: model.userHasInteracted,
          applyingProgrammaticViewport: Boolean(options?.applyingProgrammaticViewport),
        });
        log("after incoming data update", {
          path: "follow-right-edge",
          startIndex: state.startIndex,
          endIndex: state.endIndex,
          visibleCount: state.visibleCount,
          followRightEdge: state.followRightEdge,
        });
        return applyWindow(state);
      }
      let nextStartIndex = Number(model.startIndex);
      const prependedCount = Number(options?.updateMeta?.prependedCount || 0);
      const droppedFromStart = Number(options?.updateMeta?.droppedFromStart || 0);
      if (prependedCount || droppedFromStart) {
        nextStartIndex = nextStartIndex + prependedCount - droppedFromStart;
      } else if (Number.isFinite(Number(model.startValue))) {
        nextStartIndex = lowerBound(values, Number(model.startValue));
      }
      if (!Number.isFinite(nextStartIndex)) {
        const fallback = loudFallback("invalid history start index during projectWindow", values);
        return applyWindow({
          ...fallback.state,
          userHasInteracted: model.userHasInteracted,
          applyingProgrammaticViewport: Boolean(options?.applyingProgrammaticViewport),
        });
      }
      const state = historyWindow(values, nextStartIndex, visibleCount, {
        userHasInteracted: model.userHasInteracted,
        applyingProgrammaticViewport: Boolean(options?.applyingProgrammaticViewport),
      });
      log("after incoming data update", {
        path: "preserve-history",
        startIndex: state.startIndex,
        endIndex: state.endIndex,
        visibleCount: state.visibleCount,
        followRightEdge: state.followRightEdge,
        prependedCount: prependedCount,
        droppedFromStart: droppedFromStart,
      });
      return applyWindow(state);
    }

    function captureZoom(zoom, xValues) {
      const values = normalizeOrderedValues(xValues || []);
      if (!values.length) {
        reset();
        return null;
      }
      const range = resolveRangeFromZoom(zoom, values);
      if (!range) {
        if (model.initialized) {
          log("user datazoom ignored because zoom range could not be resolved", {
            zoom: zoom,
            viewport: snapshot(),
            datasetLength: values.length,
          });
          return snapshot();
        }
        const fallback = loudFallback("captureZoom had no valid zoom range and no prior viewport", values);
        return applyWindow(fallback.state);
      }
      const state = stateFromIndices(values, range.startIndex, range.endIndex, {
        followRightEdge: range.endIndex >= (values.length - 1 - toleranceItems),
        userHasInteracted: true,
        applyingProgrammaticViewport: false,
      });
      log("after user datazoom", {
        startIndex: state.startIndex,
        endIndex: state.endIndex,
        visibleCount: state.visibleCount,
        followRightEdge: state.followRightEdge,
      });
      return applyWindow(state);
    }

    return {
      reset: reset,
      initialize: initialize,
      captureZoom: captureZoom,
      currentWindow: currentWindow,
      projectWindow: projectWindow,
      setApplyingProgrammaticViewport: setApplyingProgrammaticViewport,
      ensureWindow: projectWindow,
      visibleRange: function (xValues, options) {
        const windowState = projectWindow(xValues, options);
        return windowState
          ? { min: windowState.startValue, max: windowState.endValue }
          : null;
      },
      zoomOptions: function (xValues, options) {
        const windowState = projectWindow(xValues, options);
        return windowState
          ? { startValue: windowState.startValue, endValue: windowState.endValue }
          : {};
      },
      snapshot: snapshot,
    };
  }

  function rangeItem(xStart, xEnd, yMin, yMax) {
    const resolvedXStart = Number(xStart);
    const resolvedXEnd = Number(xEnd);
    const resolvedYMin = Number(yMin);
    const resolvedYMax = Number(yMax);
    if (
      !Number.isFinite(resolvedXStart)
      || !Number.isFinite(resolvedXEnd)
      || !Number.isFinite(resolvedYMin)
      || !Number.isFinite(resolvedYMax)
    ) {
      return null;
    }
    return {
      xStart: Math.min(resolvedXStart, resolvedXEnd),
      xEnd: Math.max(resolvedXStart, resolvedXEnd),
      yMin: Math.min(resolvedYMin, resolvedYMax),
      yMax: Math.max(resolvedYMin, resolvedYMax),
    };
  }

  function pointItem(x, y) {
    return rangeItem(x, x, y, y);
  }

  function collectExtents(items, visibleRange) {
    let min = Infinity;
    let max = -Infinity;
    (items || []).forEach(function (item) {
      if (!item) {
        return;
      }
      if (
        visibleRange
        && (item.xEnd < visibleRange.min || item.xStart > visibleRange.max)
      ) {
        return;
      }
      min = Math.min(min, item.yMin);
      max = Math.max(max, item.yMax);
    });
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return null;
    }
    return { min: min, max: max };
  }

  function niceIntegerStep(minValue, maxValue, targetTickCount) {
    const span = Math.max(1, Math.abs(Number(maxValue) - Number(minValue)));
    const rawStep = Math.max(1, span / Math.max(1, targetTickCount - 1));
    const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
    const normalized = rawStep / magnitude;
    let step = 1;
    if (normalized <= 1) {
      step = 1;
    } else if (normalized <= 2) {
      step = 2;
    } else if (normalized <= 5) {
      step = 5;
    } else {
      step = 10;
    }
    return Math.max(1, Math.round(step * magnitude));
  }

  function buildIntegerYAxis(minValue, maxValue, options) {
    const axisLabelColor = options?.axisLabelColor || DEFAULT_AXIS_LABEL_COLOR;
    const splitLineColor = options?.splitLineColor || DEFAULT_SPLIT_LINE_COLOR;
    const targetTickCount = clamp(Math.round(Number(options?.targetTickCount ?? 6)), 2, 10);
    const baseAxis = {
      type: "value",
      scale: true,
      axisLabel: {
        color: axisLabelColor,
        formatter: function (value) {
          return String(Math.round(Number(value)));
        },
      },
      splitLine: { lineStyle: { color: splitLineColor } },
    };
    if (!Number.isFinite(Number(minValue)) || !Number.isFinite(Number(maxValue))) {
      return baseAxis;
    }
    const low = Math.min(Number(minValue), Number(maxValue));
    const high = Math.max(Number(minValue), Number(maxValue));
    const step = niceIntegerStep(low, high, targetTickCount);
    let snappedMin = Math.floor(low / step) * step;
    let snappedMax = Math.ceil(high / step) * step;
    if (snappedMax <= snappedMin) {
      snappedMax = snappedMin + step;
    }
    return {
      ...baseAxis,
      min: snappedMin,
      max: snappedMax,
      interval: step,
      minInterval: 1,
    };
  }

  function buildVisibleIntegerYAxis(options) {
    const coreItems = options?.coreItems || [];
    const overlayItems = options?.overlayItems || [];
    const visibleRange = options?.visibleRange || null;
    const selectedItems = options?.includeOverlays ? coreItems.concat(overlayItems) : coreItems;
    let extents = collectExtents(selectedItems, visibleRange);
    if (!extents && !selectedItems.length && overlayItems.length) {
      extents = collectExtents(overlayItems, visibleRange);
    }
    if (!extents) {
      extents = collectExtents(selectedItems.length ? selectedItems : coreItems.concat(overlayItems), null);
    }
    return buildIntegerYAxis(extents?.min, extents?.max, options);
  }

  window.DatavisCharting = {
    createViewportModel: createViewportModel,
    readChartDataZoom: readChartDataZoom,
    pointItem: pointItem,
    rangeItem: rangeItem,
    buildIntegerYAxis: buildIntegerYAxis,
    buildVisibleIntegerYAxis: buildVisibleIntegerYAxis,
  };
}());
