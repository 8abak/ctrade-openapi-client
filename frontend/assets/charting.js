(function () {
  if (window.DatavisCharting) {
    return;
  }

  const DEFAULT_AXIS_LABEL_COLOR = "#9eadc5";
  const DEFAULT_SPLIT_LINE_COLOR = "rgba(147,181,255,0.10)";

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

  function resolveRangeFromZoom(zoom, values) {
    if (!zoom || !values.length) {
      return null;
    }
    const domainStart = values[0];
    const domainEnd = values[values.length - 1];
    let startValue = Number(zoom.startValue);
    let endValue = Number(zoom.endValue);
    if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) {
      const startPercent = Number.isFinite(Number(zoom.start)) ? Number(zoom.start) / 100 : 0;
      const endPercent = Number.isFinite(Number(zoom.end)) ? Number(zoom.end) / 100 : 1;
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
    const model = {
      startValue: null,
      endValue: null,
      visibleCount: null,
      followRightEdge: true,
      initialized: false,
    };

    function applyWindow(windowState) {
      model.startValue = windowState.startValue;
      model.endValue = windowState.endValue;
      model.visibleCount = windowState.visibleCount;
      model.followRightEdge = Boolean(windowState.followRightEdge);
      model.initialized = true;
      return snapshot();
    }

    function defaultWindow(values) {
      return {
        startValue: values[0],
        endValue: values[values.length - 1],
        visibleCount: values.length,
        followRightEdge: true,
      };
    }

    function snapshot() {
      return {
        startValue: model.startValue,
        endValue: model.endValue,
        visibleCount: model.visibleCount,
        followRightEdge: model.followRightEdge,
        initialized: model.initialized,
      };
    }

    function reset() {
      model.startValue = null;
      model.endValue = null;
      model.visibleCount = null;
      model.followRightEdge = true;
      model.initialized = false;
    }

    function ensureWindow(xValues, options) {
      const values = normalizeOrderedValues(xValues || []);
      if (!values.length) {
        reset();
        return null;
      }
      if (options?.reset || !model.initialized) {
        return applyWindow(defaultWindow(values));
      }
      const visibleCount = clamp(Number(model.visibleCount) || values.length, 1, values.length);
      let startIndex = 0;
      let endIndex = values.length - 1;
      if (model.followRightEdge) {
        startIndex = Math.max(0, values.length - visibleCount);
      } else {
        const anchor = Number.isFinite(Number(model.startValue)) ? Number(model.startValue) : values[0];
        startIndex = lowerBound(values, anchor);
        if (startIndex >= values.length) {
          startIndex = values.length - 1;
        }
      }
      endIndex = Math.min(values.length - 1, startIndex + visibleCount - 1);
      if ((endIndex - startIndex + 1) < visibleCount) {
        startIndex = Math.max(0, endIndex - visibleCount + 1);
      }
      return applyWindow({
        startValue: values[startIndex],
        endValue: values[endIndex],
        visibleCount: (endIndex - startIndex) + 1,
        followRightEdge: model.followRightEdge && endIndex >= (values.length - 1 - toleranceItems),
      });
    }

    function captureZoom(zoom, xValues) {
      const values = normalizeOrderedValues(xValues || []);
      if (!values.length) {
        reset();
        return null;
      }
      const range = resolveRangeFromZoom(zoom, values) || defaultWindow(values);
      return applyWindow({
        startValue: range.startValue,
        endValue: range.endValue,
        visibleCount: range.visibleCount,
        followRightEdge: range.endIndex >= (values.length - 1 - toleranceItems),
      });
    }

    return {
      reset: reset,
      captureZoom: captureZoom,
      ensureWindow: ensureWindow,
      visibleRange: function (xValues, options) {
        const windowState = ensureWindow(xValues, options);
        return windowState
          ? { min: windowState.startValue, max: windowState.endValue }
          : null;
      },
      zoomOptions: function (xValues, options) {
        const windowState = ensureWindow(xValues, options);
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
    pointItem: pointItem,
    rangeItem: rangeItem,
    buildIntegerYAxis: buildIntegerYAxis,
    buildVisibleIntegerYAxis: buildVisibleIntegerYAxis,
  };
}());
