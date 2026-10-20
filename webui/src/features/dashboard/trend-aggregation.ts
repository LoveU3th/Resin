import type { TimeWindow } from "./types.ts";

export const MAX_THROUGHPUT_POINTS = 480;

export type TrendPoint = {
  timestamp: number;
  values: number[];
  ranges?: Array<[number, number]>;
  bucketEnd?: number;
};

export function getThroughputAxisMax(points: TrendPoint[]): number {
  let meanMax = 0;
  let peakMax = 0;
  for (const point of points) {
    point.values.forEach((value, index) => {
      // The separate live sample contributes to peaks, not the bucket means.
      if (point.ranges) meanMax = Math.max(meanMax, value);
      peakMax = Math.max(peakMax, point.ranges?.[index][1] ?? value);
    });
  }
  if (peakMax <= 0) return 1;
  const mean = meanMax || peakMax;
  const target = 1.1 * mean * (peakMax / mean) ** 0.3;
  const step = 10 ** Math.floor(Math.log10(target)) / 5;
  return Math.max(1, Math.ceil(target / step) * step);
}

// The caller supplies the view's intended duration, never the changing span of
// the samples received so far. Reserve room for a partial left bucket and the
// separate latest sample. This also supports arbitrary, non-preset durations.
export function getTrendBucketWidth(windowDurationMs: number, maxPoints = MAX_THROUGHPUT_POINTS): number {
  if (!Number.isFinite(windowDurationMs) || windowDurationMs <= 0 || !Number.isFinite(maxPoints) || maxPoints < 3) {
    throw new RangeError("A positive window duration and at least three points are required");
  }
  const minimumWidth = Math.max(1, Math.ceil(windowDurationMs / (Math.floor(maxPoints) - 2)));
  const magnitude = 10 ** Math.floor(Math.log10(minimumWidth));
  const multiplier = [1, 2, 5, 10].find((value) => value * magnitude >= minimumWidth) ?? 10;
  return multiplier * magnitude;
}

// Fetch/cache the whole left bucket until it leaves the window. Trimming it
// sample by sample would change an already completed bucket's average/peak.
export function alignTrendWindow(window: TimeWindow, bucketWidthMs: number): TimeWindow {
  const from = Date.parse(window.from);
  if (!Number.isFinite(from)) {
    return window;
  }
  return {
    from: new Date(Math.floor(from / bucketWidthMs) * bucketWidthMs).toISOString(),
    to: window.to,
  };
}

export function aggregateTrendInput(
  labels: string[],
  series: Array<{ values: number[] }>,
  bucketWidthMs: number,
): TrendPoint[] {
  if (!Number.isFinite(bucketWidthMs) || bucketWidthMs < 1) {
    throw new RangeError("Bucket width must be a positive number of milliseconds");
  }
  type Stats = { total: number; min: number; max: number };
  const buckets = new Map<number, { count: number; stats: Stats[] }>();
  let latest: TrendPoint | undefined;

  labels.forEach((label, index) => {
    const timestamp = Date.parse(label);
    if (!Number.isFinite(timestamp)) {
      return;
    }
    const values = series.map((item) => Number.isFinite(item.values[index]) ? item.values[index] : 0);
    if (!latest || timestamp >= latest.timestamp) {
      latest = { timestamp, values };
    }
    const start = Math.floor(timestamp / bucketWidthMs) * bucketWidthMs;
    let bucket = buckets.get(start);
    if (!bucket) {
      bucket = {
        count: 0,
        stats: series.map(() => ({ total: 0, min: Number.POSITIVE_INFINITY, max: Number.NEGATIVE_INFINITY })),
      };
      buckets.set(start, bucket);
    }
    bucket.count += 1;
    bucket.stats.forEach((stats, seriesIndex) => {
      const value = values[seriesIndex];
      stats.total += value;
      stats.min = Math.min(stats.min, value);
      stats.max = Math.max(stats.max, value);
    });
  });

  const points: TrendPoint[] = [];
  for (const [start, bucket] of [...buckets.entries()].sort(([left], [right]) => left - right)) {
    // A single sample exactly on a bucket boundary is already the live point.
    if (start === latest?.timestamp && bucket.count === 1) {
      continue;
    }
    points.push({
      timestamp: start,
      bucketEnd: start + bucketWidthMs,
      values: bucket.stats.map((stats) => stats.total / bucket.count),
      ranges: bucket.stats.map((stats) => [stats.min, stats.max]),
    });
  }

  if (latest) {
    // Include this sample in its bucket above as well. Removing it from the
    // aggregate would delay/falsify the peak when a new bucket starts.
    points.push(latest);
  }

  return points;
}
