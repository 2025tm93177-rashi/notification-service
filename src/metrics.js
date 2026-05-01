const DEFAULT_BUCKETS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];

function escapeLabelValue(value) {
  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\n/g, "\\n")
    .replace(/"/g, '\\"');
}

function labelKey(labels = {}) {
  return Object.keys(labels)
    .sort()
    .map((key) => `${key}=${labels[key]}`)
    .join("|");
}

function formatLabels(labels = {}) {
  const keys = Object.keys(labels);
  if (!keys.length) {
    return "";
  }

  const rendered = keys
    .sort()
    .map((key) => `${key}="${escapeLabelValue(labels[key])}"`)
    .join(",");
  return `{${rendered}}`;
}

class MetricsRegistry {
  constructor() {
    this.counters = new Map();
    this.histograms = new Map();
    this.descriptors = new Map();
  }

  registerCounter(name, help = "") {
    this.descriptors.set(name, { type: "counter", help });
  }

  registerHistogram(name, help = "", buckets = DEFAULT_BUCKETS) {
    this.descriptors.set(name, {
      type: "histogram",
      help,
      buckets: [...buckets].sort((a, b) => a - b),
    });
  }

  inc(name, labels = {}, value = 1) {
    this.registerCounter(name);
    const key = `${name}|${labelKey(labels)}`;
    const current = this.counters.get(key) || {
      name,
      labels,
      value: 0,
    };
    current.value += value;
    this.counters.set(key, current);
  }

  observe(name, value, labels = {}, buckets = DEFAULT_BUCKETS) {
    this.registerHistogram(name, "", buckets);
    const descriptor = this.descriptors.get(name);
    const key = `${name}|${labelKey(labels)}`;
    const current =
      this.histograms.get(key) || {
        name,
        labels,
        buckets: descriptor.buckets,
        counts: descriptor.buckets.map(() => 0),
        sum: 0,
        count: 0,
      };

    current.sum += value;
    current.count += 1;
    current.buckets.forEach((bucket, index) => {
      if (value <= bucket) {
        current.counts[index] += 1;
      }
    });
    this.histograms.set(key, current);
  }

  render() {
    const lines = [];
    const sortedDescriptors = [...this.descriptors.entries()].sort(([a], [b]) =>
      a.localeCompare(b),
    );

    for (const [name, descriptor] of sortedDescriptors) {
      lines.push(`# TYPE ${name} ${descriptor.type}`);
    }

    const sortedCounters = [...this.counters.values()].sort((a, b) =>
      a.name.localeCompare(b.name),
    );
    for (const series of sortedCounters) {
      lines.push(`${series.name}${formatLabels(series.labels)} ${series.value}`);
    }

    const sortedHistograms = [...this.histograms.values()].sort((a, b) =>
      a.name.localeCompare(b.name),
    );
    for (const series of sortedHistograms) {
      series.buckets.forEach((bucket, index) => {
        lines.push(
          `${series.name}_bucket${formatLabels({ ...series.labels, le: bucket })} ${series.counts[index]}`,
        );
      });
      lines.push(
        `${series.name}_bucket${formatLabels({ ...series.labels, le: "+Inf" })} ${series.count}`,
      );
      lines.push(`${series.name}_sum${formatLabels(series.labels)} ${series.sum}`);
      lines.push(`${series.name}_count${formatLabels(series.labels)} ${series.count}`);
    }

    return lines.join("\n") + "\n";
  }
}

module.exports = {
  DEFAULT_BUCKETS,
  MetricsRegistry,
};
