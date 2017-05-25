package com.jd.bdp;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by tangshangwen on 17-3-6.
 */
public class OpenFalconReporter extends ScheduledReporter {
    public static final Logger logger = LoggerFactory.getLogger(OpenFalconReporter.class);

    private final OpenFalcon openFalcon;
    private final Clock clock;
    private final String prefix;
    private final String prefixToRemove;
    private final String tags;
    private final int step;

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    @Override
    public void report(
            SortedMap<String, Gauge> gauges,
            SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms,
            SortedMap<String, Meter> meters,
            SortedMap<String, Timer> timers) {

        final long timestamp = clock.getTime() / 1000;

        final Set<OpenFalconMetric> metrics = new HashSet<OpenFalconMetric>();

        for (Map.Entry<String, Gauge> g : gauges.entrySet()) {
            if(g.getValue().getValue() instanceof Collection && ((Collection)g.getValue().getValue()).isEmpty()) {
                continue;
            }
            metrics.add(buildGauge(g.getKey(), g.getValue(), timestamp));
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            metrics.add(buildCounter(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            metrics.addAll(buildHistograms(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            metrics.addAll(buildMeters(entry.getKey(), entry.getValue(), timestamp));
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            metrics.addAll(buildTimers(entry.getKey(), entry.getValue(), timestamp));
        }
        openFalcon.send(metrics);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private String prefixToRemove;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private int batchSize;
        private String tags;
        private int step;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.rateUnit = TimeUnit.SECONDS;
            this.prefix = null;
            this.durationUnit = TimeUnit.SECONDS;
            this.filter = MetricFilter.ALL;
            this.batchSize = OpenFalcon.DEFAULT_BATCH_SIZE_LIMIT;
            this.tags = "";
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder withTags(String tags) {
            this.tags = tags;
            return this;
        }

        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder removePreFix(String prefixToRemove) {
            this.prefixToRemove = prefixToRemove;
            return this;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertRurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withStep(int step) {
            this.step = step;
            return this;
        }

        public OpenFalconReporter build(OpenFalcon openFalcon) {
            openFalcon.setBatchSizeLimit(batchSize);
            return new OpenFalconReporter(
                    registry,
                    openFalcon,
                    clock,
                    prefix,
                    prefixToRemove,
                    rateUnit,
                    durationUnit,
                    filter,
                    tags,
                    step);
        }
    }

    private static class MetricsCollector {
        private final String prefix;
        private final long timestamp;
        private final Set<OpenFalconMetric> metrics = new HashSet<OpenFalconMetric>();
        private final String endpoint;
        private final int step;

        private MetricsCollector(
                String prefix, long timestamp, String endpoint, int step) {
            this.prefix = prefix;
            this.timestamp = timestamp;
            this.endpoint = endpoint;
            this.step = step;
        }

        public static MetricsCollector createNew(
                String prefix, long timestamp, String endpoint, int step) {
            return new MetricsCollector(prefix, timestamp, endpoint, step);
        }

        public MetricsCollector addMetric(String metricName, Object value) {
            this.metrics.add(OpenFalconMetric.named(MetricRegistry.name(prefix, metricName))
                    .withEndpoint(endpoint)
                    .withTimestamp(timestamp)
                    .withValue(value)
                    .withStep(step)
                    .build());
            return this;
        }

        public Set<OpenFalconMetric> build() {
            return metrics;
        }
    }

    private Set<OpenFalconMetric> buildTimers(String name, Timer timer, long timestamp) {
        final MetricsCollector collector = MetricsCollector.createNew(prefix(name), timestamp,
                getEndpoint(prefix(name, "value")), step);
        final Snapshot snapshot = timer.getSnapshot();

        return collector.addMetric("count", timer.getCount())
                //convert rate
                .addMetric("m15", convertRate(timer.getFifteenMinuteRate()))
                .addMetric("m5", convertRate(timer.getFiveMinuteRate()))
                .addMetric("m1", convertRate(timer.getOneMinuteRate()))
                .addMetric("mean_rate", convertRate(timer.getMeanRate()))
                // convert duration
                .addMetric("max", convertDuration(snapshot.getMax()))
                .addMetric("min", convertDuration(snapshot.getMin()))
                .addMetric("mean", convertDuration(snapshot.getMean()))
                .addMetric("stddev", convertDuration(snapshot.getStdDev()))
                .addMetric("median", convertDuration(snapshot.getMedian()))
                .addMetric("p75", convertDuration(snapshot.get75thPercentile()))
                .addMetric("p95", convertDuration(snapshot.get95thPercentile()))
                .addMetric("p98", convertDuration(snapshot.get98thPercentile()))
                .addMetric("p99", convertDuration(snapshot.get99thPercentile()))
                .addMetric("p999", convertDuration(snapshot.get999thPercentile()))
                .build();
    }

    private Set<OpenFalconMetric> buildHistograms(String name, Histogram histogram, long timestamp) {

        final MetricsCollector collector = MetricsCollector.createNew(prefix(name), timestamp,
                getEndpoint(prefix(name, "value")), step);
        final Snapshot snapshot = histogram.getSnapshot();

        return collector.addMetric("count", histogram.getCount())
                .addMetric("max", snapshot.getMax())
                .addMetric("min", snapshot.getMin())
                .addMetric("mean", snapshot.getMean())
                .addMetric("stddev", snapshot.getStdDev())
                .addMetric("median", snapshot.getMedian())
                .addMetric("p75", snapshot.get75thPercentile())
                .addMetric("p95", snapshot.get95thPercentile())
                .addMetric("p98", snapshot.get98thPercentile())
                .addMetric("p99", snapshot.get99thPercentile())
                .addMetric("p999", snapshot.get999thPercentile())
                .build();
    }


    private Set<OpenFalconMetric> buildMeters(String name, Meter meter, long timestamp) {

        final MetricsCollector collector = MetricsCollector.createNew(prefix(name), timestamp,
                getEndpoint(prefix(name, "value")), step);

        return collector.addMetric("count", meter.getCount())
                // convert rate
                .addMetric("mean_rate", convertRate(meter.getMeanRate()))
                .addMetric("m1", convertRate(meter.getOneMinuteRate()))
                .addMetric("m5", convertRate(meter.getFiveMinuteRate()))
                .addMetric("m15", convertRate(meter.getFifteenMinuteRate()))
                .build();
    }

    private OpenFalconMetric buildCounter(String name, Counter counter, long timestamp) {
        return OpenFalconMetric.named(prefix(name, "count"))
                .withEndpoint(getEndpoint(prefix(name, "value")))
                .withTimestamp(timestamp)
                .withValue(counter.getCount())
                .withStep(step)
                .withTags(tags)
                .build();
    }

    private OpenFalconMetric buildGauge(String name, Gauge gauge, long timestamp) {
        return OpenFalconMetric.named(prefix(name, "value"))
                .withEndpoint(getEndpoint(prefix(name, "value")))
                .withValue(gauge.getValue())
                .withTimestamp(timestamp)
                .withCounterType("GAUGE")
                .withStep(step)
                .withTags(tags)
                .build();
    }

    private String getEndpoint(String name) {
        String[] array = name.split("\\.");
        if(array.length > 0) {
            return array[0];
        }
        return name;
    }


    private String prefix(String... components) {
        if (prefixToRemove != null) {
            components[0] = components[0].replace(prefixToRemove,"");
        }
        return MetricRegistry.name(prefix, components);
    }

    private OpenFalconReporter(MetricRegistry registry,
                               OpenFalcon openFalcon,
                               Clock clock,
                               String prefix,
                               String prefixToRemove,
                               TimeUnit rateUnit,
                               TimeUnit durationUnit,
                               MetricFilter filter,
                               String tags, int step) {
        super(registry, "open-falcon-reporter", filter, rateUnit, durationUnit);
        this.openFalcon = openFalcon;
        this.clock = clock;
        this.prefix = prefix;
        this.prefixToRemove = prefixToRemove;
        this.tags = tags;
        this.step = step;
    }

    @Override
    public void stop() {
        super.stop();
        openFalcon.close();
    }
}
