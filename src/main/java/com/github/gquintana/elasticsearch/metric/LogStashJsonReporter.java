package com.github.gquintana.elasticsearch.metric;

import com.codahale.metrics.*;
import com.github.gquintana.elasticsearch.EsStressToolException;

import java.io.*;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Metric Reporter producing Logstash compatible JSON format
 */
public class LogStashJsonReporter extends ScheduledReporter {
    private final Clock clock;
    private final File outputFile;
    private PrintWriter outputWriter;

    public LogStashJsonReporter(MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, Clock clock, File outputFile) {
        super(registry, "logstash-json-reporter", filter, rateUnit, durationUnit);
        this.clock = clock;
        this.outputFile = outputFile;
    }

    public LogStashJsonReporter(MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, ScheduledExecutorService executor, Clock clock, File outputFile) {
        super(registry, "logstash-json-reporter", filter, rateUnit, durationUnit, executor);
        this.clock = clock;
        this.outputFile = outputFile;
    }


    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        reportMap(gauges, this::reportGauge);
        reportMap(counters, this::reportCounter);
        reportMap(histograms, this::reportHistogram);
        reportMap(meters, this::reportMeter);
        reportMap(timers, this::reportTimer);
        outputWriter.flush();
    }

    private <T> void reportMap(SortedMap<String, T> map, BiConsumer<String, T> reportFunction) {
        for (Map.Entry<String, T> namedMetric : map.entrySet()) {
            reportFunction.accept(namedMetric.getKey(), namedMetric.getValue());
        }
    }

    public static LogStashJsonReporter.Builder forRegistry(MetricRegistry metricRegistry) {
        return new Builder(metricRegistry);
    }

    private class ReportBuilder {
        private final StringBuilder stringBuilder = new StringBuilder("{");
        private boolean firstField = true;

        private ReportBuilder doField(String name, String value) {
            if (value != null) {
                if (firstField) {
                    firstField = false;
                } else {
                    stringBuilder.append(',');
                }
                stringBuilder.append('"').append(name).append("\":").append(value);
            }
            return this;
        }

        public ReportBuilder field(String name, String value) {
            return doField(name, "\"" + value.replaceAll("\"", "\\\"") + "\"");
        }

        public ReportBuilder field(String name, long value) {
            return doField(name, Long.toString(value));
        }

        public ReportBuilder field(String name, double value) {
            return doField(name, Double.toString(value));
        }

        public void end() {
            stringBuilder.append("}");
            report(stringBuilder.toString());
        }
    }

    private void report(String report) {
        if (outputWriter == null) {
            try {
                outputWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
            } catch (IOException e) {
                throw new EsStressToolException(e);
            }
        }
        outputWriter.println(report);
    }

    private ReportBuilder startReport(String name, String type) {
        return new ReportBuilder().field("@timestamp", clock.getTime())
                .field("name", name)
                .field("type", type);
    }

    private void reportGauge(String name, Gauge gauge) {
        Object value = gauge.getValue();
        if (value instanceof Double) {
            startReport(name, "gauge")
                    .field("value", (Double) value)
                    .end();
        } else if (value instanceof Long) {
            startReport(name, "gauge")
                    .field("value", (Long) value)
                    .end();
        } else {
            startReport(name, "gauge")
                    .field("value", value.toString())
                    .end();
        }
    }

    private void reportCounter(String name, Counter counter) {
        startReport(name, "counter")
                .field("count", counter.getCount())
                .end();
    }

    private ReportBuilder reportSnapshot(ReportBuilder reportBuilder, Snapshot snapshot) {
        return reportBuilder.field("min", snapshot.getMin())
                .field("max", snapshot.getMax())
                .field("mean", snapshot.getMean())
                .field("median", snapshot.getMedian())
                .field("75thPercentile", snapshot.get75thPercentile())
                .field("95thPercentile", snapshot.get95thPercentile())
                .field("98thPercentile", snapshot.get98thPercentile())
                .field("99thPercentile", snapshot.get99thPercentile())
                .field("stddev", snapshot.getStdDev());
    }

    private void reportHistogram(String name, Histogram histogram) {
        ReportBuilder reportBuilder = startReport(name, "histogram")
                .field("count", histogram.getCount());
        reportSnapshot(reportBuilder, histogram.getSnapshot())
                .end();
    }

    private ReportBuilder reportMetered(ReportBuilder reportBuilder, Metered metered) {
        return reportBuilder.field("oneMinuteRate", metered.getOneMinuteRate())
                .field("fiveMinuteRate", metered.getFiveMinuteRate())
                .field("fifteenMinuteRate", metered.getFifteenMinuteRate())
                .field("meanRate", metered.getMeanRate());
    }

    private void reportMeter(String name, Meter meter) {
        ReportBuilder reportBuilder = startReport(name, "meter")
                .field("count", meter.getCount());
        reportMetered(reportBuilder, meter).end();
    }

    private void reportTimer(String name, Timer timer) {
        ReportBuilder reportBuilder = startReport(name, "timer")
                .field("count", timer.getCount());
        reportMetered(reportBuilder, timer);
        reportSnapshot(reportBuilder, timer.getSnapshot())
                .end();
    }

    public static class Builder {
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        public LogStashJsonReporter.Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public LogStashJsonReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public LogStashJsonReporter.Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public LogStashJsonReporter.Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public LogStashJsonReporter build(File file) {
            return new LogStashJsonReporter(this.registry, this.filter, this.rateUnit, this.durationUnit, this.clock, file);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (this.outputWriter!=null) {
            this.outputWriter.close();
            this.outputWriter = null;
        }
    }
}
