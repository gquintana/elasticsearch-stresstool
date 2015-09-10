package com.github.gquintana.elasticsearch;

import com.beust.jcommander.Parameter;
import com.codahale.metrics.*;
import com.github.gquintana.elasticsearch.data.ConstantDataProvider;
import com.github.gquintana.elasticsearch.data.CsvDataProvider;
import com.github.gquintana.elasticsearch.data.DataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class Command {
    @Parameter(names = {"-h", "--host"}, description = "Hosts and ports")
    protected List<String> hosts = Arrays.asList("localhost");
    @Parameter(names = {"-c", "--cluster"}, description = "Cluster name")
    protected String clusterName;
    @Parameter(names = {"-x", "--protocol"}, description = "Protocol")
    protected String protocol = "transport";
    @Parameter(names = {"-t", "--thread"}, description = "Thread number")
    protected int threads = Integer.min(32, Runtime.getRuntime().availableProcessors());
    @Parameter(names = {"-n", "--iterations"}, description = "Iterations")
    protected int iterations =  10000;
    @Parameter(names = {"-di", "--doc-index", "--index"}, description = "Index")
    protected String docIndex = ".stresstest";
    @Parameter(names = {"-dt", "--doc-type"}, description = "Document type")
    protected String docType = "stress";
    @Parameter(names = {"-dd", "--doc-data", "-qd", "--query-data"}, description = "Document/Query data CSV file")
    protected String docData;
    @Parameter(names = {"-dm", "--doc-template", "-qm", "--query-template"}, description = "Document/Query Mustache file")
    protected String docTemplate;
    @Parameter(names = {"-mp", "--metric-period"}, description = "Period in second for metric reporting")
    protected long metricPeriod = 10;
    @Parameter(names = {"-mc", "--metric-console"}, description = "Output Console for metric reporting")
    protected boolean metricConsole = false;
    @Parameter(names = {"-mo", "--metric-output"}, description = "Output CSV file for metric reporting")
    protected File metricOutput;
    @Parameter(names = {"--help"}, help = true)
    protected boolean help;
    protected TaskFactory taskFactory;
    private List<Runnable> stopCallbacks = new ArrayList<>();
    private List<Reporter> metricReporters = new ArrayList<>();
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected TaskFactory createTaskFactory() {
        if (taskFactory != null) {
            return taskFactory;
        }
        switch (protocol) {
            case "transport":
                taskFactory = new TransportTaskFactory(hosts, clusterName, true);
                break;
            case "node":
                taskFactory = new TransportTaskFactory(hosts, clusterName, false);
                break;
            case "http":
            case "jest":
                taskFactory = new JestTaskFactory(hosts, clusterName);
                break;
            default:
                throw new EsStressToolException("Unknown protocol " + protocol);
        }
        taskFactory.open();
        stopCallbacks.add(() -> {
            try {
                taskFactory.close();
            } catch (Exception e) {
            }
        });
        return taskFactory;
    }

    protected DataProvider createDataProvider() {
        if (docData == null) {
            return new ConstantDataProvider(docIndex, docType);
        } else if (docData.equals("names")) {
            CsvDataProvider csvDataProvider = new CsvDataProvider("classpath:/config/names.txt");
            csvDataProvider.setIndexDefault(docIndex);
            csvDataProvider.setTypeDefault(docType);
            return csvDataProvider;
        } else {
            CsvDataProvider csvDataProvider = new CsvDataProvider(docData);
            csvDataProvider.setIndexDefault(docIndex);
            csvDataProvider.setTypeDefault(docType);
            // TODO Configure CSV columns
            return csvDataProvider;
        }
    }
    private void registerMetricReporter(Reporter reporter) {
        metricReporters.add(reporter);
    }
    protected MetricRegistry createMetricRegistry() {
        MetricRegistry metricRegistry = new MetricRegistry();
        // JMX Reporter
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .inDomain(getClass().getPackage().getName())
                .build();
        jmxReporter.start();
        registerMetricReporter(jmxReporter);
        // Console Reporter
        if (metricConsole) {
            ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
                    .build();
            consoleReporter.start(metricPeriod, TimeUnit.SECONDS);
            registerMetricReporter(consoleReporter);
        }
        // CSV Reporter
        if (metricOutput != null) {
            metricOutput.mkdirs();
            CsvReporter csvReporter = CsvReporter.forRegistry(metricRegistry)
                    .build(metricOutput);
            csvReporter.start(metricPeriod, TimeUnit.SECONDS);
            registerMetricReporter(csvReporter);
        }
        stopCallbacks.add(() -> {
            for(Reporter metricReporter:metricReporters) {
                if (metricReporter instanceof Closeable) {
                    try {
                        ((Closeable) metricReporter).close();
                    } catch (IOException e) {
                    }
                }
            }
        });
        return metricRegistry;
    }
    protected abstract Task createTask();

    public void close() {
        for(Runnable stopCallback: stopCallbacks) {
            try {
                stopCallback.run();
            } catch (Exception e) {
            }
        }
    }

    public void execute() {
        TaskRunner taskRunner = new TaskRunner(createMetricRegistry());
        taskRunner.setThreadNumber(threads);
        taskRunner.setIterationNumber(iterations);
        taskRunner.start();
        Task task = createTask();
        try {
            taskRunner.run(task).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Command execution failed", e);
        }
        close();
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    public String getDocIndex() {
        return docIndex;
    }

    public void setDocIndex(String docIndex) {
        this.docIndex = docIndex;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    public String getDocData() {
        return docData;
    }

    public void setDocData(String docData) {
        this.docData = docData;
    }

    public String getDocTemplate() {
        return docTemplate;
    }

    public void setDocTemplate(String docTemplate) {
        this.docTemplate = docTemplate;
    }

    public long getMetricPeriod() {
        return metricPeriod;
    }

    public void setMetricPeriod(long metricPeriod) {
        this.metricPeriod = metricPeriod;
    }

    public boolean isMetricConsole() {
        return metricConsole;
    }

    public void setMetricConsole(boolean metricConsole) {
        this.metricConsole = metricConsole;
    }

    public File getMetricOutput() {
        return metricOutput;
    }

    public void setMetricOutput(File metricOutput) {
        this.metricOutput = metricOutput;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }
}
