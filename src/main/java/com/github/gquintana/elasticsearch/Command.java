package com.github.gquintana.elasticsearch;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.*;
import com.github.gquintana.elasticsearch.data.ConstantDataProvider;
import com.github.gquintana.elasticsearch.data.CsvDataProvider;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.metric.LogStashJsonReporter;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.common.io.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class Command {
    @Parameter(names = {"-h", "--host"}, description = "Hosts and ports")
    protected List<String> hosts = Arrays.asList("localhost");
    @Parameter(names = {"-c", "--cluster"}, description = "Cluster name")
    protected String clusterName;
    @Parameter(names = {"-u", "--user"}, description = "User name")
    protected String userName;
    @Parameter(names = {"-p", "--password"}, description = "Password or password file or password prompt", converter = PasswordStringConverter.class)
    protected char[] password;
    @Parameter(names = {"-x", "--protocol"}, description = "Protocol")
    protected String protocol = "transport";
    @Parameter(names = {"-t", "--thread"}, description = "Thread number")
    protected int threads = Integer.min(32, Runtime.getRuntime().availableProcessors());
    @Parameter(names = {"-n", "--iterations"}, description = "Iterations")
    protected int iterations =  10000;
    @Parameter(names = {"-ep", "--execute-period-ms"}, description = "Period in ms between each execution")
    protected Long executePeriodMs;
    @Parameter(names = {"-sp", "--start-period-ms"}, description = "Period in ms between each thread start")
    protected Long startPeriodMs;
    @Parameter(names = {"-di", "--doc-index", "--index"}, description = "Index")
    protected String docIndex = ".stresstest";
    @Parameter(names = {"-dt", "--doc-type"}, description = "Document type")
    protected String docType = "stress";
    @Parameter(names = {"-dd", "--doc-data", "-qd", "--query-data"}, description = "Document/Query data CSV file")
    protected String docData;
    @Parameter(names = {"-dm", "--doc-template", "-qm", "--query-template"}, description = "Document/Query Mustache file")
    protected String docTemplate;
    @Parameter(names = {"-mp", "--metric-period-ms"}, description = "Period in ms for metric reporting")
    protected long metricPeriodMs = 10000;
    @Parameter(names = {"-mc", "--metric-console"}, description = "Output Console for metric reporting")
    protected boolean metricConsole = false;
    @Parameter(names = {"-mo", "--metric-output"}, description = "Output File for metric reporting, ending either .csv or .json")
    protected File metricOutput;
    @Parameter(names = {"--help"}, help = true)
    protected boolean help;
    protected TaskFactory taskFactory;
    private List<AutoCloseable> closeables = new ArrayList<>();
    private List<Reporter> metricReporters = new ArrayList<>();
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private void registerCloseable(final AutoCloseable closeable) {
        closeables.add(closeable);
    }
    protected TaskFactory createTaskFactory() {
        if (taskFactory != null) {
            return taskFactory;
        }
        loadPassword();
        switch (protocol) {
            case "transport":
                taskFactory = new TransportTaskFactory(hosts, clusterName, userName, password, true);
                break;
            case "node":
                taskFactory = new TransportTaskFactory(hosts, clusterName, userName, password, false);
                break;
            case "http":
            case "jest":
                taskFactory = new JestTaskFactory(hosts, clusterName, userName, password);
                break;
            default:
                throw new EsStressToolException("Unknown protocol " + protocol);
        }
        taskFactory.open();
        registerCloseable(taskFactory);
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
            Slf4jReporter slf4jReporter = Slf4jReporter.forRegistry(metricRegistry)
                    .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
                    .outputTo(logger)
                    .build();
            slf4jReporter.start(metricPeriodMs, TimeUnit.MILLISECONDS);
            registerMetricReporter(slf4jReporter);
        }
        // CSV/JSON File Reporter
        if (metricOutput != null) {
            String metricOutputName = metricOutput.getName().toLowerCase();
            if (metricOutputName.endsWith(".csv")) {
                metricOutput.mkdirs();
                CsvReporter csvReporter = CsvReporter.forRegistry(metricRegistry)
                        .build(metricOutput);
                csvReporter.start(metricPeriodMs, TimeUnit.MILLISECONDS);
                registerMetricReporter(csvReporter);
            } else if (metricOutputName.endsWith(".json")) {
                LogStashJsonReporter jsonReporter = LogStashJsonReporter.forRegistry(metricRegistry)
                        .build(metricOutput);
                jsonReporter.start(metricPeriodMs, TimeUnit.MILLISECONDS);
                registerMetricReporter(jsonReporter);
            }
        }
        for(Reporter metricReporter:metricReporters) {
            if (metricReporter instanceof Closeable) {
                registerCloseable((Closeable) metricReporter);
            }
            if (metricReporter instanceof ScheduledReporter) {
                registerCloseable(() -> { ((ScheduledReporter) metricReporter).report(); });
            }
        }
        return metricRegistry;
    }
    protected abstract Task createTask();

    public void close() {
        Collections.reverse(closeables);
        for(AutoCloseable closeable: closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
            }
        }
    }

    public void execute() {
        MetricRegistry metricRegistry = createMetricRegistry();
        TaskRunner taskRunner = new TaskRunner(metricRegistry);
        taskRunner.setThreadNumber(threads);
        taskRunner.setIterationNumber(iterations);
        taskRunner.setExecutePeriodMs(executePeriodMs);
        taskRunner.setStartPeriodNs(executePeriodMs);
        taskRunner.start();
        registerCloseable(taskRunner);
        Task task = createTask();
        Timer timer = metricRegistry.timer(getClass().getName() + ".timer");
        logger.info("{} threads, {} iterations, {}, starting", threads, iterations, task);
        Timer.Context timerContext = timer.time();
        try {
            taskRunner.run(task).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Command execution failed", e);
        }
        long duration = timerContext.stop();
        logger.info("{} threads, {} iterations, {}, {}, {}ms", threads, iterations, task, Times.formatForHuman(duration, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS.toMillis(duration));
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

    public long getMetricPeriodMs() {
        return metricPeriodMs;
    }

    public void setMetricPeriodMs(long metricPeriodMs) {
        this.metricPeriodMs = metricPeriodMs;
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

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public char[] getPassword() {
        return password;
    }

    public void setPassword(char[] password) {
        this.password = password;
    }

    public void loadPassword() {
        try {
            if (password == null || password.length==0) {
            } else if (Arrays.equals(password, "prompt".toCharArray())) {
                Console console = System.console();
                if (console != null) {
                    password = console.readPassword("Password");
                }
            } else if (password[0] == '@') {
                File passwordFile = new File(new String(password).substring(1));
                byte[] bytes = Streams.copyToByteArray(passwordFile);
                char[] lPassword = Charset.defaultCharset().decode(ByteBuffer.wrap(bytes)).array();
                int lastNullIndex = lPassword.length;
                while(lastNullIndex > 0 && lPassword[lastNullIndex - 1] == '\u0000') {
                    lastNullIndex--;
                }
                password = new char[lastNullIndex];
                System.arraycopy(lPassword, 0, password, 0, lastNullIndex);
            }
            this.password = password == null || password.length == 0 ? null : password;
            this.userName = Strings.emptyToNull(userName);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid password", e);
        }
    }
    public static class PasswordStringConverter implements IStringConverter<char[]> {
        @Override
        public char[] convert(String s) {
            return s.toCharArray();
        }
    }
}
