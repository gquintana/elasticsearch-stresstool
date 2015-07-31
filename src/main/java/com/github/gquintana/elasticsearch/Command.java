package com.github.gquintana.elasticsearch;

import com.beust.jcommander.Parameter;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Command {
    @Parameter(names = {"-h", "--host"}, description = "Hosts and ports")
    protected List<String> hosts = Arrays.asList("localhost:9300");
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
    @Parameter(names = {"-m", "--metric-period"}, description = "Period in second for metric reporting")
    protected long metricPeriod = 10;
    @Parameter(names = {"--help"}, help = true)
    protected boolean help;
    private List<Runnable> stopCallbacks = new ArrayList<>();
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected Client createTransportClient() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        if (clusterName != null) {
            settingsBuilder.put("cluster.name", clusterName);
        }
        TransportClient transportClient = new TransportClient(settingsBuilder);
        if (hosts.isEmpty()) {
            throw new EsStressToolException("No host");
        }
        Pattern hostPattern = Pattern.compile("([^:]+)(?::([0-9]+))");
        for (String host : hosts) {
            Matcher hostMatcher = hostPattern.matcher(host);
            if (hostMatcher.matches()) {
                String sHost = hostMatcher.group(1);
                String sPort = hostMatcher.group(2);
                int port = sPort == null ? 9300 : Integer.valueOf(sPort);
                transportClient.addTransportAddress(new InetSocketTransportAddress(sHost, port));
            } else {
                throw new EsStressToolException("Invalid host " + host);
            }
        }
        stopCallbacks.add(() -> transportClient.close());
        return transportClient;
    }

    protected Client createNodeClient() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        if (clusterName != null) {
            settingsBuilder.put("cluster.name", clusterName)
                    .put("node.data", false)
                    .put("node.master", false);
        }
        if (hosts == null || hosts.isEmpty()) {
            settingsBuilder.put("discovery.zen.ping.multicast.enabled", true);
        } else {
            settingsBuilder.put("discovery.zen.ping.multicast.enabled", false);
            settingsBuilder.put("discovery.zen.ping.unicast.hosts", hosts);
        }
        Node node = NodeBuilder.nodeBuilder().client(true)
                .settings(settingsBuilder).node();
        Client nodeClient= node.client();
        stopCallbacks.add(() -> nodeClient.close());
        stopCallbacks.add(() -> node.close());
        return nodeClient;
    }

    protected Client createClient() {
        if (protocol.equals("transport")) {
            return createTransportClient();
        } else if (protocol.equals("node")) {
            return createNodeClient();
        } else {
            throw new EsStressToolException("Unknown protocol " + protocol);
        }
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

    protected MetricRegistry createMetricRegistry() {
        MetricRegistry metricRegistry = new MetricRegistry();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .inDomain(getClass().getPackage().getName())
                .build();
        jmxReporter.start();
        stopCallbacks.add(() ->{ jmxReporter.stop();});
        ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
                .build();
        consoleReporter.start(metricPeriod, TimeUnit.SECONDS);
        stopCallbacks.add(() -> {
            consoleReporter.stop();
        });
        return metricRegistry;
    }

    protected abstract Task createTask();

    protected void closeTask(Task task) {
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
        closeTask(task);
    }
}
