package com.github.gquintana.elasticsearch;

import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import com.github.gquintana.elasticsearch.index.IndexTask;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Base class for task factories
 */
public abstract class TaskFactory implements AutoCloseable {
    protected final List<String> hosts;
    protected final String clusterName;

    public TaskFactory(List<String> hosts, String clusterName) {
        this.hosts = hosts;
        this.clusterName = clusterName;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public String getClusterName() {
        return clusterName;
    }

    public abstract void open();
    protected Stream<InetSocketAddress> parseHosts(int defaultPort) {
        Pattern hostPattern = Pattern.compile("([^:]+)(?::([0-9]+))?");
        return hosts.stream()
                .map((String host) -> {
                    Matcher hostMatcher = hostPattern.matcher(host);
                    if (hostMatcher.matches()) {
                        String sHost = hostMatcher.group(1);
                        String sPort = hostMatcher.group(2);
                        int port = sPort == null ? defaultPort : Integer.valueOf(sPort);
                        return new InetSocketAddress(sHost, port);
                    } else {
                        System.err.println("Invalid host " + host);
                        return null;
                    }
                })
                .filter((inetAddress) -> inetAddress != null);
    }
    public abstract IndexTask indexingTask(DataProvider dataProvider, TemplatingService templatingService);

    public abstract Task searchingTask(DataProvider dataProvider, TemplatingService templatingService);
}
