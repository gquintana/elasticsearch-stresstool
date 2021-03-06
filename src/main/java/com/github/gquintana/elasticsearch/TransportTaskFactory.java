package com.github.gquintana.elasticsearch;

import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import com.github.gquintana.elasticsearch.index.IndexTask;
import com.github.gquintana.elasticsearch.index.TransportIndexTask;
import com.github.gquintana.elasticsearch.search.TransportSearchTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Task factory using native protocol
 */
public class TransportTaskFactory extends TaskFactory {
    private final boolean transport;
    private Node node;
    private Client client;

    public TransportTaskFactory(List<String> hosts, String clusterName, String userName, char[] password, boolean transport) {
        super(hosts, clusterName, userName, password);
        this.transport = transport;
    }

    public TransportTaskFactory(List<String> hosts, String clusterName, boolean transport) {
        super(hosts, clusterName);
        this.transport = transport;
    }

    private Client openTransportClient() {
        Settings.Builder settingsBuilder = Settings.builder();
        if (clusterName != null) {
            settingsBuilder.put("cluster.name", clusterName);
        }
        if (userName != null && password != null) {
            settingsBuilder.put("shield.user", userName + ":" + (password == null ? "" : getPasswordAsString()));
        }
        TransportClient.Builder transportClientBuilder = TransportClient.builder()
                .settings(settingsBuilder);
        TransportClient transportClient = transportClientBuilder.build();
        if (hosts.isEmpty()) {
            throw new EsStressToolException("No host");
        }
        parseHosts(9300)
                .map((inetAddress) -> new InetSocketTransportAddress(inetAddress))
                .forEach((inetAddress) -> {
                    transportClient.addTransportAddress(inetAddress);
                });
        client = transportClient;
        return transportClient;
    }

    private Client openNodeClient() {
        Settings.Builder settingsBuilder = Settings.builder();
        if (clusterName != null) {
            settingsBuilder.put("cluster.name", clusterName)
                    .put("node.data", false)
                    .put("node.client", true)
                    .put("node.master", false)
                    .put("path.home", System.getProperty("user.dir"));
        }
        if (hosts == null || hosts.isEmpty()) {
            settingsBuilder.put("discovery.zen.ping.multicast.enabled", true);
        } else {
            settingsBuilder.put("discovery.zen.ping.multicast.enabled", false);
            String sHosts = parseHosts(9300).map((inetAddress) -> inetAddress.getHostString() + ":" + inetAddress.getPort()).collect(Collectors.joining(","));
            settingsBuilder.put("discovery.zen.ping.unicast.hosts", sHosts);
        }
        if (userName != null && password != null) {
            throw new IllegalArgumentException("user/password not support with node client");
        }
        node = NodeBuilder.nodeBuilder()
                .settings(settingsBuilder).node();
        Client nodeClient = node.client();
        client = nodeClient;
        return nodeClient;
    }

    public void open() {
        if (transport) {
            openTransportClient();
        } else {
            openNodeClient();
        }
    }

    public IndexTask indexingTask(DataProvider dataProvider, TemplatingService templatingService) {
        TransportIndexTask indexingTask = new TransportIndexTask(client, dataProvider, templatingService);
        return indexingTask;
    }

    public Task searchingTask(DataProvider dataProvider, TemplatingService templatingService) {
        TransportSearchTask searchingTask = new TransportSearchTask(client, dataProvider, templatingService);
        return searchingTask;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
        if (node != null) {
            node.close();
            node = null;
        }
    }
}
