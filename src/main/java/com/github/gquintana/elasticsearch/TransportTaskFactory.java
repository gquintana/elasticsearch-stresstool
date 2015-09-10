package com.github.gquintana.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.List;

/**
 * Task factory using native protocol
 */
public class TransportTaskFactory extends TaskFactory {
    private final boolean transport;
    private Node node;
    private Client client;

    public TransportTaskFactory(List<String> hosts, String clusterName, boolean transport) {
        super(hosts, clusterName);
        this.transport = transport;
    }

    private Client openTransportClient() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        if (clusterName != null) {
            settingsBuilder.put("cluster.name", clusterName);
        }
        TransportClient transportClient = new TransportClient(settingsBuilder);
        if (hosts.isEmpty()) {
            throw new EsStressToolException("No host");
        }
        parseHosts(9300)
                .map((inetAddress) -> new InetSocketTransportAddress(inetAddress))
                .forEach((inetAddress) -> transportClient.addTransportAddress(inetAddress));

        client = transportClient;
        return transportClient;
    }

    private Client openNodeClient() {
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
        node = NodeBuilder.nodeBuilder().client(true)
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

    public IndexingTask indexingTask(DataProvider dataProvider, TemplatingService templatingService) {
        TransportIndexingTask indexingTask = new TransportIndexingTask(client, dataProvider, templatingService);
        return indexingTask;
    }

    public Task searchingTask(DataProvider dataProvider, TemplatingService templatingService) {
        TransportSearchingTask searchingTask = new TransportSearchingTask(client, dataProvider, templatingService);
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
