package com.github.gquintana.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Task factory using HTTP protocol and Jest library
 */
public class JestTaskFactory extends TaskFactory {
    private JestClient client;

    public JestTaskFactory(List<String> hosts, String clusterName) {
        super(hosts, clusterName);
    }

    public void open() {
        List<String> uris = parseHosts(9200)
                .map((inetAddress) -> "http://" + inetAddress.getHostString() + ":" + inetAddress.getPort())
                .collect(Collectors.toList());
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(uris)
                .multiThreaded(true)
                .build());
        client = factory.getObject();
    }
    public IndexingTask indexingTask(DataProvider dataProvider, TemplatingService templatingService) {
        JestIndexingTask indexingTask = new JestIndexingTask(client, dataProvider, templatingService);
        return indexingTask;
    }

    public Task searchingTask(DataProvider dataProvider, TemplatingService templatingService) {
        JestSearchingTask searchingTask = new JestSearchingTask(client, dataProvider, templatingService);
        return searchingTask;
    }

    @Override
    public void close() {
        if (client != null) {
            client.shutdownClient();
            client = null;
        }
    }
}
