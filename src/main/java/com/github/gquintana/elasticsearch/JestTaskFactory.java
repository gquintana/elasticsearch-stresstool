package com.github.gquintana.elasticsearch;

import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import com.github.gquintana.elasticsearch.index.IndexTask;
import com.github.gquintana.elasticsearch.index.JestIndexTask;
import com.github.gquintana.elasticsearch.search.JestSearchTask;
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

    public JestTaskFactory(List<String> hosts, String clusterName, String userName, char[] password) {
        super(hosts, clusterName, userName, password);
    }

    public JestTaskFactory(List<String> hosts, String clusterName) {
        super(hosts, clusterName);
    }

    public void open() {
        List<String> uris = parseHosts(9200)
                .map((inetAddress) -> "http://" + inetAddress.getHostString() + ":" + inetAddress.getPort())
                .collect(Collectors.toList());
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder httpClientConfigBuilder = new HttpClientConfig
                .Builder(uris)
                .multiThreaded(true);
        if (userName != null && password != null) {
            httpClientConfigBuilder.defaultCredentials(userName, getPasswordAsString());
        }
        factory.setHttpClientConfig(httpClientConfigBuilder.build());
        client = factory.getObject();
    }
    public IndexTask indexingTask(DataProvider dataProvider, TemplatingService templatingService) {
        JestIndexTask indexingTask = new JestIndexTask(client, dataProvider, templatingService);
        return indexingTask;
    }

    public Task searchingTask(DataProvider dataProvider, TemplatingService templatingService) {
        JestSearchTask searchingTask = new JestSearchTask(client, dataProvider, templatingService);
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
