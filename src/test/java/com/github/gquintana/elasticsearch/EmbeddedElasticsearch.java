package com.github.gquintana.elasticsearch;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmbeddedElasticsearch {
    private Node node;
    private Path path;
    private static EmbeddedElasticsearch instance;

    public static EmbeddedElasticsearch getInstance() {
        if (instance == null) {
            instance = new EmbeddedElasticsearch();
        }
        return instance;
    }

    public Node initNode() {
        if (node != null) {
            return node;
        }
        try {
            path = Files.createTempDirectory("elasticsearch");
            Path pathData = Files.createDirectory(path.resolve("data"));
            Path pathLogs = Files.createDirectory(path.resolve("logs"));

            node = NodeBuilder.nodeBuilder()
                    .clusterName("elasticsearch-cluster")
                    .local(true)
                    .settings(ImmutableSettings.builder()
                            .put("node.name", "elasticsearch-node")
                            .put("path.data", pathData.toAbsolutePath().toString())
                            .put("path.logs", pathLogs.toAbsolutePath().toString()))
                    .node();
            return node;
        } catch (IOException e) {
            throw new EsStressToolException("Unable to initialize Elasticsearch", e);
        }
    }
    public Client initClient() {
        return initNode().client();
    }
    public static Node node() {
        return getInstance().initNode();
    }
    public static Client client() {
        return getInstance().initClient();
    }
    public static void refresh(String index) {
        client().admin().indices().prepareRefresh(index).execute().actionGet();
    }
    public static long count(String index) {
        return client().prepareCount(index).execute().actionGet().getCount();
    }
    public static void delete(String index) {
        client().admin().indices().prepareDelete(index).execute().actionGet();
    }
    public static void close() {
        getInstance().closeNode();
    }
    public void closeNode() {
        if (node != null) {
            node.close();
            node = null;
        }
    }
}
