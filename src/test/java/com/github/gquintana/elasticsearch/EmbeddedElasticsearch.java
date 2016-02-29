package com.github.gquintana.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmbeddedElasticsearch {
    public static final String NODE_NAME = "elasticsearch-node";
    public static final String CLUSTER_NAME = "elasticsearch-cluster";
    private Node node;
    private Integer nodeTransportPort;
    private Integer nodeHttpPort;
    private Path path;
    private JestClientFactory jestClientFactory;
    private static EmbeddedElasticsearch instance;

    public static EmbeddedElasticsearch getInstance() {
        if (instance == null) {
            instance = new EmbeddedElasticsearch();
        }
        return instance;
    }
    private static Integer getPort(BoundTransportAddress boundTransportAddress) {
        TransportAddress transportAddress = boundTransportAddress.boundAddresses()[0];
        Integer port;
        if (transportAddress instanceof InetSocketTransportAddress) {
            port = ((InetSocketTransportAddress) transportAddress).address().getPort();
        } else {
            port = null;
        }
        return port;
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
                    .clusterName(CLUSTER_NAME)
                    .settings(Settings.builder()
                            .put("node.name", NODE_NAME)
                            .put("path.home", path.toAbsolutePath().toString())
                            .put("path.data", pathData.toAbsolutePath().toString())
                            .put("path.logs", pathLogs.toAbsolutePath().toString()))
                    .node();
            try(Client client = node.client()) {
                NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo(NODE_NAME)
                        .setHttp(true).setTransport(true).execute().actionGet();
                NodeInfo nodeInfo = nodeInfos.getAt(0);
                nodeTransportPort = getPort(nodeInfo.getTransport().getAddress());
                nodeHttpPort = getPort(nodeInfo.getHttp().address());
            }
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
    public static JestClient jestClient() {
        return getInstance().initJestClient();
    }
    public static void refresh(String index) {
        client().admin().indices().prepareRefresh(index).execute().actionGet();
    }
    public static long count(String index) {
        return client().prepareCount(index).execute().actionGet().getCount();
    }
    public static void delete(String index) {
        try {
            client().admin().indices().prepareDelete(index).execute().actionGet();
        } catch (IndexNotFoundException e) {
        }
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
    public JestClient initJestClient() {
        if (jestClientFactory == null) {
            String uri = "http://127.0.0.1:" + getNodeHttpPort();
            HttpClientConfig httpClientConfig = new HttpClientConfig.Builder(uri)
                    .build();
            jestClientFactory = new JestClientFactory();
            jestClientFactory.setHttpClientConfig(httpClientConfig);
        }
        return jestClientFactory.getObject();
    }

    public Integer getNodeHttpPort() {
        return nodeHttpPort;
    }
    public Integer getNodeTransportPort() {
        return nodeTransportPort;
    }
}
