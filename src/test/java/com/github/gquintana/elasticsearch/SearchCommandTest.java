package com.github.gquintana.elasticsearch;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Search integration test
 */
public class SearchCommandTest {
    @BeforeClass
    public static void setUpClass() {
        EmbeddedElasticsearch.node();
    }
    @AfterClass
    public static void tearDownClass() {
        EmbeddedElasticsearch.close();
    }

    @Before
    public void before() {
        IndexCommand command = new IndexCommand();
        EmbeddedElasticsearch.delete(command.getDocIndex());
        command.setClusterName(EmbeddedElasticsearch.CLUSTER_NAME);
        command.setProtocol("transport");
        command.setHosts(Arrays.asList("localhost:" + EmbeddedElasticsearch.getInstance().getNodeTransportPort()));
        command.setDocData("names");
        command.setDocTemplate("classpath:/com/github/gquintana/elasticsearch/doc-name.mustache");
        command.setIterations(2);
        command.setThreads(1);
        command.setBulkSize(10);
        command.execute();
        command.close();
        EmbeddedElasticsearch.refresh(command.getDocIndex());
        assertEquals(20, EmbeddedElasticsearch.count(command.getDocIndex()));
    }
    private SearchCommand createSearchCommand() {
        SearchCommand command = new SearchCommand();
        command.setClusterName(EmbeddedElasticsearch.CLUSTER_NAME);
        command.setDocData("names");
        command.setDocTemplate("classpath:/com/github/gquintana/elasticsearch/query-name.mustache");
        command.setIterations(10);
        command.setThreads(2);
        return command;
    }

    @Test
    public void testExecuteTransport() {
        // Given
        SearchCommand command = createSearchCommand();
        command.setProtocol("transport");
        command.setHosts(Arrays.asList("localhost:" + EmbeddedElasticsearch.getInstance().getNodeTransportPort()));
        // When
        command.execute();
        command.close();
        // Then
    }

    @Test
    public void testExecuteJest() {
        // Given
        SearchCommand command = createSearchCommand();
        command.setProtocol("jest");
        command.setHosts(Arrays.asList("localhost:" + EmbeddedElasticsearch.getInstance().getNodeHttpPort()));
        // When
        command.execute();
        command.close();
        // Then
    }

}