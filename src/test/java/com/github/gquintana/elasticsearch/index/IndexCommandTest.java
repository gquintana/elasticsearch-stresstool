package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.EmbeddedElasticsearch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Index integration test
 */
public class IndexCommandTest {
    @BeforeClass
    public static void setUpClass() {
        EmbeddedElasticsearch.node();
    }
    @AfterClass
    public static void tearDownClass() {
        EmbeddedElasticsearch.close();
    }
    private IndexCommand createIndexCommand() {
        IndexCommand command = new IndexCommand();
        command.setClusterName(EmbeddedElasticsearch.CLUSTER_NAME);
        command.setDocData("names");
        command.setDocTemplate("classpath:/com/github/gquintana/elasticsearch/doc-name.mustache");
        command.setIterations(10);
        command.setThreads(2);
        command.setBulkSize(5);
        return command;
    }

    @Test
    public void testExecuteTransport() {
        // Given
        IndexCommand command = createIndexCommand();
        command.setProtocol("transport");
        command.setHosts(Arrays.asList("localhost:" + EmbeddedElasticsearch.getInstance().getNodeTransportPort()));
        EmbeddedElasticsearch.delete(command.getDocIndex());
        // When
        command.execute();
        command.close();
        // Then
        EmbeddedElasticsearch.refresh(command.getDocIndex());
        assertEquals(100, EmbeddedElasticsearch.count(command.getDocIndex()));
    }

    @Test
    public void testExecuteJest() {
        // Given
        IndexCommand command = createIndexCommand();
        command.setProtocol("jest");
        command.setHosts(Arrays.asList("localhost:" + EmbeddedElasticsearch.getInstance().getNodeHttpPort()));
        EmbeddedElasticsearch.delete(command.getDocIndex());
        // When
        command.execute();
        command.close();
        // Then
        EmbeddedElasticsearch.refresh(command.getDocIndex());
        assertEquals(100, EmbeddedElasticsearch.count(command.getDocIndex()));
    }

}