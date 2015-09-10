package com.github.gquintana.elasticsearch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JestIndexingTaskTest {

    @BeforeClass
    public static void setUpClass() {
        EmbeddedElasticsearch.node();
    }

    @Test
    public void testExecuteBulk() {
        // Given
        TemplatingService templatingService = new TemplatingService();
        ConstantDataProvider dataProvider = new ConstantDataProvider("index", "type");
        JestIndexingTask service = new JestIndexingTask(EmbeddedElasticsearch.jestClient(), dataProvider, templatingService);
        service.setBulkSize(10);
        // When
        service.execute();
        // Then
        EmbeddedElasticsearch.refresh("index");
        assertEquals(10L, EmbeddedElasticsearch.count("index"));
        EmbeddedElasticsearch.delete("index");
    }

    @Test
    public void testExecuteOne() {
        // Given
        TemplatingService templatingService = new TemplatingService();
        ConstantDataProvider dataProvider = new ConstantDataProvider("index", "type");
        JestIndexingTask service = new JestIndexingTask(EmbeddedElasticsearch.jestClient(), dataProvider, templatingService);
        // When
        service.execute();
        // Then
        EmbeddedElasticsearch.refresh("index");
        assertEquals(1L, EmbeddedElasticsearch.count("index"));
        EmbeddedElasticsearch.delete("index");
    }

    @AfterClass
    public static void tearDownClass() {
        EmbeddedElasticsearch.close();
    }

}