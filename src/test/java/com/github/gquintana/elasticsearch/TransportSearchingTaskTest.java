package com.github.gquintana.elasticsearch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransportSearchingTaskTest {

    @BeforeClass
    public static void setUpClass() {
        EmbeddedElasticsearch.node();
    }

    @Test
    public void testExecute() {
        // Given
        TemplatingService templatingService = new TemplatingService();
        ConstantDataProvider dataProvider = new ConstantDataProvider("index", "type");
        TransportIndexingTask indexingTask = new TransportIndexingTask(EmbeddedElasticsearch.client(), dataProvider, templatingService);
        indexingTask.setTemplateLocation(Resources.classResource(getClass(), getClass().getSimpleName() + "Doc.mustache"));
        indexingTask.setBulkSize(10);
        indexingTask.execute();
        EmbeddedElasticsearch.refresh("index");
        TransportSearchingTask searchingTask = new TransportSearchingTask(EmbeddedElasticsearch.client(), dataProvider, templatingService);
        searchingTask.setTemplateLocation(Resources.classResource(getClass(), getClass().getSimpleName() + "Query.mustache"));
        // When
        searchingTask.execute();
        // Then
        EmbeddedElasticsearch.delete("index");
    }

   @AfterClass
    public static void tearDownClass() {
        EmbeddedElasticsearch.node();
    }
}