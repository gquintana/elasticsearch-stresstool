package com.github.gquintana.elasticsearch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JestSearchingTaskTest {

    @BeforeClass
    public static void setUpClass() {
        EmbeddedElasticsearch.node();
    }

    @Test
    public void testExecute() {
        // Given
        TemplatingService templatingService = new TemplatingService();
        ConstantDataProvider dataProvider = new ConstantDataProvider("index", "type");
        JestIndexingTask indexingTask = new JestIndexingTask(EmbeddedElasticsearch.jestClient(), dataProvider, templatingService);
        indexingTask.setTemplateLocation(Resources.classResource(getClass(), TransportSearchingTaskTest.class.getSimpleName() + "Doc.mustache"));
        indexingTask.setBulkSize(10);
        indexingTask.execute();
        EmbeddedElasticsearch.refresh("index");
        JestSearchingTask searchingTask = new JestSearchingTask(EmbeddedElasticsearch.jestClient(), dataProvider, templatingService);
        searchingTask.setTemplateLocation(Resources.classResource(getClass(), TransportSearchingTaskTest.class.getSimpleName() + "Query.mustache"));
        // When
        searchingTask.execute();
        // Then
        EmbeddedElasticsearch.delete("index");
    }

   @AfterClass
    public static void tearDownClass() {
        EmbeddedElasticsearch.close();
    }
}