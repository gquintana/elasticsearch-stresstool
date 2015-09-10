package com.github.gquintana.elasticsearch.search;

import com.github.gquintana.elasticsearch.EmbeddedElasticsearch;
import com.github.gquintana.elasticsearch.Resources;
import com.github.gquintana.elasticsearch.data.ConstantDataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import com.github.gquintana.elasticsearch.index.TransportIndexTask;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransportSearchTaskTest {

    @BeforeClass
    public static void setUpClass() {
        EmbeddedElasticsearch.node();
    }

    @Test
    public void testExecute() {
        // Given
        TemplatingService templatingService = new TemplatingService();
        ConstantDataProvider dataProvider = new ConstantDataProvider("index", "type");
        TransportIndexTask indexingTask = new TransportIndexTask(EmbeddedElasticsearch.client(), dataProvider, templatingService);
        indexingTask.setTemplateLocation(Resources.classResource(getClass(), getClass().getSimpleName() + "Doc.mustache"));
        indexingTask.setBulkSize(10);
        indexingTask.execute();
        EmbeddedElasticsearch.refresh("index");
        TransportSearchTask searchingTask = new TransportSearchTask(EmbeddedElasticsearch.client(), dataProvider, templatingService);
        searchingTask.setTemplateLocation(Resources.classResource(getClass(), getClass().getSimpleName() + "Query.mustache"));
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