package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.EmbeddedElasticsearch;
import com.github.gquintana.elasticsearch.data.ConstantDataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

public class TransportIndexTaskTest extends IndexTaskTest {
    protected IndexTask createTask() {
        TemplatingService templatingService = new TemplatingService();
        ConstantDataProvider dataProvider = new ConstantDataProvider("index", "type");
        return new TransportIndexTask(EmbeddedElasticsearch.client(), dataProvider, templatingService);
    }
}