package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.EmbeddedElasticsearch;
import com.github.gquintana.elasticsearch.data.ConstantDataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

public class JestIndexTaskTest extends IndexTaskTest {
    @Override
    protected IndexTask createTask() {
        TemplatingService templatingService = new TemplatingService();
        ConstantDataProvider dataProvider = new ConstantDataProvider("index", "type");
        JestIndexTask task = new JestIndexTask(EmbeddedElasticsearch.jestClient(), dataProvider, templatingService);
        return task;
    }
}