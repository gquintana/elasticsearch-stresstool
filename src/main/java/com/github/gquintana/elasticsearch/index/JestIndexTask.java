package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.JestException;
import com.github.gquintana.elasticsearch.data.Data;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * Service to index using HTTP protocol and Jest
 */
public class JestIndexTask extends IndexTask {
    private final JestClient client;
    private int bulkSize;

    public JestIndexTask(JestClient client, DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
        this.client = client;
    }

    private Index prepareIndex() {
        Data data = provideData();
        return new Index.Builder(data.getSource())
                .index(data.getIndex())
                .type(data.getType()).id(data.getId())
                .build();
    }

    public void execute() {
        Action action;
        if (shouldBulkIndex()) {
            Bulk.Builder bulkBuilder = new Bulk.Builder();
            for (int i = 0; i < bulkSize; i++) {
                bulkBuilder.addAction(prepareIndex());
            }
            action = bulkBuilder.build();
        } else {
            action = prepareIndex();
        }
        try {
            JestResult indexResult = client.execute(action);
            JestException.handleResult(indexResult, "Index failed");
        } catch (IOException e) {
            throw new JestException(e);
        } catch (RuntimeException e) {
            JestException.handleException(e);
        }
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    public JestClient getClient() {
        return client;
    }
}
