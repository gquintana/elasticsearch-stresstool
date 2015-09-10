package com.github.gquintana.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

/**
 * Service to index using transport protocol
 */
public class TransportIndexingTask extends IndexingTask {
    private final Client client;

    public TransportIndexingTask(Client client, DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
        this.client = client;
    }

    private IndexRequestBuilder prepareIndex() {
        Data data = provideData();
        return client.prepareIndex(data.getIndex(), data.getType(), data.getId())
                .setSource(data.getSource());
    }

    public void execute() {
        if (shouldBulkIndex()) {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (int i = 0; i < bulkSize; i++) {
                bulkRequestBuilder.add(prepareIndex());
            }
            bulkRequestBuilder.execute().actionGet();
        } else {
            prepareIndex().execute().actionGet();
        }
    }

    public Client getClient() {
        return client;
    }
}
